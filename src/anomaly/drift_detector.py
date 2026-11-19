"""
drift_detector.py
-----------------
Détection du Data Drift sur le dataset US Accidents partitionné en Parquet.

Méthodes utilisées :
  - PSI (Population Stability Index) : mesure le glissement de distribution
      PSI < 0.1  → stable
      0.1 ≤ PSI < 0.2 → léger changement (WARNING)
      PSI ≥ 0.2  → drift significatif (ALERT)
  - Test KS (Kolmogorov-Smirnov) : compare la CDF d'une période à la référence
      p-value < 0.05 → distributions significativement différentes

Stratégie :
  - Période de référence = les N premiers mois (config thresholds.yaml)
  - Comparaison mois par mois vs référence
  - Spark pour les agrégations, Pandas pour les calculs statistiques
  - Échantillonnage (5 %) pour les colonnes volumineuses
"""

import os
import sys
import time
import warnings
from pathlib import Path

# ── Hadoop doit être défini AVANT tout import pyspark ──────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
_HADOOP_HOME  = str(_PROJET_ROOT / "hadoop")
os.environ["HADOOP_HOME"]    = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = str(Path(_HADOOP_HOME) / "bin") + os.pathsep + os.environ.get("PATH", "")

import numpy as np
import pandas as pd
import yaml
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from scipy import stats
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

warnings.filterwarnings("ignore")

# ── Chemins ─────────────────────────────────────────────────────────────────────
PARQUET_DIR  = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
METRICS_DIR  = _PROJET_ROOT / "data" / "metrics" / "drift"
FIGURES_DIR  = _PROJET_ROOT / "reports" / "figures"
CONFIG_FILE  = _PROJET_ROOT / "config" / "thresholds.yaml"

METRICS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# ── Colonnes numériques surveillées ─────────────────────────────────────────────
COLONNES_NUMERIQUES = [
    "Temperature(F)", "Visibility(mi)", "Wind_Speed(mph)",
    "Distance(mi)", "Precipitation(in)", "Humidity(%)", "Pressure(in)"
]

TAUX_ECHANTILLON = 0.05   # 5 % pour les calculs Pandas


def charger_config() -> dict:
    """Charge la configuration depuis thresholds.yaml."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def creer_session_spark(driver_memory: str = "6g") -> SparkSession:
    """Crée et retourne une session Spark optimisée pour la machine locale."""
    return (
        SparkSession.builder
        .appName("BigDataObs_DriftDetector")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


# ════════════════════════════════════════════════════════════════════════════════
# PSI
# ════════════════════════════════════════════════════════════════════════════════

def calculer_psi(reference: np.ndarray, cible: np.ndarray, n_bins: int = 10) -> float:
    """
    Calcule le Population Stability Index entre deux échantillons.

    PSI = Σ (cible_% - ref_%) × ln(cible_% / ref_%)
    """
    reference = reference[~np.isnan(reference)]
    cible     = cible[~np.isnan(cible)]

    if len(reference) == 0 or len(cible) == 0:
        return np.nan

    # Découpage sur les quantiles de la référence pour des bins équilibrés
    bornes = np.unique(np.percentile(reference, np.linspace(0, 100, n_bins + 1)))
    if len(bornes) < 2:
        return 0.0

    # Comptages normalisés (on ajoute ε pour éviter log(0))
    eps  = 1e-6
    ref_counts, _ = np.histogram(reference, bins=bornes)
    cib_counts, _ = np.histogram(cible,     bins=bornes)

    ref_pct = ref_counts / (len(reference) + eps) + eps
    cib_pct = cib_counts / (len(cible)     + eps) + eps

    psi = np.sum((cib_pct - ref_pct) * np.log(cib_pct / ref_pct))
    return float(psi)


def label_psi(valeur: float) -> str:
    """Retourne le label de sévérité PSI."""
    if np.isnan(valeur):
        return "N/A"
    if valeur < 0.1:
        return "STABLE"
    if valeur < 0.2:
        return "WARNING"
    return "ALERT"


# ════════════════════════════════════════════════════════════════════════════════
# Collecte des échantillons mensuels via Spark
# ════════════════════════════════════════════════════════════════════════════════

def collecter_echantillons_mensuels(spark: SparkSession, df) -> dict:
    """
    Pour chaque (year, month) et chaque colonne numérique,
    collecte un échantillon de TAUX_ECHANTILLON en Pandas.
    Retourne un dict : {(year, month): pd.DataFrame}
    """
    print("  → Collecte des échantillons mensuels (5 %)…")

    # Sélection des colonnes utiles + cast en double
    cols_cast = [F.col(c).cast(DoubleType()).alias(c) for c in COLONNES_NUMERIQUES]
    df_num = df.select(["year", "month"] + cols_cast)

    # On récupère les paires (year, month) disponibles
    periodes = (
        df_num
        .select("year", "month")
        .distinct()
        .orderBy("year", "month")
        .collect()
    )

    echantillons = {}
    for row in periodes:
        y, m = int(row["year"]), int(row["month"])
        sample = (
            df_num
            .filter((F.col("year") == y) & (F.col("month") == m))
            .sample(fraction=TAUX_ECHANTILLON, seed=42)
            .toPandas()
        )
        echantillons[(y, m)] = sample
        print(f"     ({y}-{m:02d}) : {len(sample):,} lignes échantillonnées")

    return echantillons, periodes


# ════════════════════════════════════════════════════════════════════════════════
# Calcul PSI + KS pour toutes les paires (mois, colonne)
# ════════════════════════════════════════════════════════════════════════════════

def detecter_drift(echantillons: dict, periodes: list, n_mois_ref: int = 3) -> pd.DataFrame:
    """
    Calcule PSI et KS test pour chaque mois vs la période de référence.

    Retourne un DataFrame avec les colonnes :
    year, month, colonne, psi, label_psi, ks_stat, ks_pvalue, drift_detecte
    """
    # Construction de la référence : union des N premiers mois
    periodes_triees = sorted(periodes, key=lambda r: (int(r["year"]), int(r["month"])))
    periodes_ref    = periodes_triees[:n_mois_ref]
    periodes_cibles = periodes_triees[n_mois_ref:]

    print(f"\n  → Référence : {n_mois_ref} premiers mois")
    for p in periodes_ref:
        print(f"     {int(p['year'])}-{int(p['month']):02d}")

    # Concaténation des données de référence
    df_ref = pd.concat(
        [echantillons[(int(p["year"]), int(p["month"]))] for p in periodes_ref],
        ignore_index=True
    )

    resultats = []
    for p in periodes_cibles:
        y, m  = int(p["year"]), int(p["month"])
        df_cib = echantillons.get((y, m), pd.DataFrame())

        for col in COLONNES_NUMERIQUES:
            ref_vals = df_ref[col].dropna().values
            cib_vals = df_cib[col].dropna().values if not df_cib.empty else np.array([])

            # PSI
            psi_val = calculer_psi(ref_vals, cib_vals)
            lbl_psi = label_psi(psi_val)

            # KS test
            if len(cib_vals) > 1 and len(ref_vals) > 1:
                ks_stat, ks_pval = stats.ks_2samp(ref_vals, cib_vals)
            else:
                ks_stat, ks_pval = np.nan, np.nan

            drift = (lbl_psi in ("WARNING", "ALERT")) or (ks_pval is not np.nan and ks_pval < 0.05)

            resultats.append({
                "year":          y,
                "month":         m,
                "colonne":       col,
                "psi":           round(psi_val, 6) if not np.isnan(psi_val) else None,
                "label_psi":     lbl_psi,
                "ks_stat":       round(ks_stat, 6) if not np.isnan(ks_stat) else None,
                "ks_pvalue":     round(ks_pval, 6) if not np.isnan(ks_pval) else None,
                "drift_detecte": drift,
            })

    return pd.DataFrame(resultats)


# ════════════════════════════════════════════════════════════════════════════════
# Visualisations
# ════════════════════════════════════════════════════════════════════════════════

def tracer_psi_heatmap(df_drift: pd.DataFrame):
    """Heatmap PSI : colonnes × mois."""
    pivot = df_drift.pivot_table(index="colonne", columns=["year", "month"], values="psi")
    # Labels des colonnes : YYYY-MM
    pivot.columns = [f"{y}-{m:02d}" for y, m in pivot.columns]

    fig, ax = plt.subplots(figsize=(max(10, len(pivot.columns)), 5))
    cmap = plt.cm.RdYlGn_r
    im = ax.imshow(pivot.values, aspect="auto", cmap=cmap, vmin=0, vmax=0.3)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)

    # Valeurs dans les cellules
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.3f}", ha="center", va="center",
                        fontsize=7, color="black" if val < 0.15 else "white")

    plt.colorbar(im, ax=ax, label="PSI")
    ax.set_title("Heatmap PSI par colonne et par mois\n(vert=stable, rouge=drift)", fontsize=11)
    ax.set_xlabel("Période (année-mois)")
    ax.set_ylabel("Colonne numérique")
    plt.tight_layout()
    out = FIGURES_DIR / "drift_psi_heatmap.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Heatmap PSI sauvegardée : {out.name}")


def tracer_ks_pvalue(df_drift: pd.DataFrame):
    """Graphique p-values KS pour détecter les mois atypiques."""
    fig, axes = plt.subplots(
        len(COLONNES_NUMERIQUES), 1,
        figsize=(12, 2.2 * len(COLONNES_NUMERIQUES)),
        sharex=True
    )

    for ax, col in zip(axes, COLONNES_NUMERIQUES):
        sub = df_drift[df_drift["colonne"] == col].copy()
        sub["periode"] = sub.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
        sub = sub.sort_values("periode")

        couleurs = ["red" if p < 0.05 else "steelblue" for p in sub["ks_pvalue"].fillna(1)]
        ax.bar(sub["periode"], sub["ks_pvalue"], color=couleurs, alpha=0.8)
        ax.axhline(0.05, color="orange", linestyle="--", linewidth=1, label="seuil 0.05")
        ax.set_ylabel("p-value", fontsize=8)
        ax.set_title(col, fontsize=9)
        ax.set_ylim(0, 1)
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))

    plt.xticks(rotation=45, ha="right", fontsize=7)
    fig.suptitle("Test KS p-value par colonne et par mois\n(rouge = drift détecté, p<0.05)", fontsize=11)
    plt.tight_layout()
    out = FIGURES_DIR / "drift_ks_pvalue.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Graphique KS p-values sauvegardé : {out.name}")


def tracer_resume_drift(df_drift: pd.DataFrame):
    """Bar chart : nombre de colonnes en drift par mois."""
    df_drift["periode"] = df_drift.apply(
        lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1
    )
    resume = (
        df_drift[df_drift["drift_detecte"]]
        .groupby("periode")["colonne"]
        .nunique()
        .reset_index()
        .rename(columns={"colonne": "nb_colonnes_drift"})
        .sort_values("periode")
    )

    fig, ax = plt.subplots(figsize=(12, 4))
    couleurs = ["crimson" if n >= 4 else "darkorange" if n >= 2 else "gold"
                for n in resume["nb_colonnes_drift"]]
    ax.bar(resume["periode"], resume["nb_colonnes_drift"], color=couleurs)
    ax.axhline(len(COLONNES_NUMERIQUES) // 2, color="red", linestyle="--",
               linewidth=1, label="50 % des colonnes")
    ax.set_title("Nombre de colonnes en drift par mois", fontsize=11)
    ax.set_xlabel("Période")
    ax.set_ylabel("Nb colonnes")
    ax.set_ylim(0, len(COLONNES_NUMERIQUES) + 1)
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.tight_layout()
    out = FIGURES_DIR / "drift_resume_mensuel.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Résumé drift mensuel sauvegardé : {out.name}")


# ════════════════════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    print("=" * 60)
    print("PARTIE 3a — Détection du Data Drift")
    print("=" * 60)

    # Chargement config
    cfg         = charger_config()
    n_mois_ref  = cfg.get("drift", {}).get("fenetre_reference_mois", 3)
    driver_mem  = cfg.get("spark", {}).get("driver_memory", "6g")
    print(f"\n  Config : référence={n_mois_ref} mois, driver_memory={driver_mem}")

    # Spark
    print("\n[1/4] Démarrage de Spark…")
    spark = creer_session_spark(driver_mem)
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(PARQUET_DIR))
    nb_lignes = df.count()
    print(f"  → {nb_lignes:,} lignes chargées depuis le Parquet")

    # Échantillons mensuels
    print("\n[2/4] Collecte des échantillons mensuels…")
    echantillons, periodes = collecter_echantillons_mensuels(spark, df)
    print(f"  → {len(periodes)} périodes disponibles")

    # Drift PSI + KS
    print("\n[3/4] Calcul PSI et KS test…")
    df_drift = detecter_drift(echantillons, periodes, n_mois_ref=n_mois_ref)

    # Résumé console
    n_drifts = df_drift["drift_detecte"].sum()
    print(f"\n  Résultat global : {n_drifts} / {len(df_drift)} combinaisons (col × mois) en drift")
    if n_drifts > 0:
        top = (
            df_drift[df_drift["drift_detecte"]]
            .sort_values("psi", ascending=False)
            .head(10)[["year", "month", "colonne", "psi", "label_psi", "ks_pvalue"]]
        )
        print("\n  Top drifts détectés :")
        print(top.to_string(index=False))

    # Export CSV
    out_drift = METRICS_DIR / "drift_psi_ks.csv"
    df_drift.to_csv(out_drift, index=False, encoding="utf-8")
    print(f"\n  ✓ Métriques drift exportées : {out_drift.name}")

    # Résumé par colonne
    resume_col = (
        df_drift.groupby("colonne")
        .agg(
            psi_moyen=("psi", "mean"),
            psi_max=("psi", "max"),
            nb_mois_drift=("drift_detecte", "sum"),
            nb_mois_total=("drift_detecte", "count"),
        )
        .reset_index()
    )
    resume_col["pct_mois_drift"] = (
        resume_col["nb_mois_drift"] / resume_col["nb_mois_total"] * 100
    ).round(2)
    resume_col["label_global"] = resume_col["psi_moyen"].apply(label_psi)
    out_col = METRICS_DIR / "drift_resume_colonnes.csv"
    resume_col.to_csv(out_col, index=False, encoding="utf-8")
    print(f"  ✓ Résumé par colonne exporté : {out_col.name}")
    print("\n  Résumé par colonne :")
    print(resume_col.to_string(index=False))

    # Visualisations
    print("\n[4/4] Génération des graphiques…")
    tracer_psi_heatmap(df_drift)
    tracer_ks_pvalue(df_drift)
    tracer_resume_drift(df_drift)

    spark.stop()
    duree = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"✅ Drift detection terminé en {duree:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
