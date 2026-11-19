"""
anomaly_detector.py
-------------------
Détection d'anomalies sur le dataset US Accidents par fenêtre temporelle.

Méthodes utilisées :
  - Z-score : détecte les enregistrements dont les valeurs s'écartent de plus
              de N écart-types de la moyenne (seuil configurable)
  - Isolation Forest : modèle non supervisé basé sur la séparation aléatoire
              des points dans l'espace des features (contamination configurable)

Stratégie :
  - Spark pour le chargement et l'agrégation
  - Pandas + sklearn sur un échantillon de 5 % pour l'Isolation Forest
  - Z-score calculé sur l'échantillon par fenêtre mensuelle
  - Résultats exportés dans data/metrics/anomalies/
"""

import os
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
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

warnings.filterwarnings("ignore")

# ── Chemins ─────────────────────────────────────────────────────────────────────
PARQUET_DIR   = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
METRICS_DIR   = _PROJET_ROOT / "data" / "metrics" / "anomalies"
FIGURES_DIR   = _PROJET_ROOT / "reports" / "figures"
CONFIG_FILE   = _PROJET_ROOT / "config" / "thresholds.yaml"

METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ── Colonnes numériques surveillées ─────────────────────────────────────────────
COLONNES_NUMERIQUES = [
    "Temperature(F)", "Visibility(mi)", "Wind_Speed(mph)",
    "Distance(mi)", "Precipitation(in)", "Humidity(%)", "Pressure(in)"
]

TAUX_ECHANTILLON = 0.05   # 5 %


def charger_config() -> dict:
    """Charge la configuration depuis thresholds.yaml."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def creer_session_spark(driver_memory: str = "6g") -> SparkSession:
    """Crée et retourne une session Spark optimisée pour la machine locale."""
    return (
        SparkSession.builder
        .appName("BigDataObs_AnomalyDetector")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


# ════════════════════════════════════════════════════════════════════════════════
# Z-SCORE
# ════════════════════════════════════════════════════════════════════════════════

def detecter_zscore(df_sample: pd.DataFrame, seuil: float = 3.0) -> pd.DataFrame:
    """
    Calcule le Z-score pour chaque colonne numérique.
    Retourne un DataFrame avec le nb et le % d'anomalies par colonne.
    """
    resultats = []
    for col in COLONNES_NUMERIQUES:
        vals = df_sample[col].dropna()
        if len(vals) == 0:
            continue
        z      = np.abs(stats.zscore(vals))
        nb_ok  = int((z >= seuil).sum())
        pct    = round(nb_ok / len(vals) * 100, 4)
        resultats.append({
            "colonne":           col,
            "nb_lignes":         len(vals),
            "nb_anomalies":      nb_ok,
            "pct_anomalies":     pct,
            "seuil_zscore":      seuil,
            "methode":           "z-score",
        })
    return pd.DataFrame(resultats)


# ════════════════════════════════════════════════════════════════════════════════
# ISOLATION FOREST
# ════════════════════════════════════════════════════════════════════════════════

def detecter_isolation_forest(
    df_sample: pd.DataFrame,
    contamination: float = 0.05,
    n_estimators: int = 100,
    random_state: int = 42,
) -> dict:
    """
    Applique Isolation Forest sur les colonnes numériques.

    Retourne :
      - df_scores   : DataFrame avec colonne 'anomaly' (-1 = anomalie, 1 = normal)
      - stats_globales : dict avec nb_anomalies, pct_anomalies
    """
    df_work = df_sample[COLONNES_NUMERIQUES].copy()

    # Imputation des NaN par la médiane (Isolation Forest ne supporte pas NaN)
    for col in COLONNES_NUMERIQUES:
        mediane = df_work[col].median()
        df_work[col] = df_work[col].fillna(mediane)

    # Normalisation
    scaler    = StandardScaler()
    X_scaled  = scaler.fit_transform(df_work)

    # Modèle
    clf = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )
    predictions   = clf.fit_predict(X_scaled)
    scores_bruts  = clf.score_samples(X_scaled)   # score d'anomalie (plus négatif = plus anormal)

    df_result = df_sample.copy()
    df_result["if_prediction"]  = predictions       # -1 = anomalie
    df_result["if_score"]       = scores_bruts

    nb_anomalies = int((predictions == -1).sum())
    pct          = round(nb_anomalies / len(predictions) * 100, 4)

    stats_globales = {
        "methode":        "isolation_forest",
        "nb_lignes":      len(df_sample),
        "nb_anomalies":   nb_anomalies,
        "pct_anomalies":  pct,
        "contamination":  contamination,
        "n_estimators":   n_estimators,
    }

    return df_result, stats_globales


# ════════════════════════════════════════════════════════════════════════════════
# Anomalies par mois
# ════════════════════════════════════════════════════════════════════════════════

def analyser_anomalies_par_mois(
    spark: SparkSession,
    df,
    seuil_zscore: float,
    contamination_if: float,
) -> tuple:
    """
    Pour chaque mois, échantillonne les données et applique
    Z-score et Isolation Forest. Retourne les DataFrames de résultats.
    """
    # Cast des colonnes numériques
    cols_cast = [F.col(c).cast(DoubleType()).alias(c) for c in COLONNES_NUMERIQUES]
    df_num = df.select(["year", "month"] + cols_cast)

    # Paires (year, month) disponibles
    periodes = (
        df_num.select("year", "month")
        .distinct()
        .orderBy("year", "month")
        .collect()
    )
    print(f"  → {len(periodes)} périodes à analyser")

    rows_zscore = []
    rows_if     = []

    for row in periodes:
        y, m = int(row["year"]), int(row["month"])

        sample_pd = (
            df_num
            .filter((F.col("year") == y) & (F.col("month") == m))
            .sample(fraction=TAUX_ECHANTILLON, seed=42)
            .toPandas()
        )

        if len(sample_pd) < 30:
            print(f"     ({y}-{m:02d}) : échantillon trop petit ({len(sample_pd)}), ignoré")
            continue

        # Z-score
        df_zs = detecter_zscore(sample_pd, seuil=seuil_zscore)
        df_zs["year"]  = y
        df_zs["month"] = m
        rows_zscore.append(df_zs)

        # Isolation Forest
        _, stats_if = detecter_isolation_forest(
            sample_pd, contamination=contamination_if
        )
        stats_if["year"]  = y
        stats_if["month"] = m
        rows_if.append(stats_if)

        print(
            f"     ({y}-{m:02d}) Z-score: {df_zs['nb_anomalies'].sum()} anomalies | "
            f"IF: {stats_if['nb_anomalies']} anomalies"
        )

    df_zscore_all = pd.concat(rows_zscore, ignore_index=True) if rows_zscore else pd.DataFrame()
    df_if_all     = pd.DataFrame(rows_if)

    return df_zscore_all, df_if_all


# ════════════════════════════════════════════════════════════════════════════════
# Visualisations
# ════════════════════════════════════════════════════════════════════════════════

def tracer_anomalies_zscore(df_zs: pd.DataFrame):
    """Heatmap du % d'anomalies Z-score par colonne et par mois."""
    if df_zs.empty:
        return

    df_zs["periode"] = df_zs.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
    pivot = df_zs.pivot_table(index="colonne", columns="periode", values="pct_anomalies")

    fig, ax = plt.subplots(figsize=(max(10, len(pivot.columns)), 5))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd", vmin=0, vmax=10)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.1f}%", ha="center", va="center", fontsize=7)

    plt.colorbar(im, ax=ax, label="% anomalies")
    ax.set_title("% Anomalies Z-score par colonne et par mois", fontsize=11)
    plt.tight_layout()
    out = FIGURES_DIR / "anomalies_zscore_heatmap.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Heatmap Z-score sauvegardée : {out.name}")


def tracer_anomalies_if(df_if: pd.DataFrame):
    """Courbe du % d'anomalies Isolation Forest par mois."""
    if df_if.empty:
        return

    df_if["periode"] = df_if.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
    df_if_sorted = df_if.sort_values("periode")

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(df_if_sorted["periode"], df_if_sorted["pct_anomalies"],
            marker="o", color="crimson", linewidth=2)
    ax.axhline(df_if_sorted["pct_anomalies"].mean(), color="orange",
               linestyle="--", label=f"Moyenne : {df_if_sorted['pct_anomalies'].mean():.2f}%")
    ax.fill_between(df_if_sorted["periode"], df_if_sorted["pct_anomalies"],
                    alpha=0.15, color="crimson")
    ax.set_title("% Anomalies Isolation Forest par mois", fontsize=11)
    ax.set_xlabel("Période")
    ax.set_ylabel("% Anomalies")
    ax.legend()
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.tight_layout()
    out = FIGURES_DIR / "anomalies_isolation_forest.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Courbe Isolation Forest sauvegardée : {out.name}")


def tracer_top_anomalies_zscore(df_zs: pd.DataFrame):
    """Bar chart horizontal : colonnes les plus touchées par anomalies Z-score."""
    if df_zs.empty:
        return

    resume = (
        df_zs.groupby("colonne")["pct_anomalies"]
        .mean()
        .sort_values(ascending=True)
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(8, 4))
    couleurs = ["crimson" if p > 5 else "darkorange" if p > 2 else "steelblue"
                for p in resume["pct_anomalies"]]
    ax.barh(resume["colonne"], resume["pct_anomalies"], color=couleurs)
    ax.axvline(3, color="red", linestyle="--", linewidth=1, label="seuil 3%")
    ax.set_title("% Anomalies Z-score moyen par colonne", fontsize=11)
    ax.set_xlabel("% Anomalies (moyenne mensuelle)")
    ax.legend()
    plt.tight_layout()
    out = FIGURES_DIR / "anomalies_zscore_par_colonne.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Bar chart Z-score par colonne sauvegardé : {out.name}")


# ════════════════════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    print("=" * 60)
    print("PARTIE 3b — Détection d'Anomalies")
    print("=" * 60)

    # Config
    cfg             = charger_config()
    seuil_zscore    = cfg.get("anomalies", {}).get("seuil_zscore",      3.0)
    contamination   = cfg.get("anomalies", {}).get("contamination_if",  0.05)
    driver_mem      = cfg.get("spark", {}).get("driver_memory",        "6g")
    print(f"\n  Config : z-score seuil={seuil_zscore}, IF contamination={contamination}")

    # Spark
    print("\n[1/4] Démarrage de Spark…")
    spark = creer_session_spark(driver_mem)
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(PARQUET_DIR))
    print(f"  → {df.count():,} lignes chargées")

    # Analyse par mois
    print("\n[2/4] Analyse par mois (Z-score + Isolation Forest)…")
    df_zs, df_if = analyser_anomalies_par_mois(
        spark, df, seuil_zscore=seuil_zscore, contamination_if=contamination
    )

    # Export CSV
    print("\n[3/4] Export des métriques…")
    if not df_zs.empty:
        out_zs = METRICS_DIR / "anomalies_zscore.csv"
        df_zs.to_csv(out_zs, index=False, encoding="utf-8")
        print(f"  ✓ Z-score exporté : {out_zs.name}")

        # Résumé global Z-score
        resume_zs = (
            df_zs.groupby("colonne")
            .agg(
                pct_moyen=("pct_anomalies", "mean"),
                pct_max=("pct_anomalies", "max"),
                nb_mois=("pct_anomalies", "count"),
            )
            .reset_index()
        )
        resume_zs["pct_moyen"] = resume_zs["pct_moyen"].round(4)
        resume_zs["pct_max"]   = resume_zs["pct_max"].round(4)
        print("\n  Résumé Z-score par colonne :")
        print(resume_zs.to_string(index=False))

    if not df_if.empty:
        out_if = METRICS_DIR / "anomalies_isolation_forest.csv"
        df_if.to_csv(out_if, index=False, encoding="utf-8")
        print(f"\n  ✓ Isolation Forest exporté : {out_if.name}")
        pct_moy = df_if["pct_anomalies"].mean()
        pct_max = df_if["pct_anomalies"].max()
        print(f"  IF global → pct moyen : {pct_moy:.2f}%, pct max : {pct_max:.2f}%")

    # Visualisations
    print("\n[4/4] Génération des graphiques…")
    tracer_anomalies_zscore(df_zs)
    tracer_top_anomalies_zscore(df_zs)
    tracer_anomalies_if(df_if)

    spark.stop()
    duree = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"✅ Détection d'anomalies terminée en {duree:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
