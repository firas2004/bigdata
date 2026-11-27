"""
exp4_anomaly_drift_eval.py
--------------------------
Expérience 4 — Évaluation de la détection d'anomalies et du Data Drift.

Partie A : compare Z-Score et Isolation Forest sur un jeu de test contrôlé
(5 000 observations normales + 100 anomalies injectées à positions connues :
50 univariées et 50 multivariées). Mesure : Précision, Rappel, F1, faux positifs.

Partie B : compare les distributions PSI et KS entre les périodes 2016–2019 et
2020–2023, puis valide la sensibilité par injection d'un drift artificiel
(+15 °F sur Temperature, ×2 sur Wind_Speed) versus un jeu témoin sans modification.

Sorties : data/metrics/experiments/, reports/figures/
"""

import os
import sys
import warnings
from pathlib import Path

# ── Hadoop doit être défini AVANT tout import pyspark ──────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
_HADOOP_HOME = str(_PROJET_ROOT / "hadoop")
os.environ["HADOOP_HOME"] = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = str(Path(_HADOOP_HOME) / "bin") + os.pathsep + os.environ.get("PATH", "")

sys.path.insert(0, str(_PROJET_ROOT))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.anomaly.drift_detector import calculer_psi, label_psi

warnings.filterwarnings("ignore")

PARQUET_SRC = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
METRICS_DIR = _PROJET_ROOT / "data" / "metrics" / "experiments"
FIGURES_DIR = _PROJET_ROOT / "reports" / "figures"

METRICS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

COLONNES_NUM = [
    "Temperature(F)", "Visibility(mi)", "Wind_Speed(mph)",
    "Distance(mi)", "Precipitation(in)", "Humidity(%)", "Pressure(in)"
]


def creer_session_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BigDataObs_Exp4_AnomalyDrift")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


# ════════════════════════════════════════════════════════════════════════════════
# PARTIE A — Détection d'anomalies (Réel + Jeu contrôlé)
# ════════════════════════════════════════════════════════════════════════════════

def evaluer_anomalies_controlees(df_clean_pd: pd.DataFrame) -> pd.DataFrame:
    """
    Crée un jeu de test de 5 000 lignes saines et injecte 100 anomalies
    à des positions connues (50 univariées + 50 multivariées).
    Compare Z-Score et Isolation Forest.
    """
    rng = np.random.default_rng(42)
    base = df_clean_pd[COLONNES_NUM].dropna().sample(n=5000, random_state=42).reset_index(drop=True)

    # On supprime d'éventuelles valeurs déjà extrêmes du socle sain pour isoler les anomalies injectées
    for col in COLONNES_NUM:
        q05, q95 = base[col].quantile(0.05), base[col].quantile(0.95)
        base[col] = base[col].clip(q05, q95)

    y_true = np.zeros(len(base), dtype=int)

    # 50 anomalies univariées franches (indices 0..49)
    base.loc[0:24, "Temperature(F)"] = 195.0
    base.loc[25:49, "Wind_Speed(mph)"] = 160.0
    y_true[0:50] = 1

    # 50 anomalies multivariées subtiles (indices 50..99) :
    # chaque variable reste modérément élevée/basse mais la combinaison est incohérente
    base.loc[50:99, "Temperature(F)"] = base["Temperature(F)"].quantile(0.95)
    base.loc[50:99, "Humidity(%)"] = base["Humidity(%)"].quantile(0.05)
    base.loc[50:99, "Pressure(in)"] = base["Pressure(in)"].quantile(0.05)
    base.loc[50:99, "Precipitation(in)"] = base["Precipitation(in)"].max() + 2.5
    y_true[50:100] = 1

    # 1) Z-Score (anomalie si au moins 1 colonne dépasse 3 sigma)
    z_mat = np.abs(stats.zscore(base[COLONNES_NUM]))
    pred_zscore = (z_mat >= 3.0).any(axis=1).astype(int)

    # 2) Isolation Forest (contamination proche du taux réel 100/5000 = 2%)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(base[COLONNES_NUM])
    clf = IsolationForest(n_estimators=150, contamination=0.025, random_state=42, n_jobs=-1)
    pred_if = (clf.fit_predict(X_scaled) == -1).astype(int)

    lignes = []
    for nom, pred in [("Z-Score (3 sigma)", pred_zscore), ("Isolation Forest", pred_if)]:
        tp = int(((pred == 1) & (y_true == 1)).sum())
        fn = int(((pred == 0) & (y_true == 1)).sum())
        fp = int(((pred == 1) & (y_true == 0)).sum())
        recall = round(tp / 100 * 100, 2)
        precision = round(tp / (tp + fp) * 100, 2) if (tp + fp) > 0 else 0.0
        lignes.append({
            "methode": nom,
            "anomalies_injectees": 100,
            "vrais_positifs_TP": tp,
            "faux_negatifs_FN": fn,
            "faux_positifs_FP": fp,
            "taux_detection_rappel_pct": recall,
            "precision_pct": precision,
        })

    return pd.DataFrame(lignes)


def comparer_anomalies_reel(df_sample_pd: pd.DataFrame) -> pd.DataFrame:
    """Compare Z-score et Isolation Forest sur un échantillon réel (5 % du dataset)."""
    df_work = df_sample_pd[COLONNES_NUM].copy()
    for col in COLONNES_NUM:
        df_work[col] = df_work[col].fillna(df_work[col].median())

    z_mat = np.abs(stats.zscore(df_work))
    nb_zs = int((z_mat >= 3.0).any(axis=1).sum())

    X_scaled = StandardScaler().fit_transform(df_work)
    clf = IsolationForest(n_estimators=100, contamination=0.05, random_state=42, n_jobs=-1)
    nb_if = int((clf.fit_predict(X_scaled) == -1).sum())

    n = len(df_work)
    return pd.DataFrame([
        {"methode": "Z-Score (au moins 1 col >= 3 sigma)", "nb_lignes": n, "nb_anomalies": nb_zs, "pct_anomalies": round(nb_zs / n * 100, 2)},
        {"methode": "Isolation Forest (contamination=5%)", "nb_lignes": n, "nb_anomalies": nb_if, "pct_anomalies": round(nb_if / n * 100, 2)},
    ])


# ════════════════════════════════════════════════════════════════════════════════
# PARTIE B — Détection de Data Drift (2016–2019 vs 2020–2023 + Drift artificiel)
# ════════════════════════════════════════════════════════════════════════════════

def evaluer_drift_periodes(df_2016_2019: pd.DataFrame, df_2020_2023: pd.DataFrame) -> pd.DataFrame:
    """Compare la distribution 2016–2019 (référence) vs 2020–2023 (cible)."""
    res = []
    for col in COLONNES_NUM:
        ref = df_2016_2019[col].dropna().values
        cib = df_2020_2023[col].dropna().values
        psi = calculer_psi(ref, cib)
        ks_stat, ks_p = stats.ks_2samp(ref, cib)
        res.append({
            "colonne": col,
            "psi": round(psi, 4),
            "label_psi": label_psi(psi),
            "ks_stat": round(float(ks_stat), 4),
            "ks_pvalue": float(f"{ks_p:.2e}"),
            "drift_detecte": (psi >= 0.1) or (ks_p < 0.05),
        })
    return pd.DataFrame(res)


def evaluer_drift_artificiel(df_ref: pd.DataFrame) -> pd.DataFrame:
    """
    Teste la sensibilité de PSI et KS en comparant un jeu témoin sans drift
    vs un jeu avec drift artificiel injecté (+15 °F sur Temperature, x2 Wind_Speed).
    """
    base = df_ref[COLONNES_NUM].dropna().sample(n=10000, random_state=42).reset_index(drop=True)
    moitie_ref = base.iloc[:5000].copy()
    moitie_temoin = base.iloc[5000:].copy()

    moitie_driftee = moitie_temoin.copy()
    moitie_driftee["Temperature(F)"] = moitie_driftee["Temperature(F)"] + 15.0
    moitie_driftee["Wind_Speed(mph)"] = moitie_driftee["Wind_Speed(mph)"] * 2.0 + 3.0

    lignes = []
    for scenario, cible_df in [("Sans drift (Témoin)", moitie_temoin), ("Avec drift artificiel", moitie_driftee)]:
        for col in ["Temperature(F)", "Wind_Speed(mph)", "Humidity(%)"]:
            r = moitie_ref[col].values
            c = cible_df[col].values
            psi = calculer_psi(r, c)
            ks_stat, ks_p = stats.ks_2samp(r, c)
            lignes.append({
                "scenario": scenario,
                "colonne": col,
                "psi": round(psi, 4),
                "label_psi": label_psi(psi),
                "ks_stat": round(float(ks_stat), 4),
                "ks_pvalue": round(float(ks_p), 6),
            })
    return pd.DataFrame(lignes)


def tracer_graphiques(df_periodes: pd.DataFrame, df_anom_ctrl: pd.DataFrame):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    df_p = df_periodes.sort_values("psi", ascending=True)
    couleurs = ["#dc2626" if v >= 0.2 else "#f59e0b" if v >= 0.1 else "#16a34a" for v in df_p["psi"]]
    ax1.barh(df_p["colonne"], df_p["psi"], color=couleurs)
    ax1.axvline(0.1, color="#f59e0b", linestyle="--", label="Seuil Warning (0.1)")
    ax1.axvline(0.2, color="#dc2626", linestyle="--", label="Seuil Alert (0.2)")
    ax1.set_title("Data Drift 2016–2019 vs 2020–2023 (PSI)")
    ax1.set_xlabel("Population Stability Index (PSI)")
    ax1.legend()

    x = np.arange(len(df_anom_ctrl))
    w = 0.35
    ax2.bar(x - w / 2, df_anom_ctrl["taux_detection_rappel_pct"], w, label="Rappel / Détection (%)", color="#2563eb")
    ax2.bar(x + w / 2, df_anom_ctrl["precision_pct"], w, label="Précision (%)", color="#16a34a")
    ax2.set_xticks(x)
    ax2.set_xticklabels(df_anom_ctrl["methode"])
    ax2.set_ylim(0, 110)
    ax2.set_ylabel("Pourcentage (%)")
    ax2.set_title("Anomalies contrôlées : Z-Score vs Isolation Forest")
    ax2.legend()
    ax2.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "exp4_anomaly_and_drift.png", dpi=130)
    plt.close()
    print("  ✓ Graphique Expérience 4 sauvegardé : exp4_anomaly_and_drift.png")


def main():
    print("=" * 65)
    print("EXPÉRIENCE 4 — Anomalies & Data Drift (Réel + Contrôlé)")
    print("=" * 65)

    spark = creer_session_spark()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(PARQUET_SRC))

    cols_cast = [F.col(c).cast(DoubleType()).alias(c) for c in COLONNES_NUM]
    df_num = df.select(["year"] + cols_cast)

    print("\n[1/4] Échantillonnage stratifié (5 %) pour 2016–2019 et 2020–2023…")
    df_ref_pd = df_num.filter(F.col("year").between(2016, 2019)).sample(fraction=0.05, seed=42).toPandas()
    df_cib_pd = df_num.filter(F.col("year").between(2020, 2023)).sample(fraction=0.05, seed=42).toPandas()
    df_all_pd = pd.concat([df_ref_pd, df_cib_pd], ignore_index=True)
    print(f"  → Référence 2016-2019 : {len(df_ref_pd):,} lignes | Cible 2020-2023 : {len(df_cib_pd):,} lignes")

    print("\n[2/4] Partie A — Comparaison Z-Score vs Isolation Forest…")
    df_anom_reel = comparer_anomalies_reel(df_all_pd)
    df_anom_ctrl = evaluer_anomalies_controlees(df_all_pd)
    df_anom_reel.to_csv(METRICS_DIR / "exp4_anomalies_reel.csv", index=False, encoding="utf-8")
    df_anom_ctrl.to_csv(METRICS_DIR / "exp4_anomalies_controlees.csv", index=False, encoding="utf-8")
    print("\n  Sur données réelles (5 %):")
    print(df_anom_reel.to_string(index=False))
    print("\n  Sur jeu contrôlé (100 anomalies injectées):")
    print(df_anom_ctrl.to_string(index=False))

    print("\n[3/4] Partie B — Data Drift 2016–2019 vs 2020–2023…")
    df_drift_periodes = evaluer_drift_periodes(df_ref_pd, df_cib_pd)
    df_drift_periodes.to_csv(METRICS_DIR / "exp4_drift_2016_2019_vs_2020_2023.csv", index=False, encoding="utf-8")
    print(df_drift_periodes.to_string(index=False))

    print("\n[4/4] Partie B — Validation sur drift artificiel contrôlé…")
    df_drift_art = evaluer_drift_artificiel(df_ref_pd)
    df_drift_art.to_csv(METRICS_DIR / "exp4_drift_artificiel.csv", index=False, encoding="utf-8")
    print(df_drift_art.to_string(index=False))

    tracer_graphiques(df_drift_periodes, df_anom_ctrl)
    spark.stop()


if __name__ == "__main__":
    main()
