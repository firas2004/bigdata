"""
src/rules/quality_runner.py
============================
Orchestrateur principal des règles de qualité — Partie 2.

Lance les 5 dimensions de qualité dans l'ordre, agrège les résultats,
calcule le score global et génère le rapport de synthèse.

Usage :
    python src/rules/quality_runner.py

Auteur : Big Data Observability Project
"""

import os
import sys
import time
from pathlib import Path

# --- HADOOP_HOME pour Windows (avant import pyspark) ---
_RACINE      = Path(__file__).resolve().parents[2]
_HADOOP_HOME = str(_RACINE / "hadoop")
os.environ["HADOOP_HOME"]     = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
_bin = str(Path(_HADOOP_HOME) / "bin")
if _bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _bin + os.pathsep + os.environ.get("PATH", "")

import pandas as pd
from pyspark.sql import SparkSession
import yaml

# Ajout du src au path pour les imports relatifs
sys.path.insert(0, str(_RACINE))

from src.rules import completude, unicite, validite, coherence, conformite

DIR_METRICS   = _RACINE / "data" / "metrics" / "rules"
CHEMIN_PARQUET = _RACINE / "data" / "parquet" / "us_accidents"
CHEMIN_THRESH  = _RACINE / "config" / "thresholds.yaml"


def creer_spark() -> SparkSession:
    with open(CHEMIN_THRESH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    sp = cfg.get("performance", {}).get("spark", {})

    spark = (
        SparkSession.builder
        .appName("US_Accidents_QualityRules")
        .master("local[*]")
        .config("spark.driver.memory", sp.get("driver_memory", "4g"))
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.shuffle.partitions", str(sp.get("shuffle_partitions", 8)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def score_qualite_global(resultats: dict) -> pd.DataFrame:
    """
    Agrège tous les résultats et calcule le score global par dimension et global.
    """
    frames = []
    for dim, df in resultats.items():
        if df is not None and not df.empty and "pct_ok" in df.columns:
            df = df.copy()
            df["dimension"] = dim
            frames.append(df)

    df_all = pd.concat(frames, ignore_index=True)

    # Score par dimension
    scores_dim = (
        df_all.groupby("dimension")["pct_ok"]
              .mean()
              .round(2)
              .reset_index()
              .rename(columns={"pct_ok": "score_moyen_pct"})
    )

    score_global = df_all["pct_ok"].mean().round(2)

    print("\n" + "=" * 55)
    print("  SCORES DE QUALITÉ PAR DIMENSION")
    print("=" * 55)
    for _, row in scores_dim.iterrows():
        icone = "✅" if row["score_moyen_pct"] >= 95 else "⚠️ " if row["score_moyen_pct"] >= 80 else "❌"
        print(f"  {icone} {row['dimension']:15s} : {row['score_moyen_pct']:6.2f} %")

    print("-" * 55)
    icone_g = "✅" if score_global >= 95 else "⚠️ " if score_global >= 80 else "❌"
    print(f"  {icone_g} SCORE GLOBAL    : {score_global:6.2f} %")
    print("=" * 55)

    # Tableau des règles KO
    df_ko = df_all[df_all["statut"].isin(["KO", "WARNING"])][
        ["dimension", "regle", "colonne", "nb_ko", "pct_ok", "statut"]
    ].sort_values("pct_ok")

    if not df_ko.empty:
        print("\n⚠️  Règles en échec ou warning :")
        print(df_ko.to_string(index=False))

    return df_all, scores_dim, score_global


def main():
    print("=" * 60)
    print("  PARTIE 2 — DATA QUALITY RULES | US Accidents")
    print("=" * 60)

    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    spark = creer_spark()
    print(f"[SPARK] Version {spark.version} démarrée")

    df = spark.read.parquet(str(CHEMIN_PARQUET))
    nb_lignes = df.count()
    print(f"[DATA]  {nb_lignes:,} lignes chargées depuis Parquet\n")

    t0 = time.time()
    resultats = {}

    # --- 1. Complétude ---
    print("\n" + "─" * 50)
    print("  DIMENSION 1 — COMPLÉTUDE")
    print("─" * 50)
    resultats["completude"] = completude.run(df)

    # --- 2. Unicité ---
    print("\n" + "─" * 50)
    print("  DIMENSION 2 — UNICITÉ")
    print("─" * 50)
    resultats["unicite"] = unicite.run(df)

    # --- 3. Validité ---
    print("\n" + "─" * 50)
    print("  DIMENSION 3 — VALIDITÉ")
    print("─" * 50)
    resultats["validite"] = validite.run(df)

    # --- 4. Cohérence ---
    print("\n" + "─" * 50)
    print("  DIMENSION 4 — COHÉRENCE")
    print("─" * 50)
    resultats["coherence"] = coherence.run(df)

    # --- 5. Conformité ---
    print("\n" + "─" * 50)
    print("  DIMENSION 5 — CONFORMITÉ")
    print("─" * 50)
    resultats["conformite"] = conformite.run(df)

    # --- Synthèse globale ---
    print("\n" + "─" * 50)
    df_all, scores_dim, score_global = score_qualite_global(resultats)

    # Sauvegarde rapport complet
    chemin_rapport = DIR_METRICS / "rapport_qualite_complet.csv"
    df_all.to_csv(chemin_rapport, index=False, encoding="utf-8")

    chemin_scores = DIR_METRICS / "scores_par_dimension.csv"
    scores_dim.to_csv(chemin_scores, index=False, encoding="utf-8")

    t_total = time.time() - t0
    print(f"\n[TERMINÉ] Partie 2 complète en {t_total:.1f}s ({t_total/60:.1f} min)")
    print(f"[FICHIERS] → {DIR_METRICS}")

    spark.stop()


if __name__ == "__main__":
    main()
