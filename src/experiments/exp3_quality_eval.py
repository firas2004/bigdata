"""
exp3_quality_eval.py
--------------------
Expérience 3 — Évaluation des règles de qualité par injection d'erreurs contrôlées.

Protocole :
  1. Extraction d'une copie propre de test (10 000 lignes valides sur toutes les règles)
     sans modifier le dataset original.
  2. Vérification sur le jeu sain (mesure des faux positifs = 0).
  3. Injection d'erreurs artificielles connues sur les 5 dimensions :
       - Complétude  : 50 valeurs manquantes dans State (colonne obligatoire)
       - Validité    : 40 valeurs hors domaine dans Temperature(F) (= 999.0)
       - Cohérence   : 30 lignes avec End_Time antérieur à Start_Time
       - Unicité     : 25 duplications d'identifiant ID
       - Conformité  : 20 valeurs Country != 'US' ('FR')
  4. Exécution des règles existantes (src/rules/) et calcul du taux de détection
     et des faux positifs.
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

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.rules import completude, unicite, validite, coherence, conformite

warnings.filterwarnings("ignore")

PARQUET_SRC = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
METRICS_DIR = _PROJET_ROOT / "data" / "metrics" / "experiments"
FIGURES_DIR = _PROJET_ROOT / "reports" / "figures"

METRICS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def creer_session_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BigDataObs_Exp3_QualityEval")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def construire_copie_saine(spark: SparkSession, n_lignes: int = 10000):
    """
    Extrait une copie de test de n_lignes 100% conformes aux 5 dimensions évaluées
    (afin de garantir une baseline propre sans erreur résiduelle).
    """
    df = spark.read.parquet(str(PARQUET_SRC))
    df_clean = (
        df.filter(
            F.col("ID").isNotNull()
            & F.col("Severity").isNotNull()
            & F.col("Start_Time").isNotNull()
            & F.col("End_Time").isNotNull()
            & F.col("Start_Lat").isNotNull()
            & F.col("Start_Lng").isNotNull()
            & F.col("State").isNotNull()
            & (F.col("Country") == "US")
            & (F.col("End_Time") >= F.col("Start_Time"))
            & F.col("Temperature(F)").between(-60, 140)
        )
        .limit(n_lignes)
        .withColumn("_row_idx", F.monotonically_increasing_id())
    )
    # Numérotation séquentielle exacte 0 .. n_lignes-1
    w = Window.orderBy("_row_idx")
    df_indexed = df_clean.withColumn("_idx", F.row_number().over(w) - 1).drop("_row_idx").cache()
    _ = df_indexed.count()
    return df_indexed


def evaluer_jeu(df_test, rules, thresh) -> dict:
    """Exécute les 5 règles ciblées du projet et retourne le nombre d'erreurs détectées."""
    r_comp = completude.regle_colonnes_obligatoires(df_test, rules)
    ko_comp = int(r_comp.loc[r_comp["colonne"] == "State", "nb_ko"].values[0])

    r_val = validite.regle_plages_numeriques(df_test, rules)
    ko_val = int(r_val.loc[r_val["colonne"] == "Temperature(F)", "nb_hors_bornes"].values[0])

    r_coh = coherence.regle_chronologie(df_test, rules, thresh)
    ko_coh = int(r_coh.loc[r_coh["regle"] == "R8a_coherence_end_apres_start", "nb_ko"].values[0])

    r_uni = unicite.regle_cle_primaire(df_test, rules)
    ko_uni = int(r_uni["nb_doublons"].values[0])

    r_conf = conformite.regle_country(df_test, rules)
    ko_conf = int(r_conf["nb_ko"].values[0])

    return {
        "Complétude": ko_comp,
        "Validité": ko_val,
        "Cohérence": ko_coh,
        "Unicité": ko_uni,
        "Conformité": ko_conf,
    }


def main():
    print("=" * 65)
    print("EXPÉRIENCE 3 — Évaluation des règles de qualité (erreurs injectées)")
    print("=" * 65)

    spark = creer_session_spark()
    spark.sparkContext.setLogLevel("ERROR")
    rules, thresh = completude.charger_config()

    print("\n[1/3] Construction d'une copie de test saine (10 000 lignes)…")
    df_sain = construire_copie_saine(spark, n_lignes=10000)

    print("\n[2/3] Vérification sur le jeu sain (contrôle faux positifs baseline)…")
    baseline_ko = evaluer_jeu(df_sain, rules, thresh)
    print("  Erreurs sur jeu sain :", baseline_ko)

    # Injection d'erreurs contrôlées sur des tranches disjointes de lignes
    injections = {
        "Complétude": 50,   # _idx 0..49 -> State = None
        "Validité": 40,     # _idx 50..89 -> Temperature(F) = 999.0
        "Cohérence": 30,    # _idx 90..119 -> End_Time = Start_Time - 1h
        "Unicité": 25,      # duplication des _idx 120..144
        "Conformité": 20,   # _idx 145..164 -> Country = 'FR'
    }

    df_corrompu = (
        df_sain
        .withColumn("State", F.when(F.col("_idx") < 50, F.lit(None)).otherwise(F.col("State")))
        .withColumn(
            "Temperature(F)",
            F.when((F.col("_idx") >= 50) & (F.col("_idx") < 90), F.lit(999.0)).otherwise(F.col("Temperature(F)")),
        )
        .withColumn(
            "End_Time",
            F.when((F.col("_idx") >= 90) & (F.col("_idx") < 120), F.expr("Start_Time - INTERVAL 1 HOUR")).otherwise(F.col("End_Time")),
        )
        .withColumn(
            "Country",
            F.when((F.col("_idx") >= 145) & (F.col("_idx") < 165), F.lit("FR")).otherwise(F.col("Country")),
        )
    )

    df_dups = df_corrompu.filter((F.col("_idx") >= 120) & (F.col("_idx") < 145))
    df_corrompu = df_corrompu.union(df_dups).drop("_idx").cache()
    _ = df_corrompu.count()

    print("\n[3/3] Exécution des règles sur le jeu corrompu…")
    detectees = evaluer_jeu(df_corrompu, rules, thresh)

    lignes = []
    for dim, inj in injections.items():
        det = detectees[dim]
        non_det = max(0, inj - det)
        faux_pos = max(0, det - inj)
        taux_det = round((min(det, inj) / inj) * 100, 2) if inj > 0 else 100.0
        lignes.append({
            "dimension": dim,
            "erreurs_injectees": inj,
            "erreurs_detectees": det,
            "erreurs_non_detectees": non_det,
            "faux_positifs": faux_pos,
            "taux_detection_pct": taux_det,
        })

    df_eval = pd.DataFrame(lignes)
    out_csv = METRICS_DIR / "exp3_quality_eval.csv"
    df_eval.to_csv(out_csv, index=False, encoding="utf-8")

    print("\n=== Résultats Expérience 3 ===")
    print(df_eval.to_string(index=False))
    print(f"\n  ✓ Résultats sauvegardés : {out_csv}")

    # Graphique
    fig, ax = plt.subplots(figsize=(9, 4.5))
    x = range(len(df_eval))
    w = 0.35
    ax.bar([i - w / 2 for i in x], df_eval["erreurs_injectees"], w, label="Erreurs injectées", color="#64748b")
    ax.bar([i + w / 2 for i in x], df_eval["erreurs_detectees"], w, label="Erreurs détectées", color="#16a34a")
    ax.set_xticks(list(x))
    ax.set_xticklabels(df_eval["dimension"])
    ax.set_ylabel("Nombre d'erreurs")
    ax.set_title("Expérience 3 — Évaluation des règles de qualité (Injectées vs Détectées)")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "exp3_quality_eval.png", dpi=130)
    plt.close()

    spark.stop()


if __name__ == "__main__":
    main()
