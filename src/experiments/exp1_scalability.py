"""
exp1_scalability.py
-------------------
Expérience 1 — Scalabilité selon le volume de données.

Protocole :
  1. Préparation hors-chronomètre de 4 versions déterministes (seed=42) :
       - V1 : 10 %
       - V2 : 25 %
       - V3 : 50 %
       - V4 : 100 %
  2. Exécution de la même pipeline PySpark sur chaque version :
       lecture -> filtrage cohérence -> enrichissement -> agrégation par État.
  3. Collecte des métriques :
       nb_lignes, taille_parquet_mo, temps_total_s, debit_rows_par_sec, memoire_ram_mo.
  4. Export CSV et génération des courbes (temps vs volume, débit vs volume).
"""

import os
import time
import warnings
from pathlib import Path

# ── Hadoop doit être défini AVANT tout import pyspark ──────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
_HADOOP_HOME = str(_PROJET_ROOT / "hadoop")
os.environ["HADOOP_HOME"] = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = str(Path(_HADOOP_HOME) / "bin") + os.pathsep + os.environ.get("PATH", "")

import psutil
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

warnings.filterwarnings("ignore")

# ── Chemins ─────────────────────────────────────────────────────────────────────
PARQUET_SRC = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
EXP_DATA_DIR = _PROJET_ROOT / "data" / "experiments"
METRICS_DIR = _PROJET_ROOT / "data" / "metrics" / "experiments"
FIGURES_DIR = _PROJET_ROOT / "reports" / "figures"

EXP_DATA_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

CONFIGURATIONS = [
    ("V1_10pct", 0.10),
    ("V2_25pct", 0.25),
    ("V3_50pct", 0.50),
    ("V4_100pct", 1.00),
]


def creer_session_spark(driver_memory: str = "6g") -> SparkSession:
    """Crée une session Spark configurée pour la machine locale."""
    return (
        SparkSession.builder
        .appName("BigDataObs_Exp1_Scalability")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def taille_dossier_mo(dossier: Path) -> float:
    """Calcule la taille totale des fichiers d'un dossier en Mo."""
    total_bytes = sum(f.stat().st_size for f in dossier.rglob("*") if f.is_file())
    return round(total_bytes / (1024 * 1024), 2)


def preparer_jeux_donnees(spark: SparkSession) -> dict:
    """
    Prépare les 4 jeux de données Parquet (V1..V4) avant toute mesure.
    Si un jeu existe déjà, on ne le régénère pas.
    """
    print("\n[Phase A] Préparation préalable des 4 jeux de données (hors-chronomètre)…")
    df_source = spark.read.parquet(str(PARQUET_SRC))
    chemins = {}

    for nom, fraction in CONFIGURATIONS:
        dest = EXP_DATA_DIR / f"scale_{nom}"
        chemins[nom] = dest
        if dest.exists() and any(dest.glob("*.parquet")):
            print(f"  ✓ {nom} déjà préparé ({taille_dossier_mo(dest):.1f} Mo)")
            continue

        print(f"  → Génération de {nom} (fraction={fraction})…", end=" ", flush=True)
        df_sub = df_source if fraction >= 1.0 else df_source.sample(withReplacement=False, fraction=fraction, seed=42)
        df_sub.write.mode("overwrite").parquet(str(dest))
        print(f"OK ({taille_dossier_mo(dest):.1f} Mo)")

    return chemins


def executer_pipeline_reference(spark: SparkSession, chemin_parquet: Path) -> dict:
    """
    Exécute la pipeline PySpark standard sur un jeu de données et mesure
    temps total, débit et utilisation mémoire.
    """
    process = psutil.Process(os.getpid())
    ram_avant_mo = psutil.virtual_memory().used / (1024 * 1024)

    t_debut = time.perf_counter()

    df = spark.read.parquet(str(chemin_parquet))
    nb_lignes = df.count()

    df_clean = (
        df.filter(
            F.col("End_Time").isNotNull()
            & F.col("Start_Time").isNotNull()
            & (F.col("End_Time") > F.col("Start_Time"))
        )
        .withColumn("duree_minutes", (F.unix_timestamp("End_Time") - F.unix_timestamp("Start_Time")) / 60.0)
        .withColumn("flag_nuit", F.when(F.col("Sunrise_Sunset") == "Night", 1).otherwise(0))
        .withColumn("Severity_int", F.col("Severity").cast(LongType()))
    )

    agg_state = (
        df_clean.groupBy("State")
        .agg(
            F.count("*").alias("nb_accidents"),
            F.avg("Severity_int").alias("severite_moy"),
            F.avg("duree_minutes").alias("duree_moy_min"),
            F.sum("flag_nuit").alias("nb_nuit"),
            F.avg(F.col("Temperature(F)").cast(DoubleType())).alias("temp_moy_F"),
        )
        .collect()
    )

    t_total = time.perf_counter() - t_debut
    ram_apres_mo = psutil.virtual_memory().used / (1024 * 1024)
    rss_py_mo = process.memory_info().rss / (1024 * 1024)
    debit = nb_lignes / t_total if t_total > 0 else 0.0

    return {
        "nb_lignes": nb_lignes,
        "nb_states": len(agg_state),
        "temps_total_s": round(t_total, 3),
        "debit_rows_par_sec": round(debit, 1),
        "ram_systeme_utilisee_mo": round(ram_apres_mo, 1),
        "delta_ram_mo": round(max(0.0, ram_apres_mo - ram_avant_mo), 1),
        "rss_python_mo": round(rss_py_mo, 1),
    }


def tracer_graphiques(df_res: pd.DataFrame):
    """Génère les courbes temps vs volume et débit vs volume."""
    fig, ax = plt.subplots(figsize=(9, 5))
    x_millions = df_res["nb_lignes"] / 1e6
    ax.plot(x_millions, df_res["temps_total_s"], marker="o", linewidth=2.5, color="#2563eb")
    for _, r in df_res.iterrows():
        ax.annotate(
            f"{r['config']}\n({r['temps_total_s']:.2f}s)",
            (r["nb_lignes"] / 1e6, r["temps_total_s"]),
            textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8
        )
    ax.set_title("Expérience 1 — Temps d'exécution selon le volume de données", fontsize=12)
    ax.set_xlabel("Volume traité (millions de lignes)")
    ax.set_ylabel("Temps total d'exécution (secondes)")
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "exp1_temps_vs_volume.png", dpi=130)
    plt.close()

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(x_millions, df_res["debit_rows_par_sec"], marker="s", linewidth=2.5, color="#16a34a")
    for _, r in df_res.iterrows():
        ax.annotate(
            f"{r['config']}\n({r['debit_rows_par_sec']:,.0f} rows/s)",
            (r["nb_lignes"] / 1e6, r["debit_rows_par_sec"]),
            textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8
        )
    ax.set_title("Expérience 1 — Débit de traitement selon le volume de données", fontsize=12)
    ax.set_xlabel("Volume traité (millions de lignes)")
    ax.set_ylabel("Débit (lignes / seconde)")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "exp1_debit_vs_volume.png", dpi=130)
    plt.close()
    print("  ✓ Graphiques Expérience 1 générés dans reports/figures/")


def main():
    print("=" * 65)
    print("EXPÉRIENCE 1 — Scalabilité selon le volume (V1=10% → V4=100%)")
    print("=" * 65)

    spark = creer_session_spark()
    spark.sparkContext.setLogLevel("ERROR")

    chemins = preparer_jeux_donnees(spark)

    # Warmup réel de la pipeline (compilations JIT / Catalyst codegen) avant mesures
    print("\n[Phase B] Warmup pipeline Spark (hors-chronomètre), puis benchmarks…")
    _ = executer_pipeline_reference(spark, chemins["V1_10pct"])

    resultats = []
    for nom, fraction in CONFIGURATIONS:
        chemin = chemins[nom]
        taille_mo = taille_dossier_mo(chemin)
        print(f"  → Exécution {nom} ({taille_mo:.1f} Mo)…", end=" ", flush=True)
        metr = executer_pipeline_reference(spark, chemin)
        metr["config"] = nom
        metr["fraction_pct"] = int(fraction * 100)
        metr["taille_parquet_mo"] = taille_mo
        resultats.append(metr)
        print(f"{metr['nb_lignes']:,} lignes en {metr['temps_total_s']:.2f}s → {metr['debit_rows_par_sec']:,.0f} rows/s")

    cols_ordre = [
        "config", "fraction_pct", "nb_lignes", "taille_parquet_mo",
        "temps_total_s", "debit_rows_par_sec", "ram_systeme_utilisee_mo", "delta_ram_mo", "rss_python_mo"
    ]
    df_res = pd.DataFrame(resultats)[cols_ordre]
    out_csv = METRICS_DIR / "exp1_scalability.csv"
    df_res.to_csv(out_csv, index=False, encoding="utf-8")

    print("\n=== Tableau comparatif Expérience 1 ===")
    print(df_res.to_string(index=False))
    print(f"\n  ✓ Résultats sauvegardés : {out_csv}")

    tracer_graphiques(df_res)
    spark.stop()


if __name__ == "__main__":
    main()
