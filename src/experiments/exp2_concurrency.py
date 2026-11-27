"""
exp2_concurrency.py
-------------------
Expérience 2 — Comparaison traitement séquentiel vs soumission concurrente des micro-batches.

Exécute les 8 batches annuels (2016–2023) en mode séquentiel (Mode A) puis en mode concurrent
avec deux workers Python partageant une unique SparkSession locale (Mode B). Les temps sont
mesurés sur les actions Spark réelles (évaluation différée exclue). CPU et RAM sont surveillés
via psutil pendant chaque mode.

Sorties : data/metrics/experiments/, reports/figures/
"""

import os
import time
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── Hadoop doit être défini AVANT tout import pyspark ──────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
_HADOOP_HOME = str(_PROJET_ROOT / "hadoop")
os.environ["HADOOP_HOME"] = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = str(Path(_HADOOP_HOME) / "bin") + os.pathsep + os.environ.get("PATH", "")

import psutil
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

warnings.filterwarnings("ignore")

BATCHES_DIR = _PROJET_ROOT / "data" / "batches"
METRICS_DIR = _PROJET_ROOT / "data" / "metrics" / "experiments"
FIGURES_DIR = _PROJET_ROOT / "reports" / "figures"

METRICS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
ANNEES = list(range(2016, 2024))


def creer_session_spark(driver_memory: str = "6g") -> SparkSession:
    return (
        SparkSession.builder
        .appName("BigDataObs_Exp2_Concurrency")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


class ResourceMonitor:
    """Moniteur léger CPU et RAM système en arrière-plan."""

    def __init__(self, interval_s: float = 0.25):
        self.interval_s = interval_s
        self._stop = threading.Event()
        self.cpu_samples = []
        self.ram_samples_mo = []
        self._thread = None

    def _run(self):
        psutil.cpu_percent(interval=None)
        while not self._stop.is_set():
            self.cpu_samples.append(psutil.cpu_percent(interval=self.interval_s))
            self.ram_samples_mo.append(psutil.virtual_memory().used / (1024 * 1024))

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def stats(self) -> dict:
        return {
            "cpu_moyen_pct": round(float(np.mean(self.cpu_samples)) if self.cpu_samples else 0.0, 1),
            "cpu_max_pct": round(float(np.max(self.cpu_samples)) if self.cpu_samples else 0.0, 1),
            "ram_pic_mo": round(float(np.max(self.ram_samples_mo)) if self.ram_samples_mo else 0.0, 1),
        }


def traiter_un_batch(spark: SparkSession, annee: int) -> dict:
    """
    Exécute la pipeline complète sur un batch annuel en garantissant que toutes
    les transformations (filtrage + enrichissement + agrégation) sont matérialisées
    avant la fin du chronomètre.
    """
    chemin = BATCHES_DIR / f"batch_{annee}"
    t_debut = time.perf_counter()

    df = spark.read.parquet(str(chemin))

    # Transformations paresseuses enchaînées
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

    # Action unique consolidant le comptage filtré et l'agrégation par État
    agg_rows = (
        df_clean.groupBy("State")
        .agg(
            F.count("*").alias("nb_accidents_valides"),
            F.avg("Severity_int").alias("severite_moyenne"),
            F.avg("duree_minutes").alias("duree_moyenne_min"),
            F.sum("flag_nuit").alias("nb_nuit"),
            F.avg(F.col("Temperature(F)").cast(DoubleType())).alias("temp_moy_F"),
        )
        .collect()
    )

    t_batch = time.perf_counter() - t_debut
    nb_lignes_valides = sum(r["nb_accidents_valides"] for r in agg_rows)
    debit = nb_lignes_valides / t_batch if t_batch > 0 else 0.0

    return {
        "batch_id": f"batch_{annee}",
        "annee": annee,
        "nb_lignes_traitees": nb_lignes_valides,
        "nb_states": len(agg_rows),
        "temps_batch_s": round(t_batch, 3),
        "debit_rows_par_sec": round(debit, 1),
    }


def executer_mode_a_sequentiel(spark: SparkSession) -> tuple:
    """Mode A : traitement séquentiel un batch après l'autre."""
    print("\n[Mode A] Exécution séquentielle (1 batch à la fois)…")
    resultats = []
    with ResourceMonitor() as mon:
        t0 = time.perf_counter()
        for annee in ANNEES:
            res = traiter_un_batch(spark, annee)
            res["mode"] = "Mode_A_Sequentiel"
            resultats.append(res)
            print(f"  ✓ {res['batch_id']} : {res['nb_lignes_traitees']:,} lignes en {res['temps_batch_s']:.2f}s")
        t_total = time.perf_counter() - t0
    return pd.DataFrame(resultats), t_total, mon.stats()


def executer_mode_b_concurrent(spark: SparkSession, max_workers: int = 2) -> tuple:
    """Mode B : soumission concurrente (2 workers Python sur 1 SparkSession partagée)."""
    print(f"\n[Mode B] Soumission concurrente ({max_workers} workers Python sur 1 SparkSession)…")
    resultats = []
    with ResourceMonitor() as mon:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(traiter_un_batch, spark, annee): annee for annee in ANNEES}
            for fut in as_completed(futures):
                res = fut.result()
                res["mode"] = "Mode_B_Concurrent_2workers"
                resultats.append(res)
                print(f"  ✓ {res['batch_id']} : {res['nb_lignes_traitees']:,} lignes en {res['temps_batch_s']:.2f}s")
        t_total = time.perf_counter() - t0
    df_res = pd.DataFrame(resultats).sort_values("annee").reset_index(drop=True)
    return df_res, t_total, mon.stats()


def tracer_comparaison(df_all: pd.DataFrame, t_seq: float, t_conc: float, gain: float):
    """Génère le graphique comparatif des temps par batch et temps total."""
    df_a = df_all[df_all["mode"] == "Mode_A_Sequentiel"].sort_values("annee")
    df_b = df_all[df_all["mode"] == "Mode_B_Concurrent_2workers"].sort_values("annee")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={"width_ratios": [2.2, 1]})

    x = np.arange(len(df_a))
    w = 0.36
    ax1.bar(x - w / 2, df_a["temps_batch_s"], w, label="Mode A : Séquentiel", color="#2563eb")
    ax1.bar(x + w / 2, df_b["temps_batch_s"], w, label="Mode B : Concurrent (2 workers)", color="#ea580c")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df_a["batch_id"], rotation=30, ha="right")
    ax1.set_ylabel("Temps par batch (s)")
    ax1.set_title("Temps individuel par batch (concurrence vs séquentiel)")
    ax1.legend()
    ax1.grid(axis="y", linestyle="--", alpha=0.4)

    bars = ax2.bar(
        ["Mode A\nSéquentiel", "Mode B\nConcurrent"],
        [t_seq, t_conc],
        color=["#2563eb", "#ea580c"],
        width=0.5,
    )
    for bar, val in zip(bars, [t_seq, t_conc]):
        ax2.text(bar.get_x() + bar.get_width() / 2, val + 0.15, f"{val:.2f}s", ha="center", fontweight="bold")
    ax2.set_ylabel("Temps total (s)")
    ax2.set_title(f"Temps total (Gain : {gain:+.1f} %)")
    ax2.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "exp2_seq_vs_concurrent.png", dpi=130)
    plt.close()
    print("  ✓ Graphique Expérience 2 sauvegardé : exp2_seq_vs_concurrent.png")


def main():
    print("=" * 65)
    print("EXPÉRIENCE 2 — Séquentiel vs Soumission Concurrente")
    print("=" * 65)

    spark = creer_session_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Warmup JVM Spark
    _ = spark.read.parquet(str(BATCHES_DIR / "batch_2023")).limit(100).count()

    df_a, t_seq, res_a = executer_mode_a_sequentiel(spark)
    df_b, t_conc, res_b = executer_mode_b_concurrent(spark, max_workers=2)

    nb_lignes_total = int(df_a["nb_lignes_traitees"].sum())
    debit_global_a = round(nb_lignes_total / t_seq, 1)
    debit_global_b = round(nb_lignes_total / t_conc, 1)
    gain_pct = round(((t_seq - t_conc) / t_seq) * 100, 2)

    df_batches = pd.concat([df_a, df_b], ignore_index=True)
    df_batches.to_csv(METRICS_DIR / "exp2_concurrency_batches.csv", index=False, encoding="utf-8")

    df_summary = pd.DataFrame([
        {
            "mode": "Mode A (Séquentiel)",
            "nb_lignes_total": nb_lignes_total,
            "temps_total_s": round(t_seq, 3),
            "debit_global_rows_s": debit_global_a,
            "cpu_moyen_pct": res_a["cpu_moyen_pct"],
            "cpu_max_pct": res_a["cpu_max_pct"],
            "ram_pic_mo": res_a["ram_pic_mo"],
            "gain_pct": 0.0,
        },
        {
            "mode": "Mode B (Concurrent 2 workers)",
            "nb_lignes_total": nb_lignes_total,
            "temps_total_s": round(t_conc, 3),
            "debit_global_rows_s": debit_global_b,
            "cpu_moyen_pct": res_b["cpu_moyen_pct"],
            "cpu_max_pct": res_b["cpu_max_pct"],
            "ram_pic_mo": res_b["ram_pic_mo"],
            "gain_pct": gain_pct,
        },
    ])
    df_summary.to_csv(METRICS_DIR / "exp2_concurrency_summary.csv", index=False, encoding="utf-8")

    print("\n=== Résumé Comparatif Expérience 2 ===")
    print(df_summary.to_string(index=False))
    tracer_comparaison(df_batches, t_seq, t_conc, gain_pct)
    spark.stop()


if __name__ == "__main__":
    main()
