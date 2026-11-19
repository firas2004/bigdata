"""
microbatch_processor.py
-----------------------
Partie 4 — Big Data Processing : traitement en micro-batches et mesure de performance.

Stratégie :
  - Découpe le Parquet en N micro-batches (par année ou par tranche fixe)
  - Pour chaque batch, applique une pipeline de transformations représentative :
      * filtrage des lignes invalides (End_Time < Start_Time)
      * calcul de la durée (minutes)
      * agrégation : comptage, sévérité moyenne, durée moyenne par State
      * enrichissement : flag "nuit" (Sunrise_Sunset == Night)
  - Mesure le temps de traitement et le débit (rows/sec) par batch
  - Compare traitement séquentiel vs parallèle (ThreadPoolExecutor)
  - Exporte les métriques de performance dans data/metrics/performance/
  - Génère des graphiques de débit et de comparaison

Résultats exportés :
  data/metrics/performance/perf_sequentiel.csv
  data/metrics/performance/perf_parallele.csv
  data/metrics/performance/comparaison_modes.csv
  reports/figures/perf_debit_par_batch.png
  reports/figures/perf_comparaison.png
  reports/figures/perf_agregation_par_state.png
"""

import os
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

warnings.filterwarnings("ignore")

# ── Chemins ─────────────────────────────────────────────────────────────────────
PARQUET_DIR  = _PROJET_ROOT / "data" / "parquet" / "us_accidents"
BATCHES_DIR  = _PROJET_ROOT / "data" / "batches"
METRICS_DIR  = _PROJET_ROOT / "data" / "metrics" / "performance"
FIGURES_DIR  = _PROJET_ROOT / "reports" / "figures"
CONFIG_FILE  = _PROJET_ROOT / "config" / "thresholds.yaml"

BATCHES_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ── Années disponibles dans le dataset ──────────────────────────────────────────
ANNEES_DATASET = list(range(2016, 2024))   # 2016 → 2023


def charger_config() -> dict:
    """Charge la configuration depuis thresholds.yaml."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def creer_session_spark(driver_memory: str = "6g", app_name: str = "BigDataObs_MicroBatch") -> SparkSession:
    """Crée et retourne une session Spark optimisée pour la machine locale."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


# ════════════════════════════════════════════════════════════════════════════════
# Découpage en micro-batches par année
# ════════════════════════════════════════════════════════════════════════════════

def decouper_en_batches(spark: SparkSession) -> list:
    """
    Charge le Parquet et découpe par année dans data/batches/.
    Retourne la liste des chemins de batches créés avec leur taille.
    """
    print("  → Lecture du Parquet global…")
    df = spark.read.parquet(str(PARQUET_DIR))

    batches_info = []
    for annee in ANNEES_DATASET:
        dest = BATCHES_DIR / f"batch_{annee}"
        if dest.exists() and any(dest.glob("*.parquet")):
            # Batch déjà créé → on mesure juste la taille
            nb = spark.read.parquet(str(dest)).count()
            batches_info.append({
                "batch_id":  f"batch_{annee}",
                "annee":     annee,
                "chemin":    str(dest),
                "nb_lignes": nb,
            })
            print(f"     batch_{annee} : déjà existant ({nb:,} lignes)")
            continue

        df_annee = df.filter(F.col("year") == annee)
        nb = df_annee.count()
        if nb == 0:
            print(f"     batch_{annee} : aucune donnée, ignoré")
            continue

        df_annee.write.mode("overwrite").parquet(str(dest))
        batches_info.append({
            "batch_id":  f"batch_{annee}",
            "annee":     annee,
            "chemin":    str(dest),
            "nb_lignes": nb,
        })
        print(f"     batch_{annee} : {nb:,} lignes → {dest.name}")

    return batches_info


# ════════════════════════════════════════════════════════════════════════════════
# Pipeline de transformation (appliquée à chaque batch)
# ════════════════════════════════════════════════════════════════════════════════

def pipeline_transformation(spark: SparkSession, chemin_batch: str) -> dict:
    """
    Charge un batch Parquet et applique une pipeline de transformations :
      1. Lecture
      2. Filtrage cohérence temporelle
      3. Calcul de la durée en minutes
      4. Flag nuit / jour
      5. Agrégation par State : count, sévérité moyenne, durée moyenne
    Retourne un dict avec les métriques de performance.
    """
    t_debut = time.perf_counter()

    # ── Lecture ──────────────────────────────────────────────────────────────────
    df = spark.read.parquet(chemin_batch)
    t_lecture = time.perf_counter()

    # ── Filtrage cohérence temporelle ────────────────────────────────────────────
    df_filtre = df.filter(
        F.col("End_Time").isNotNull() &
        F.col("Start_Time").isNotNull() &
        (F.col("End_Time") > F.col("Start_Time"))
    )
    nb_avant  = df.count()
    nb_apres  = df_filtre.count()
    nb_filtre = nb_avant - nb_apres
    t_filtrage = time.perf_counter()

    # ── Calcul durée (minutes) ───────────────────────────────────────────────────
    df_enrichi = df_filtre.withColumn(
        "duree_minutes",
        (F.unix_timestamp("End_Time") - F.unix_timestamp("Start_Time")) / 60.0
    ).withColumn(
        # Flag nuit/jour basé sur Sunrise_Sunset
        "flag_nuit",
        F.when(F.col("Sunrise_Sunset") == "Night", 1).otherwise(0)
    ).withColumn(
        # Sévérité en entier
        "Severity_int",
        F.col("Severity").cast(LongType())
    )
    t_enrichissement = time.perf_counter()

    # ── Agrégation par State ─────────────────────────────────────────────────────
    agg_state = (
        df_enrichi
        .groupBy("State")
        .agg(
            F.count("*").alias("nb_accidents"),
            F.avg("Severity_int").alias("severite_moyenne"),
            F.avg("duree_minutes").alias("duree_moyenne_min"),
            F.sum("flag_nuit").alias("nb_accidents_nuit"),
            F.avg(F.col("Temperature(F)").cast(DoubleType())).alias("temp_moy_F"),
        )
        .orderBy(F.desc("nb_accidents"))
    )
    # Force l'évaluation (action)
    nb_states = agg_state.count()
    t_agregation = time.perf_counter()

    # ── Métriques de performance ─────────────────────────────────────────────────
    t_total = t_agregation - t_debut
    debit   = nb_avant / t_total if t_total > 0 else 0

    metriques = {
        "chemin":              chemin_batch,
        "nb_lignes":           nb_avant,
        "nb_lignes_apres_filtre": nb_apres,
        "nb_lignes_filtrees":  nb_filtre,
        "nb_states":           nb_states,
        "t_lecture_s":         round(t_lecture - t_debut,          3),
        "t_filtrage_s":        round(t_filtrage - t_lecture,        3),
        "t_enrichissement_s":  round(t_enrichissement - t_filtrage, 3),
        "t_agregation_s":      round(t_agregation - t_enrichissement, 3),
        "t_total_s":           round(t_total,                        3),
        "debit_rows_par_sec":  round(debit,                          1),
    }

    # On retourne aussi le DataFrame agrégé pour la visualisation
    return metriques, agg_state.toPandas()


# ════════════════════════════════════════════════════════════════════════════════
# Traitement séquentiel
# ════════════════════════════════════════════════════════════════════════════════

def traitement_sequentiel(spark: SparkSession, batches_info: list) -> tuple:
    """
    Traite les batches un par un et collecte les métriques.
    """
    print(f"\n  → Traitement séquentiel de {len(batches_info)} batches…")
    t0 = time.perf_counter()

    resultats     = []
    aggs_all      = []

    for info in batches_info:
        bid = info["batch_id"]
        print(f"     {bid} ({info['nb_lignes']:,} lignes)…", end=" ", flush=True)
        t_b = time.perf_counter()
        metr, agg_pd = pipeline_transformation(spark, info["chemin"])
        metr["batch_id"] = bid
        metr["annee"]    = info["annee"]
        resultats.append(metr)
        agg_pd["batch_id"] = bid
        agg_pd["annee"]    = info["annee"]
        aggs_all.append(agg_pd)
        print(f"{metr['t_total_s']:.1f}s — {metr['debit_rows_par_sec']:,.0f} rows/s")

    t_total_seq = time.perf_counter() - t0
    df_perf     = pd.DataFrame(resultats)
    df_aggs     = pd.concat(aggs_all, ignore_index=True) if aggs_all else pd.DataFrame()

    print(f"\n  Séquentiel total : {t_total_seq:.1f}s")
    print(f"  Débit moyen : {df_perf['debit_rows_par_sec'].mean():,.0f} rows/s")
    print(f"  Débit max   : {df_perf['debit_rows_par_sec'].max():,.0f} rows/s (batch {df_perf.loc[df_perf['debit_rows_par_sec'].idxmax(),'batch_id']})")

    return df_perf, df_aggs, t_total_seq


# ════════════════════════════════════════════════════════════════════════════════
# Traitement parallèle (simulation via ThreadPoolExecutor)
# Note : Spark est déjà parallèle en interne (local[*]).
# Ici on simule le parallélisme au niveau des batches (workers Python).
# On utilise un seul SparkSession partagé → les workers s'exécutent en fait
# de façon concurrente mais partagent les ressources Spark.
# ════════════════════════════════════════════════════════════════════════════════

def traitement_parallele(spark: SparkSession, batches_info: list, max_workers: int = 2) -> tuple:
    """
    Traite les batches en parallèle avec ThreadPoolExecutor.
    max_workers=2 : on limite pour ne pas saturer la RAM sur machine locale.
    """
    print(f"\n  → Traitement parallèle ({max_workers} workers) de {len(batches_info)} batches…")
    t0 = time.perf_counter()

    resultats = {}
    aggs_all  = {}

    def _traiter(info):
        metr, agg_pd = pipeline_transformation(spark, info["chemin"])
        metr["batch_id"] = info["batch_id"]
        metr["annee"]    = info["annee"]
        agg_pd["batch_id"] = info["batch_id"]
        agg_pd["annee"]    = info["annee"]
        return info["batch_id"], metr, agg_pd

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_traiter, info): info["batch_id"] for info in batches_info}
        for fut in as_completed(futures):
            bid, metr, agg_pd = fut.result()
            resultats[bid] = metr
            aggs_all[bid]  = agg_pd
            print(f"     {bid} terminé : {metr['t_total_s']:.1f}s — {metr['debit_rows_par_sec']:,.0f} rows/s")

    t_total_par = time.perf_counter() - t0
    df_perf     = pd.DataFrame(list(resultats.values())).sort_values("annee")
    df_aggs     = pd.concat(list(aggs_all.values()), ignore_index=True) if aggs_all else pd.DataFrame()

    print(f"\n  Parallèle total : {t_total_par:.1f}s")
    print(f"  Débit moyen : {df_perf['debit_rows_par_sec'].mean():,.0f} rows/s")

    return df_perf, df_aggs, t_total_par


# ════════════════════════════════════════════════════════════════════════════════
# Visualisations
# ════════════════════════════════════════════════════════════════════════════════

def tracer_debit_par_batch(df_seq: pd.DataFrame, df_par: pd.DataFrame):
    """Bar chart comparatif débit séquentiel vs parallèle par batch."""
    batches = df_seq["batch_id"].tolist()
    x = np.arange(len(batches))
    w = 0.35

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(x - w/2, df_seq["debit_rows_par_sec"], w, label="Séquentiel", color="steelblue", alpha=0.85)

    # Tri df_par dans le même ordre
    df_par_sorted = df_par.set_index("batch_id").reindex(batches).reset_index()
    ax.bar(x + w/2, df_par_sorted["debit_rows_par_sec"], w, label="Parallèle", color="darkorange", alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(batches, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Débit (rows/sec)")
    ax.set_title("Débit de traitement par batch — Séquentiel vs Parallèle", fontsize=11)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.legend()
    plt.tight_layout()
    out = FIGURES_DIR / "perf_debit_par_batch.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Débit par batch sauvegardé : {out.name}")


def tracer_comparaison_temps(df_seq: pd.DataFrame, df_par: pd.DataFrame):
    """Bar chart empilé des temps par étape (séquentiel seulement)."""
    etapes = ["t_lecture_s", "t_filtrage_s", "t_enrichissement_s", "t_agregation_s"]
    labels = ["Lecture", "Filtrage", "Enrichissement", "Agrégation"]
    couleurs = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759"]

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(df_seq))

    for etape, label, couleur in zip(etapes, labels, couleurs):
        vals = df_seq[etape].values
        ax.bar(df_seq["batch_id"], vals, bottom=bottom, label=label, color=couleur, alpha=0.85)
        bottom += vals

    ax.set_ylabel("Temps (secondes)")
    ax.set_title("Décomposition du temps de traitement par batch (séquentiel)", fontsize=11)
    ax.legend(loc="upper left")
    plt.xticks(rotation=30, ha="right", fontsize=9)
    plt.tight_layout()
    out = FIGURES_DIR / "perf_decomposition_temps.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Décomposition temps sauvegardée : {out.name}")


def tracer_agregation_top_states(df_aggs: pd.DataFrame):
    """Bar chart horizontal : top 15 États par nombre d'accidents (global)."""
    if df_aggs.empty:
        return

    top_states = (
        df_aggs.groupby("State")["nb_accidents"]
        .sum()
        .sort_values(ascending=True)
        .tail(15)
    )

    fig, ax = plt.subplots(figsize=(8, 6))
    couleurs = ["#e15759" if s == "CA" else "steelblue" for s in top_states.index]
    ax.barh(top_states.index, top_states.values, color=couleurs)
    ax.set_title("Top 15 États — Nombre total d'accidents (2016-2023)", fontsize=11)
    ax.set_xlabel("Nombre d'accidents")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    plt.tight_layout()
    out = FIGURES_DIR / "perf_top_states.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Top États sauvegardé : {out.name}")


def tracer_nb_lignes_par_batch(batches_info: list):
    """Bar chart : nombre de lignes par batch (par année)."""
    df_b = pd.DataFrame(batches_info).sort_values("annee")

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(df_b["batch_id"], df_b["nb_lignes"], color="steelblue", alpha=0.85)
    ax.set_title("Nombre d'enregistrements par batch (par année)", fontsize=11)
    ax.set_ylabel("Nombre de lignes")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    plt.xticks(rotation=30, ha="right", fontsize=9)
    plt.tight_layout()
    out = FIGURES_DIR / "perf_lignes_par_batch.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Lignes par batch sauvegardé : {out.name}")


# ════════════════════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════════════════════

def main():
    t0_global = time.time()
    print("=" * 60)
    print("PARTIE 4 — Big Data Processing (Micro-batches)")
    print("=" * 60)

    # Config
    cfg        = charger_config()
    driver_mem = cfg.get("spark", {}).get("driver_memory", "6g")
    print(f"\n  Config : driver_memory={driver_mem}")

    # Spark
    print("\n[1/5] Démarrage de Spark…")
    spark = creer_session_spark(driver_mem)
    spark.sparkContext.setLogLevel("ERROR")

    # Découpage en batches
    print("\n[2/5] Découpage en micro-batches par année…")
    batches_info = decouper_en_batches(spark)
    nb_total     = sum(b["nb_lignes"] for b in batches_info)
    print(f"\n  → {len(batches_info)} batches créés, {nb_total:,} lignes au total")
    tracer_nb_lignes_par_batch(batches_info)

    # Traitement séquentiel
    print("\n[3/5] Traitement séquentiel…")
    df_seq, df_aggs_seq, t_seq = traitement_sequentiel(spark, batches_info)
    out_seq = METRICS_DIR / "perf_sequentiel.csv"
    df_seq.to_csv(out_seq, index=False, encoding="utf-8")
    print(f"  ✓ Métriques séquentiel exportées : {out_seq.name}")

    # Traitement parallèle
    print("\n[4/5] Traitement parallèle (2 workers)…")
    df_par, df_aggs_par, t_par = traitement_parallele(spark, batches_info, max_workers=2)
    out_par = METRICS_DIR / "perf_parallele.csv"
    df_par.to_csv(out_par, index=False, encoding="utf-8")
    print(f"  ✓ Métriques parallèle exportées : {out_par.name}")

    # Comparaison
    gain = (t_seq - t_par) / t_seq * 100 if t_seq > 0 else 0
    df_comp = pd.DataFrame([
        {"mode": "séquentiel", "t_total_s": round(t_seq, 2),
         "debit_moyen_rows_s": round(df_seq["debit_rows_par_sec"].mean(), 1),
         "debit_max_rows_s":   round(df_seq["debit_rows_par_sec"].max(), 1)},
        {"mode": "parallèle",  "t_total_s": round(t_par, 2),
         "debit_moyen_rows_s": round(df_par["debit_rows_par_sec"].mean(), 1),
         "debit_max_rows_s":   round(df_par["debit_rows_par_sec"].max(), 1)},
    ])
    out_comp = METRICS_DIR / "comparaison_modes.csv"
    df_comp.to_csv(out_comp, index=False, encoding="utf-8")

    print(f"\n  ═══ Comparaison ═══")
    print(f"  Séquentiel  : {t_seq:.1f}s — débit moy {df_seq['debit_rows_par_sec'].mean():,.0f} rows/s")
    print(f"  Parallèle   : {t_par:.1f}s — débit moy {df_par['debit_rows_par_sec'].mean():,.0f} rows/s")
    print(f"  Gain        : {gain:.1f}% ({'plus rapide' if gain > 0 else 'plus lent'} en parallèle)")
    print(df_comp.to_string(index=False))

    # Agrégation globale par State
    if not df_aggs_seq.empty:
        agg_global = (
            df_aggs_seq
            .groupby("State")
            .agg(
                nb_accidents=("nb_accidents", "sum"),
                severite_moyenne=("severite_moyenne", "mean"),
                duree_moyenne_min=("duree_moyenne_min", "mean"),
            )
            .reset_index()
            .sort_values("nb_accidents", ascending=False)
        )
        out_agg = METRICS_DIR / "agregation_par_state.csv"
        agg_global.to_csv(out_agg, index=False, encoding="utf-8")
        print(f"\n  ✓ Agrégation par State exportée : {out_agg.name}")
        print("\n  Top 10 États :")
        print(agg_global.head(10).to_string(index=False))

    # Visualisations
    print("\n[5/5] Génération des graphiques…")
    tracer_debit_par_batch(df_seq, df_par)
    tracer_comparaison_temps(df_seq, df_par)
    tracer_agregation_top_states(df_aggs_seq)

    spark.stop()
    duree_totale = time.time() - t0_global
    print(f"\n{'=' * 60}")
    print(f"✅ Partie 4 terminée en {duree_totale:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
