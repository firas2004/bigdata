"""
src/profiling/profiler.py
==========================
Module principal de profilage des données US Accidents.

Toutes les fonctions de comptage et de calcul de taux opèrent sur Spark
(dataset complet). Les graphiques utilisent un échantillon converti en Pandas.

Auteur : Big Data Observability Project
"""

import os
import time
import yaml
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

# Pas de fenêtre graphique (mode fichier uniquement)
matplotlib.use("Agg")

# =============================================================================
# Chemins
# =============================================================================
RACINE          = Path(__file__).resolve().parents[2]
CHEMIN_PARQUET  = RACINE / "data" / "parquet" / "us_accidents"
DIR_METRICS     = RACINE / "data" / "metrics" / "profiling"
DIR_FIGURES     = RACINE / "reports" / "figures"
CHEMIN_CONFIG   = RACINE / "config" / "thresholds.yaml"

# Colonnes numériques analysées en Partie 1
COLS_NUMERIQUES = [
    "Temperature(F)", "Visibility(mi)", "Wind_Speed(mph)",
    "Distance(mi)", "Precipitation(in)", "Humidity(%)", "Pressure(in)"
]
# Colonnes catégorielles analysées en Partie 1
COLS_CATEGORIELLES = ["Severity", "State", "Weather_Condition", "Sunrise_Sunset"]

# Taux d'échantillonnage pour les graphiques (Pandas)
TAUX_ECHANTILLON = 0.05


def charger_config() -> dict:
    """Charge la configuration depuis thresholds.yaml."""
    with open(CHEMIN_CONFIG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def creer_session_spark(app_name: str = "US_Accidents_Profiling") -> SparkSession:
    """Crée une session Spark configurée pour le profiling local."""
    config = charger_config()
    cfg_spark = config.get("performance", {}).get("spark", {})

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", cfg_spark.get("driver_memory", "4g"))
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.shuffle.partitions", str(cfg_spark.get("shuffle_partitions", 8)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def charger_parquet(spark: SparkSession) -> DataFrame:
    """Charge le dataset Parquet US Accidents."""
    if not CHEMIN_PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet introuvable : {CHEMIN_PARQUET}\n"
            "→ Lancez d'abord : python src/pipeline/convert_to_parquet.py"
        )
    return spark.read.parquet(str(CHEMIN_PARQUET))


# =============================================================================
# 1. Vue d'ensemble
# =============================================================================
def vue_ensemble(df: DataFrame) -> dict:
    """
    Retourne le nombre de lignes et de colonnes du dataset.
    Affiche un résumé formaté.
    """
    nb_lignes  = df.count()
    nb_colonnes = len(df.columns)

    print(f"\n{'='*50}")
    print(f"  VUE D'ENSEMBLE")
    print(f"{'='*50}")
    print(f"  Nombre d'enregistrements : {nb_lignes:>12,}")
    print(f"  Nombre d'attributs       : {nb_colonnes:>12}")
    print(f"{'='*50}")

    return {"nb_lignes": nb_lignes, "nb_colonnes": nb_colonnes}


# =============================================================================
# 2. Schéma et types de données
# =============================================================================
def schema_et_types(df: DataFrame) -> pd.DataFrame:
    """
    Retourne un DataFrame Pandas décrivant le schéma :
    nom de colonne, type Spark, catégorie (numérique / catégorielle / temporelle / booléenne).
    """
    lignes = []
    for champ in df.schema.fields:
        nom  = champ.name
        type_spark = str(champ.dataType)
        # Catégorisation
        if isinstance(champ.dataType, NumericType):
            categorie = "numérique"
        elif type_spark == "TimestampType()":
            categorie = "temporelle"
        elif type_spark == "BooleanType()":
            categorie = "booléenne"
        else:
            categorie = "catégorielle / texte"

        lignes.append({
            "colonne": nom,
            "type_spark": type_spark.replace("Type()", "").replace("()", ""),
            "categorie": categorie,
            "nullable": champ.nullable
        })

    df_schema = pd.DataFrame(lignes)
    print("\n[SCHEMA] Extrait des 10 premières colonnes :")
    print(df_schema.head(10).to_string(index=False))

    # Sauvegarde
    DIR_METRICS.mkdir(parents=True, exist_ok=True)
    chemin_out = DIR_METRICS / "schema.csv"
    df_schema.to_csv(chemin_out, index=False, encoding="utf-8")
    print(f"[OK] Schéma sauvegardé → {chemin_out}")

    return df_schema


# =============================================================================
# 3. Valeurs manquantes
# =============================================================================
def analyser_valeurs_manquantes(df: DataFrame) -> pd.DataFrame:
    """
    Calcule le taux de valeurs manquantes par colonne (sur le dataset complet via Spark).
    Génère un bar chart horizontal sauvegardé dans reports/figures/.
    """
    nb_total = df.count()

    # Comptage des nulls pour chaque colonne en un seul passage Spark
    exprs_null = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
    df_nulls = df.agg(*exprs_null).toPandas().T
    df_nulls.columns = ["nb_manquants"]
    df_nulls.index.name = "colonne"
    df_nulls = df_nulls.reset_index()
    df_nulls["nb_manquants"] = df_nulls["nb_manquants"].astype(int)
    df_nulls["pct_manquants"] = (df_nulls["nb_manquants"] / nb_total * 100).round(2)
    df_nulls = df_nulls.sort_values("pct_manquants", ascending=False)

    print("\n[VALEURS MANQUANTES] Top 15 colonnes avec le plus de manquants :")
    print(df_nulls[df_nulls["pct_manquants"] > 0].head(15).to_string(index=False))

    # --- Graphique bar chart horizontal ---
    df_plot = df_nulls[df_nulls["pct_manquants"] > 0].head(20)
    if not df_plot.empty:
        fig, ax = plt.subplots(figsize=(10, max(5, len(df_plot) * 0.4)))
        colors = [
            "#d62728" if p > 20 else "#ff7f0e" if p > 5 else "#1f77b4"
            for p in df_plot["pct_manquants"]
        ]
        ax.barh(df_plot["colonne"], df_plot["pct_manquants"], color=colors)
        ax.set_xlabel("% de valeurs manquantes")
        ax.set_title("Taux de valeurs manquantes par colonne (Top 20)")
        ax.axvline(x=5,  color="#ff7f0e", linestyle="--", linewidth=1, label="Seuil 5%")
        ax.axvline(x=20, color="#d62728", linestyle="--", linewidth=1, label="Seuil 20%")
        ax.legend()
        ax.invert_yaxis()
        plt.tight_layout()
        DIR_FIGURES.mkdir(parents=True, exist_ok=True)
        chemin_fig = DIR_FIGURES / "valeurs_manquantes.png"
        plt.savefig(chemin_fig, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"[OK] Graphique sauvegardé → {chemin_fig}")

    # Sauvegarde CSV
    chemin_out = DIR_METRICS / "valeurs_manquantes.csv"
    df_nulls.to_csv(chemin_out, index=False, encoding="utf-8")
    print(f"[OK] Métriques sauvegardées → {chemin_out}")

    return df_nulls


# =============================================================================
# 4. Détection des doublons
# =============================================================================
def detecter_doublons(df: DataFrame) -> dict:
    """
    Détecte les doublons sur :
      - la colonne ID (clé primaire)
      - la combinaison (Start_Lat, Start_Lng, Start_Time, Description)
    Retourne un dict avec les comptages.
    """
    nb_total = df.count()

    # Doublons sur ID
    nb_uniques_id = df.select("ID").distinct().count()
    nb_doublons_id = nb_total - nb_uniques_id

    # Doublons sur clé composite
    cle_composite = ["Start_Lat", "Start_Lng", "Start_Time", "Description"]
    nb_uniques_cle = df.select(*cle_composite).distinct().count()
    nb_doublons_cle = nb_total - nb_uniques_cle

    resultats = {
        "nb_total": nb_total,
        "nb_doublons_id": nb_doublons_id,
        "pct_doublons_id": round(nb_doublons_id / nb_total * 100, 4),
        "nb_doublons_cle_composite": nb_doublons_cle,
        "pct_doublons_cle_composite": round(nb_doublons_cle / nb_total * 100, 4),
    }

    print("\n[DOUBLONS]")
    print(f"  Total enregistrements          : {nb_total:>10,}")
    print(f"  Doublons sur ID                : {nb_doublons_id:>10,} ({resultats['pct_doublons_id']:.4f}%)")
    print(f"  Doublons sur clé composite     : {nb_doublons_cle:>10,} ({resultats['pct_doublons_cle_composite']:.4f}%)")

    # Sauvegarde
    df_out = pd.DataFrame([resultats])
    chemin_out = DIR_METRICS / "doublons.csv"
    df_out.to_csv(chemin_out, index=False, encoding="utf-8")
    print(f"[OK] Résultats doublons → {chemin_out}")

    return resultats


# =============================================================================
# 5. Statistiques descriptives des variables numériques
# =============================================================================
def stats_descriptives_numeriques(df: DataFrame) -> pd.DataFrame:
    """
    Calcule min, max, moyenne, médiane (approx.), écart-type, quartiles
    pour les colonnes numériques cibles via Spark (dataset complet).
    """
    # Colonnes existantes dans le df
    cols_presentes = [c for c in COLS_NUMERIQUES if c in df.columns]

    # Calcul en un seul passage Spark (approximation médiane/quartiles via percentile_approx)
    exprs = []
    for col in cols_presentes:
        c = F.col(col)
        exprs += [
            F.min(c).alias(f"{col}_min"),
            F.max(c).alias(f"{col}_max"),
            F.mean(c).alias(f"{col}_mean"),
            F.stddev(c).alias(f"{col}_std"),
            F.percentile_approx(col, 0.25).alias(f"{col}_q1"),
            F.percentile_approx(col, 0.50).alias(f"{col}_median"),
            F.percentile_approx(col, 0.75).alias(f"{col}_q3"),
        ]

    row = df.agg(*exprs).collect()[0].asDict()

    # Restructuration en DataFrame lisible
    lignes = []
    for col in cols_presentes:
        q1  = row[f"{col}_q1"]
        q3  = row[f"{col}_q3"]
        iqr = (q3 - q1) if (q1 is not None and q3 is not None) else None
        lignes.append({
            "colonne":  col,
            "min":      row[f"{col}_min"],
            "max":      row[f"{col}_max"],
            "moyenne":  round(row[f"{col}_mean"], 4) if row[f"{col}_mean"] else None,
            "mediane":  row[f"{col}_median"],
            "ecart_type": round(row[f"{col}_std"], 4) if row[f"{col}_std"] else None,
            "q1":       q1,
            "q3":       q3,
            "iqr":      round(iqr, 4) if iqr else None,
        })

    df_stats = pd.DataFrame(lignes)
    print("\n[STATS NUMÉRIQUES]")
    print(df_stats.to_string(index=False))

    # Sauvegarde
    chemin_out = DIR_METRICS / "stats_numeriques.csv"
    df_stats.to_csv(chemin_out, index=False, encoding="utf-8")
    print(f"[OK] Stats numériques → {chemin_out}")

    return df_stats


# =============================================================================
# 6. Distribution des variables catégorielles
# =============================================================================
def distribution_categorielles(df: DataFrame) -> dict:
    """
    Calcule la cardinalité et le top 10 des valeurs pour chaque colonne catégorielle cible.
    Sauvegarde un CSV par colonne dans data/metrics/profiling/.
    """
    resultats = {}

    for col in COLS_CATEGORIELLES:
        if col not in df.columns:
            print(f"[WARN] Colonne '{col}' absente du dataset, ignorée.")
            continue

        # Cardinalité (nombre de valeurs distinctes)
        cardinalite = df.select(col).distinct().count()

        # Top 10 des valeurs les plus fréquentes
        df_top10 = (
            df.groupBy(col)
              .count()
              .orderBy(F.col("count").desc())
              .limit(10)
              .toPandas()
        )
        df_top10.columns = ["valeur", "count"]
        df_top10["pct"] = (df_top10["count"] / df.count() * 100).round(2)

        resultats[col] = {
            "cardinalite": cardinalite,
            "top10": df_top10
        }

        print(f"\n[CATÉGORIELLE] {col} — Cardinalité : {cardinalite:,}")
        print(df_top10.to_string(index=False))

        # Sauvegarde CSV
        chemin_out = DIR_METRICS / f"distribution_{col.lower().replace('(', '').replace(')', '')}.csv"
        df_top10.to_csv(chemin_out, index=False, encoding="utf-8")

    print(f"\n[OK] Distributions catégorielles sauvegardées dans {DIR_METRICS}")
    return resultats


# =============================================================================
# 7. Détection des outliers (IQR + z-score)
# =============================================================================
def detecter_outliers(df: DataFrame, df_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte les valeurs aberrantes via la méthode IQR (sur Spark, dataset complet)
    et la méthode z-score (sur un échantillon Pandas 5%).

    Paramètres IQR : facteur 1.5 (configurable dans thresholds.yaml).
    Paramètres z-score : seuil 3.0 (configurable).
    """
    config = charger_config()
    facteur_iqr = config["alertes"]["outliers"]["methode_iqr_facteur"]
    seuil_zscore = config["alertes"]["outliers"]["methode_zscore_seuil"]

    nb_total = df.count()
    lignes_outliers = []

    # --- Méthode IQR (Spark — dataset complet) ---
    for _, row in df_stats.iterrows():
        col      = row["colonne"]
        q1, q3   = row["q1"], row["q3"]
        if col not in df.columns or q1 is None or q3 is None:
            continue

        iqr            = q3 - q1
        borne_basse    = q1 - facteur_iqr * iqr
        borne_haute    = q3 + facteur_iqr * iqr

        nb_outliers_iqr = df.filter(
            (F.col(col) < borne_basse) | (F.col(col) > borne_haute)
        ).count()

        lignes_outliers.append({
            "colonne":           col,
            "methode":           "IQR",
            "borne_basse":       round(borne_basse, 4),
            "borne_haute":       round(borne_haute, 4),
            "nb_outliers":       nb_outliers_iqr,
            "pct_outliers":      round(nb_outliers_iqr / nb_total * 100, 4),
        })
        print(f"[IQR] {col:25s} → {nb_outliers_iqr:>8,} outliers ({nb_outliers_iqr/nb_total*100:.2f}%)")

    # --- Méthode z-score (Pandas — échantillon 5%) ---
    print(f"\n[Z-SCORE] Calcul sur échantillon {TAUX_ECHANTILLON*100:.0f}%...")
    df_sample = df.sample(fraction=TAUX_ECHANTILLON, seed=42).toPandas()

    cols_presentes = [c for c in COLS_NUMERIQUES if c in df_sample.columns]
    for col in cols_presentes:
        serie = df_sample[col].dropna()
        if len(serie) == 0:
            continue
        z_scores    = np.abs(stats.zscore(serie))
        nb_out_z    = int((z_scores > seuil_zscore).sum())
        pct_out_z   = nb_out_z / len(serie) * 100

        lignes_outliers.append({
            "colonne":       col,
            "methode":       f"Z-score (seuil {seuil_zscore})",
            "borne_basse":   None,
            "borne_haute":   None,
            "nb_outliers":   nb_out_z,
            "pct_outliers":  round(pct_out_z, 4),
        })
        print(f"[Z-SCORE] {col:25s} → {nb_out_z:>6,} outliers ({pct_out_z:.2f}%) dans l'échantillon")

    df_outliers = pd.DataFrame(lignes_outliers)

    # Sauvegarde
    chemin_out = DIR_METRICS / "outliers.csv"
    df_outliers.to_csv(chemin_out, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats outliers → {chemin_out}")

    return df_outliers


# =============================================================================
# 8. Graphiques sur échantillon (histogrammes + boxplots)
# =============================================================================
def generer_graphiques(df: DataFrame) -> None:
    """
    Génère des histogrammes et boxplots pour les colonnes numériques cibles.
    Travaille sur un échantillon de 5% pour les performances.
    Sauvegarde les figures dans reports/figures/.
    """
    print(f"\n[GRAPHIQUES] Génération sur échantillon {TAUX_ECHANTILLON*100:.0f}%...")
    df_sample = df.sample(fraction=TAUX_ECHANTILLON, seed=42).toPandas()
    cols_presentes = [c for c in COLS_NUMERIQUES if c in df_sample.columns]

    DIR_FIGURES.mkdir(parents=True, exist_ok=True)

    # Palette
    palette = sns.color_palette("husl", len(cols_presentes))

    # --- Histogrammes ---
    fig, axes = plt.subplots(
        nrows=(len(cols_presentes) + 2) // 3,
        ncols=3,
        figsize=(16, 4 * ((len(cols_presentes) + 2) // 3))
    )
    axes = axes.flatten()

    for i, col in enumerate(cols_presentes):
        serie = df_sample[col].dropna()
        axes[i].hist(serie, bins=50, color=palette[i], edgecolor="white", alpha=0.85)
        axes[i].set_title(col, fontsize=10)
        axes[i].set_xlabel("Valeur")
        axes[i].set_ylabel("Fréquence")

    # Masquer les axes vides
    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(f"Histogrammes des variables numériques (échantillon {TAUX_ECHANTILLON*100:.0f}%)", fontsize=13)
    plt.tight_layout()
    chemin_hist = DIR_FIGURES / "histogrammes_numeriques.png"
    plt.savefig(chemin_hist, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] Histogrammes → {chemin_hist}")

    # --- Boxplots ---
    fig, axes = plt.subplots(
        nrows=(len(cols_presentes) + 2) // 3,
        ncols=3,
        figsize=(16, 4 * ((len(cols_presentes) + 2) // 3))
    )
    axes = axes.flatten()

    for i, col in enumerate(cols_presentes):
        serie = df_sample[col].dropna()
        axes[i].boxplot(serie, vert=True, patch_artist=True,
                        boxprops=dict(facecolor=palette[i], alpha=0.7))
        axes[i].set_title(col, fontsize=10)
        axes[i].set_ylabel("Valeur")

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(f"Boxplots des variables numériques (échantillon {TAUX_ECHANTILLON*100:.0f}%)", fontsize=13)
    plt.tight_layout()
    chemin_box = DIR_FIGURES / "boxplots_numeriques.png"
    plt.savefig(chemin_box, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] Boxplots → {chemin_box}")


# =============================================================================
# Point d'entrée principal
# =============================================================================
def run_profiling_complet() -> dict:
    """
    Lance l'ensemble du profilage et retourne un dict de synthèse.
    Peut être appelé depuis le notebook ou en ligne de commande.
    """
    print("\n" + "="*60)
    print("  DATA PROFILING — US Accidents 2016-2023")
    print("="*60)

    spark = creer_session_spark()

    t_global = time.time()

    # Chargement
    df = charger_parquet(spark)

    # 1. Vue d'ensemble
    res_vue    = vue_ensemble(df)

    # 2. Schéma
    df_schema  = schema_et_types(df)

    # 3. Valeurs manquantes
    df_missing = analyser_valeurs_manquantes(df)

    # 4. Doublons
    res_doublons = detecter_doublons(df)

    # 5. Stats numériques
    df_stats   = stats_descriptives_numeriques(df)

    # 6. Catégorielles
    res_cat    = distribution_categorielles(df)

    # 7. Outliers
    df_outliers = detecter_outliers(df, df_stats)

    # 8. Graphiques (sur échantillon)
    generer_graphiques(df)

    duree_totale = time.time() - t_global
    print(f"\n[TERMINÉ] Profilage complet en {duree_totale:.1f}s ({duree_totale/60:.1f} min)")

    spark.stop()

    return {
        "vue_ensemble": res_vue,
        "schema": df_schema,
        "missing": df_missing,
        "doublons": res_doublons,
        "stats_numeriques": df_stats,
        "categories": res_cat,
        "outliers": df_outliers,
    }


if __name__ == "__main__":
    run_profiling_complet()
