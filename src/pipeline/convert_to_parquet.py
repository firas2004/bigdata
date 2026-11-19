"""
src/pipeline/convert_to_parquet.py
====================================
Conversion du CSV US Accidents vers le format Parquet partitionné par année/mois.

Usage :
    python src/pipeline/convert_to_parquet.py

Auteur : Big Data Observability Project
"""

import os
import time
import glob
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, DoubleType
)


# =============================================================================
# Chemins (relatifs à la racine du projet)
# =============================================================================
RACINE_PROJET = Path(__file__).resolve().parents[2]
CHEMIN_CSV    = RACINE_PROJET / "data" / "raw" / "US_Accidents_March23.csv"
CHEMIN_PARQUET = RACINE_PROJET / "data" / "parquet" / "us_accidents"
CHEMIN_CONFIG  = RACINE_PROJET / "config" / "thresholds.yaml"


def charger_config_spark() -> dict:
    """Charge les paramètres Spark depuis thresholds.yaml."""
    with open(CHEMIN_CONFIG, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("performance", {}).get("spark", {})


def creer_session_spark(config_spark: dict) -> SparkSession:
    """
    Crée et retourne une session Spark configurée pour une machine locale
    avec les ressources définies dans thresholds.yaml.
    """
    driver_memory      = config_spark.get("driver_memory", "4g")
    shuffle_partitions = config_spark.get("shuffle_partitions", 8)

    spark = (
        SparkSession.builder
        .appName("US_Accidents_CSV_to_Parquet")
        .master("local[*]")  # Utilise tous les cœurs disponibles
        # Mémoire driver (adapté à ~16 Go RAM locale)
        .config("spark.driver.memory", driver_memory)
        # Évite le swap en cas de pression mémoire
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        # Partitions shuffle raisonnables pour local (par défaut : 200, trop élevé)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Accélère l'inférence CSV
        .config("spark.sql.csv.filterPushdown.enabled", "true")
        # Logs moins verbeux
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def definir_schema() -> StructType:
    """
    Définit le schéma explicite du CSV US Accidents pour éviter une inférence lente.
    Les 46 colonnes sont typées manuellement d'après la documentation du dataset.
    """
    return StructType([
        StructField("ID",                   StringType(),  True),
        StructField("Severity",             IntegerType(), True),
        StructField("Start_Time",           StringType(),  True),  # → converti en timestamp après
        StructField("End_Time",             StringType(),  True),  # → converti en timestamp après
        StructField("Start_Lat",            DoubleType(),  True),
        StructField("Start_Lng",            DoubleType(),  True),
        StructField("End_Lat",              DoubleType(),  True),
        StructField("End_Lng",              DoubleType(),  True),
        StructField("Distance(mi)",         FloatType(),   True),
        StructField("Description",          StringType(),  True),
        StructField("Street",               StringType(),  True),
        StructField("City",                 StringType(),  True),
        StructField("County",               StringType(),  True),
        StructField("State",                StringType(),  True),
        StructField("Zipcode",              StringType(),  True),
        StructField("Country",              StringType(),  True),
        StructField("Timezone",             StringType(),  True),
        StructField("Airport_Code",         StringType(),  True),
        StructField("Weather_Timestamp",    StringType(),  True),
        StructField("Temperature(F)",       FloatType(),   True),
        StructField("Wind_Chill(F)",        FloatType(),   True),
        StructField("Humidity(%)",          FloatType(),   True),
        StructField("Pressure(in)",         FloatType(),   True),
        StructField("Visibility(mi)",       FloatType(),   True),
        StructField("Wind_Direction",       StringType(),  True),
        StructField("Wind_Speed(mph)",      FloatType(),   True),
        StructField("Precipitation(in)",    FloatType(),   True),
        StructField("Weather_Condition",    StringType(),  True),
        StructField("Amenity",              BooleanType(), True),
        StructField("Bump",                 BooleanType(), True),
        StructField("Crossing",             BooleanType(), True),
        StructField("Give_Way",             BooleanType(), True),
        StructField("Junction",             BooleanType(), True),
        StructField("No_Exit",              BooleanType(), True),
        StructField("Railway",              BooleanType(), True),
        StructField("Roundabout",           BooleanType(), True),
        StructField("Station",              BooleanType(), True),
        StructField("Stop",                 BooleanType(), True),
        StructField("Traffic_Calming",      BooleanType(), True),
        StructField("Traffic_Signal",       BooleanType(), True),
        StructField("Turning_Loop",         BooleanType(), True),
        StructField("Sunrise_Sunset",       StringType(),  True),
        StructField("Civil_Twilight",       StringType(),  True),
        StructField("Nautical_Twilight",    StringType(),  True),
        StructField("Astronomical_Twilight",StringType(),  True),
    ])


def convertir_timestamps(df):
    """
    Convertit les colonnes Start_Time et End_Time (chaînes) en timestamps Spark.
    Gère les deux formats rencontrés dans le dataset :
      - "2016-02-08 05:46:00"       (sans fractions de secondes)
      - "2016-02-08 05:46:00.000"   (avec fractions de secondes)
    """
    for col_name in ["Start_Time", "End_Time"]:
        df = df.withColumn(
            col_name,
            F.coalesce(
                # Format avec fractions de secondes
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss.SSS"),
                # Format sans fractions
                F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
            )
        )
    return df


def ajouter_colonnes_partitionnement(df):
    """
    Ajoute les colonnes 'year' et 'month' extraites de Start_Time.
    Ces colonnes servent de clés de partitionnement Parquet.
    """
    df = df.withColumn("year",  F.year(F.col("Start_Time")))
    df = df.withColumn("month", F.month(F.col("Start_Time")))
    return df


def taille_repertoire_mo(chemin: Path) -> float:
    """Calcule la taille totale d'un répertoire en Mo."""
    total = sum(
        f.stat().st_size
        for f in chemin.rglob("*")
        if f.is_file()
    )
    return total / (1024 * 1024)


def main():
    print("=" * 60)
    print("  Conversion CSV → Parquet  |  US Accidents 2016-2023")
    print("=" * 60)

    # Vérification du fichier source
    if not CHEMIN_CSV.exists():
        print(f"\n[ERREUR] Fichier CSV introuvable : {CHEMIN_CSV}")
        print("→ Téléchargez le dataset avec :")
        print("  kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw --unzip")
        return

    # Taille du fichier source
    taille_csv_mo = CHEMIN_CSV.stat().st_size / (1024 * 1024)
    print(f"\n[INFO] Fichier source : {CHEMIN_CSV.name}")
    print(f"[INFO] Taille CSV     : {taille_csv_mo:.1f} Mo ({taille_csv_mo/1024:.2f} Go)")

    # Chargement de la configuration Spark
    config_spark = charger_config_spark()
    print(f"\n[CONFIG] driver_memory      : {config_spark.get('driver_memory', '4g')}")
    print(f"[CONFIG] shuffle_partitions : {config_spark.get('shuffle_partitions', 8)}")

    # Création de la session Spark
    print("\n[SPARK] Démarrage de la session Spark...")
    spark = creer_session_spark(config_spark)
    print(f"[SPARK] Version : {spark.version}")

    # Lecture du CSV avec schéma explicite
    print("\n[LECTURE] Chargement du CSV en cours...")
    t0 = time.time()

    schema = definir_schema()
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "false")       # Performances meilleures sans multiLine
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(str(CHEMIN_CSV))
    )

    t_lecture = time.time() - t0
    print(f"[LECTURE] Temps de lecture schéma : {t_lecture:.1f}s")

    # Transformations
    print("\n[TRANSFORM] Conversion des timestamps...")
    df = convertir_timestamps(df)

    print("[TRANSFORM] Ajout des colonnes year/month...")
    df = ajouter_colonnes_partitionnement(df)

    # Affichage du schéma final
    print("\n[SCHEMA] Schéma final du DataFrame :")
    df.printSchema()

    # Écriture en Parquet partitionné
    print(f"\n[ECRITURE] Destination : {CHEMIN_PARQUET}")
    print("[ECRITURE] Partitionnement : year / month")
    print("[ECRITURE] Format de compression : snappy (par défaut)")

    CHEMIN_PARQUET.mkdir(parents=True, exist_ok=True)
    t1 = time.time()

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(CHEMIN_PARQUET))
    )

    t_ecriture = time.time() - t1
    t_total    = time.time() - t0

    # Taille du répertoire Parquet
    taille_parquet_mo = taille_repertoire_mo(CHEMIN_PARQUET)

    print("\n" + "=" * 60)
    print("  Résultats de la conversion")
    print("=" * 60)
    print(f"  CSV source          : {taille_csv_mo:>8.1f} Mo")
    print(f"  Parquet partitionné : {taille_parquet_mo:>8.1f} Mo")
    taux_compression = (1 - taille_parquet_mo / taille_csv_mo) * 100
    print(f"  Taux de compression : {taux_compression:>8.1f} %")
    print(f"  Temps lecture CSV   : {t_lecture:>8.1f} s")
    print(f"  Temps écriture Parquet: {t_ecriture:>6.1f} s")
    print(f"  Temps total         : {t_total:>8.1f} s")
    print("=" * 60)

    # Vérification rapide : nombre de lignes
    print("\n[VERIF] Comptage des lignes (lecture Parquet)...")
    df_verif = spark.read.parquet(str(CHEMIN_PARQUET))
    nb_lignes = df_verif.count()
    nb_cols   = len(df_verif.columns)
    print(f"[VERIF] Lignes lues depuis Parquet : {nb_lignes:,}")
    print(f"[VERIF] Colonnes                   : {nb_cols}")

    spark.stop()
    print("\n[OK] Session Spark arrêtée. Conversion terminée avec succès.")


if __name__ == "__main__":
    main()
