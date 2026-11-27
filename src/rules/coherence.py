"""
src/rules/coherence.py
=======================
Règles de cohérence : vérifie la logique entre plusieurs colonnes
(chronologie, géographie, durée des accidents) — R8 et R9.

Auteur : Big Data Observability Project
"""

from pathlib import Path
import yaml
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RACINE       = Path(__file__).resolve().parents[2]
DIR_METRICS  = RACINE / "data" / "metrics" / "rules"
CHEMIN_RULES = RACINE / "config" / "rules.yaml"
CHEMIN_THRESH = RACINE / "config" / "thresholds.yaml"


def charger_config():
    with open(CHEMIN_RULES,  "r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)
    with open(CHEMIN_THRESH, "r", encoding="utf-8") as f:
        thresh = yaml.safe_load(f)
    return rules, thresh


def regle_chronologie(df: DataFrame, rules: dict, thresh: dict) -> pd.DataFrame:
    """
    R8 — End_Time doit être >= Start_Time.
    Contrôle complémentaire : durée de l'accident ≤ 30 jours (configurable).
    """
    nb_total     = df.count()
    duree_max_j  = rules["coherence"]["duree_max_jours"]
    duree_max_s  = duree_max_j * 86400  # en secondes

    # Lignes où End_Time < Start_Time (incohérence temporelle)
    nb_inversion = df.filter(
        F.col("Start_Time").isNotNull() &
        F.col("End_Time").isNotNull() &
        (F.col("End_Time") < F.col("Start_Time"))
    ).count()

    # Durée > 30 jours
    nb_duree_excessive = df.filter(
        F.col("Start_Time").isNotNull() &
        F.col("End_Time").isNotNull() &
        (F.col("End_Time") >= F.col("Start_Time")) &
        ((F.unix_timestamp("End_Time") - F.unix_timestamp("Start_Time")) > duree_max_s)
    ).count()

    pct_ok_inv = round((1 - nb_inversion / nb_total) * 100, 4)
    pct_ok_dur = round((1 - nb_duree_excessive / nb_total) * 100, 4)

    lignes = [
        {
            "regle":   "R8a_coherence_end_apres_start",
            "colonne": "End_Time vs Start_Time",
            "nb_total": nb_total,
            "nb_ko":   nb_inversion,
            "pct_ok":  pct_ok_inv,
            "statut":  "OK" if nb_inversion == 0 else "KO",
        },
        {
            "regle":   f"R8b_coherence_duree_max_{duree_max_j}j",
            "colonne": "Durée accident",
            "nb_total": nb_total,
            "nb_ko":   nb_duree_excessive,
            "pct_ok":  pct_ok_dur,
            "statut":  "OK" if nb_duree_excessive == 0 else "WARNING",
        },
    ]

    df_res = pd.DataFrame(lignes)
    print("\n[R8 — Cohérence chronologique]")
    print(df_res[["regle", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def regle_bounding_box_geo(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R9 — Les coordonnées Start_Lat/Start_Lng doivent être dans la bounding box US.
    Valeurs hors zone = erreur de géocodage ou accident non-US.
    """
    nb_total = df.count()
    geo_cfg  = rules["coherence"]["geo_us"]

    lat_min, lat_max = geo_cfg["lat_min"], geo_cfg["lat_max"]
    lng_min, lng_max = geo_cfg["lng_min"], geo_cfg["lng_max"]

    nb_hors_bbox = df.filter(
        F.col("Start_Lat").isNotNull() &
        F.col("Start_Lng").isNotNull() &
        (
            (F.col("Start_Lat") < lat_min) | (F.col("Start_Lat") > lat_max) |
            (F.col("Start_Lng") < lng_min) | (F.col("Start_Lng") > lng_max)
        )
    ).count()

    pct_ok = round((1 - nb_hors_bbox / nb_total) * 100, 4)

    df_res = pd.DataFrame([{
        "regle":      "R9_coherence_geo_us",
        "colonne":    "Start_Lat/Start_Lng",
        "bbox":       f"[{lat_min},{lat_max}°N] [{lng_min},{lng_max}°W]",
        "nb_total":   nb_total,
        "nb_ko":      nb_hors_bbox,
        "pct_ok":     pct_ok,
        "statut":     "OK" if nb_hors_bbox == 0 else "KO",
    }])

    print("\n[R9 — Cohérence géographique US]")
    print(df_res[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def regle_distance_coherente(df: DataFrame) -> pd.DataFrame:
    """
    Cohérence additionnelle : Distance(mi) doit être >= 0.
    Une distance négative est physiquement impossible.
    """
    nb_total   = df.count()
    nb_neg_dist = df.filter(
        F.col("Distance(mi)").isNotNull() &
        (F.col("Distance(mi)") < 0)
    ).count()

    pct_ok = round((1 - nb_neg_dist / nb_total) * 100, 4)

    df_res = pd.DataFrame([{
        "regle":   "R8c_coherence_distance_positive",
        "colonne": "Distance(mi)",
        "nb_total": nb_total,
        "nb_ko":   nb_neg_dist,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_neg_dist == 0 else "KO",
    }])

    print("\n[R8c — Distance >= 0]")
    print(df_res[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def run(df: DataFrame) -> pd.DataFrame:
    """Point d'entrée : exécute toutes les règles de cohérence."""
    rules, thresh = charger_config()
    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    df_r8  = regle_chronologie(df, rules, thresh)
    df_r9  = regle_bounding_box_geo(df, rules)
    df_r8c = regle_distance_coherente(df)

    df_all = pd.concat([df_r8, df_r9, df_r8c], ignore_index=True)
    chemin = DIR_METRICS / "coherence.csv"
    df_all.to_csv(chemin, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats cohérence → {chemin}")
    return df_all
