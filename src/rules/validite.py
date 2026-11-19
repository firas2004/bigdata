"""
src/rules/validite.py
======================
Règles de validité : vérifie que les valeurs sont dans les plages acceptables
et respectent les formats attendus (R4-R7).

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


def charger_config():
    with open(CHEMIN_RULES, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def regle_plages_numeriques(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R4-R6 — Chaque colonne numérique doit être dans sa plage [min, max] définie.
    Compte les valeurs hors-bornes sur le dataset complet via Spark.
    """
    plages    = rules["validite"]["plages_numeriques"]
    nb_total  = df.count()

    # Mapping noms YAML → noms colonnes réels
    MAP_NOMS = {
        "Severity":        "Severity",
        "Temperature_F":   "Temperature(F)",
        "Visibility_mi":   "Visibility(mi)",
        "Wind_Speed_mph":  "Wind_Speed(mph)",
        "Humidity_pct":    "Humidity(%)",
        "Pressure_in":     "Pressure(in)",
        "Distance_mi":     "Distance(mi)",
        "Precipitation_in":"Precipitation(in)",
        "Start_Lat":       "Start_Lat",
        "Start_Lng":       "Start_Lng",
    }

    lignes = []
    for col_yaml, bornes in plages.items():
        col_reel = MAP_NOMS.get(col_yaml, col_yaml)
        if col_reel not in df.columns:
            continue

        borne_min = bornes["min"]
        borne_max = bornes["max"]

        # Nombre de valeurs hors-bornes (non nulles)
        nb_hors_bornes = df.filter(
            F.col(col_reel).isNotNull() &
            ((F.col(col_reel) < borne_min) | (F.col(col_reel) > borne_max))
        ).count()

        pct_ok = round((1 - nb_hors_bornes / nb_total) * 100, 4)

        lignes.append({
            "regle":          f"R4_validite_plage",
            "colonne":        col_reel,
            "borne_min":      borne_min,
            "borne_max":      borne_max,
            "nb_total":       nb_total,
            "nb_hors_bornes": nb_hors_bornes,
            "pct_ok":         pct_ok,
            "statut":         "OK" if nb_hors_bornes == 0 else "KO",
        })
        print(f"[R4] {col_reel:25s} hors [{borne_min}, {borne_max}] → "
              f"{nb_hors_bornes:>8,} ({100-pct_ok:.4f}%)")

    df_res = pd.DataFrame(lignes)
    return df_res


def regle_severity(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R7 — Severity doit appartenir à {1, 2, 3, 4}.
    """
    valeurs_ok = rules["conformite"]["severity_valeurs"]
    nb_total   = df.count()

    nb_ko = df.filter(~F.col("Severity").isin(valeurs_ok)).count()
    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)

    df_res = pd.DataFrame([{
        "regle":          "R7_validite_severity",
        "colonne":        "Severity",
        "valeurs_valides": str(valeurs_ok),
        "nb_total":       nb_total,
        "nb_ko":          nb_ko,
        "pct_ok":         pct_ok,
        "statut":         "OK" if nb_ko == 0 else "KO",
    }])

    print(f"\n[R7 — Severity ∈ {valeurs_ok}]")
    print(df_res[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def regle_format_zipcode(df: DataFrame) -> pd.DataFrame:
    """
    Validité format — Zipcode doit correspondre au pattern US : 5 chiffres
    ou 5+4 chiffres (ex: "12345" ou "12345-6789").
    """
    nb_total = df.count()
    pattern  = r"^\d{5}(-\d{4})?$"

    # Exclut les nulls du comptage des KO
    nb_ko = df.filter(
        F.col("Zipcode").isNotNull() &
        ~F.col("Zipcode").rlike(pattern)
    ).count()

    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)

    df_res = pd.DataFrame([{
        "regle":   "R4_validite_format_zipcode",
        "colonne": "Zipcode",
        "pattern": pattern,
        "nb_total": nb_total,
        "nb_ko":   nb_ko,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_ko == 0 else "WARNING",
    }])

    print(f"\n[R4 — Format Zipcode]")
    print(df_res[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def run(df: DataFrame) -> pd.DataFrame:
    """Point d'entrée : exécute toutes les règles de validité."""
    rules = charger_config()
    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    print("\n[VALIDITÉ] Vérification des plages numériques...")
    df_plages = regle_plages_numeriques(df, rules)
    df_sev    = regle_severity(df, rules)
    df_zip    = regle_format_zipcode(df)

    df_all = pd.concat([df_plages, df_sev, df_zip], ignore_index=True)
    chemin = DIR_METRICS / "validite.csv"
    df_all.to_csv(chemin, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats validité → {chemin}")
    return df_all
