"""
src/rules/completude.py
========================
Règles de complétude : vérifie que les colonnes obligatoires sont renseignées
et que les colonnes importantes respectent un seuil de complétude minimum.

Auteur : Big Data Observability Project
"""

import os
from pathlib import Path
import yaml
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RACINE        = Path(__file__).resolve().parents[2]
DIR_METRICS   = RACINE / "data" / "metrics" / "rules"
CHEMIN_RULES  = RACINE / "config" / "rules.yaml"
CHEMIN_THRESH = RACINE / "config" / "thresholds.yaml"


def charger_config():
    with open(CHEMIN_RULES,  "r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)
    with open(CHEMIN_THRESH, "r", encoding="utf-8") as f:
        thresh = yaml.safe_load(f)
    return rules, thresh


def regle_colonnes_obligatoires(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R1 — Les colonnes obligatoires ne doivent avoir aucune valeur manquante.
    Retourne un DataFrame avec le nb et % de nulls par colonne obligatoire.
    """
    cols_obligatoires = rules["completude"]["colonnes_obligatoires"]
    cols_presentes    = [c for c in cols_obligatoires if c in df.columns]
    nb_total          = df.count()

    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols_presentes]
    nulls = df.agg(*exprs).collect()[0].asDict()

    lignes = []
    for col in cols_presentes:
        nb_null = nulls[col]
        pct_ok  = round((1 - nb_null / nb_total) * 100, 4)
        lignes.append({
            "regle":       "R1_completude_obligatoire",
            "colonne":     col,
            "nb_total":    nb_total,
            "nb_ko":       nb_null,
            "pct_ok":      pct_ok,
            "statut":      "OK" if nb_null == 0 else "KO",
        })

    df_res = pd.DataFrame(lignes)
    print("\n[R1 — Colonnes obligatoires]")
    print(df_res[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def regle_seuils_completude(df: DataFrame, rules: dict, thresh: dict) -> pd.DataFrame:
    """
    R2 — Les colonnes importantes doivent dépasser leur seuil de complétude minimum.
    Seuils définis dans rules.yaml → completude.seuil_completude.
    """
    seuils    = rules["completude"]["seuil_completude"]
    nb_total  = df.count()

    # Renommage des clés (les noms YAML ont été simplifiés)
    MAP_NOMS = {
        "Temperature_F":   "Temperature(F)",
        "Visibility_mi":   "Visibility(mi)",
        "Wind_Speed_mph":  "Wind_Speed(mph)",
        "Humidity_pct":    "Humidity(%)",
        "Pressure_in":     "Pressure(in)",
        "Weather_Condition": "Weather_Condition",
        "City":            "City",
        "Zipcode":         "Zipcode",
    }

    cols_a_tester = {MAP_NOMS.get(k, k): v for k, v in seuils.items()}
    cols_presentes = {k: v for k, v in cols_a_tester.items() if k in df.columns}

    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols_presentes]
    nulls = df.agg(*exprs).collect()[0].asDict()

    lignes = []
    for col, seuil_min in cols_presentes.items():
        nb_null = nulls[col]
        pct_ok  = round((1 - nb_null / nb_total) * 100, 4)
        lignes.append({
            "regle":       "R2_completude_seuil",
            "colonne":     col,
            "seuil_min_pct": seuil_min,
            "nb_total":    nb_total,
            "nb_ko":       nb_null,
            "pct_ok":      pct_ok,
            "statut":      "OK" if pct_ok >= seuil_min else "KO",
        })

    df_res = pd.DataFrame(lignes)
    print("\n[R2 — Seuils de complétude]")
    print(df_res[["colonne", "seuil_min_pct", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def run(df: DataFrame) -> pd.DataFrame:
    """Point d'entrée : exécute toutes les règles de complétude."""
    rules, thresh = charger_config()
    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    df_r1 = regle_colonnes_obligatoires(df, rules)
    df_r2 = regle_seuils_completude(df, rules, thresh)

    df_all = pd.concat([df_r1, df_r2], ignore_index=True)
    chemin = DIR_METRICS / "completude.csv"
    df_all.to_csv(chemin, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats complétude → {chemin}")
    return df_all
