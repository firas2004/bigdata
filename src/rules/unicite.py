"""
src/rules/unicite.py
=====================
Règles d'unicité : vérifie l'absence de doublons sur la clé primaire
et sur la clé composite métier.

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


def regle_cle_primaire(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R3a — La colonne ID doit être unique (clé primaire).
    """
    nb_total   = df.count()
    nb_uniques = df.select("ID").distinct().count()
    nb_doublons = nb_total - nb_uniques

    df_res = pd.DataFrame([{
        "regle":      "R3a_unicite_id",
        "colonne":    "ID",
        "nb_total":   nb_total,
        "nb_uniques": nb_uniques,
        "nb_doublons": nb_doublons,
        "pct_ok":     round(nb_uniques / nb_total * 100, 4),
        "statut":     "OK" if nb_doublons == 0 else "KO",
    }])

    print("\n[R3a — Unicité ID]")
    print(df_res[["colonne", "nb_doublons", "pct_ok", "statut"]].to_string(index=False))
    return df_res


def regle_cle_composite(df: DataFrame, rules: dict) -> pd.DataFrame:
    """
    R3b — La combinaison (Start_Lat, Start_Lng, Start_Time, Description)
    doit être unique. Identifie et échantillonne les doublons.
    """
    nb_total = df.count()
    cle      = ["Start_Lat", "Start_Lng", "Start_Time", "Description"]
    cle_pres = [c for c in cle if c in df.columns]

    nb_uniques  = df.select(*cle_pres).distinct().count()
    nb_doublons = nb_total - nb_uniques
    pct_doublons = round(nb_doublons / nb_total * 100, 4)

    # Échantillon des enregistrements dupliqués (top 10 groupes)
    df_dupes = (
        df.groupBy(*cle_pres)
          .count()
          .filter(F.col("count") > 1)
          .orderBy(F.col("count").desc())
          .limit(10)
          .toPandas()
    )

    df_res = pd.DataFrame([{
        "regle":        "R3b_unicite_cle_composite",
        "colonnes":     "+".join(cle_pres),
        "nb_total":     nb_total,
        "nb_uniques":   nb_uniques,
        "nb_doublons":  nb_doublons,
        "pct_doublons": pct_doublons,
        "pct_ok":       round(100 - pct_doublons, 4),
        "statut":       "OK" if pct_doublons == 0 else "WARNING",
    }])

    print("\n[R3b — Unicité clé composite]")
    print(df_res[["colonnes", "nb_doublons", "pct_doublons", "statut"]].to_string(index=False))
    print("Top 10 groupes dupliqués :")
    print(df_dupes.to_string(index=False))

    # Sauvegarde des doublons
    chemin_dupes = DIR_METRICS / "doublons_composite_top10.csv"
    df_dupes.to_csv(chemin_dupes, index=False, encoding="utf-8")

    return df_res


def run(df: DataFrame) -> pd.DataFrame:
    """Point d'entrée : exécute toutes les règles d'unicité."""
    rules = charger_config()
    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    df_r3a = regle_cle_primaire(df, rules)
    df_r3b = regle_cle_composite(df, rules)

    df_all = pd.concat([df_r3a, df_r3b], ignore_index=True)
    chemin = DIR_METRICS / "unicite.csv"
    df_all.to_csv(chemin, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats unicité → {chemin}")
    return df_all
