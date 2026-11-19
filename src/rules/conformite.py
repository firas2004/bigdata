"""
src/rules/conformite.py
========================
Règles de conformité : vérifie que les valeurs respectent les référentiels
et les listes de valeurs autorisées (R10).

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


def regle_country(df: DataFrame, rules: dict) -> pd.DataFrame:
    """R10a — Country doit être uniquement 'US'."""
    nb_total = df.count()
    nb_ko = df.filter(
        F.col("Country").isNotNull() & (F.col("Country") != "US")
    ).count()
    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)

    # Distribution des valeurs non-US
    df_non_us = (
        df.filter(F.col("Country") != "US")
          .groupBy("Country").count()
          .orderBy(F.col("count").desc())
          .limit(10)
          .toPandas()
    )
    if not df_non_us.empty:
        print("  Valeurs non-US :")
        print(df_non_us.to_string(index=False))

    return pd.DataFrame([{
        "regle":   "R10a_conformite_country",
        "colonne": "Country",
        "valeur_attendue": "US",
        "nb_total": nb_total,
        "nb_ko":   nb_ko,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_ko == 0 else "KO",
    }])


def regle_sunrise_sunset(df: DataFrame, rules: dict) -> pd.DataFrame:
    """R10b — Sunrise_Sunset doit être dans {'Day', 'Night'}."""
    valeurs_ok = rules["conformite"]["sunrise_sunset_valeurs"]
    nb_total   = df.count()
    nb_ko = df.filter(
        F.col("Sunrise_Sunset").isNotNull() &
        ~F.col("Sunrise_Sunset").isin(valeurs_ok)
    ).count()
    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)

    return pd.DataFrame([{
        "regle":   "R10b_conformite_sunrise_sunset",
        "colonne": "Sunrise_Sunset",
        "valeurs_valides": str(valeurs_ok),
        "nb_total": nb_total,
        "nb_ko":   nb_ko,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_ko == 0 else "WARNING",
    }])


def regle_side(df: DataFrame, rules: dict) -> pd.DataFrame:
    """R10c — Side (côté de la route) doit être dans {'L', 'R'}."""
    if "Side" not in df.columns:
        return pd.DataFrame()

    valeurs_ok = rules["conformite"]["side_valeurs"]
    nb_total   = df.count()
    nb_ko = df.filter(
        F.col("Side").isNotNull() &
        ~F.col("Side").isin(valeurs_ok)
    ).count()
    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)

    return pd.DataFrame([{
        "regle":   "R10c_conformite_side",
        "colonne": "Side",
        "valeurs_valides": str(valeurs_ok),
        "nb_total": nb_total,
        "nb_ko":   nb_ko,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_ko == 0 else "WARNING",
    }])


def regle_timezone(df: DataFrame) -> pd.DataFrame:
    """
    R10d — Timezone doit être un fuseau US valide.
    Référentiel : valeurs distinctes observées dans le dataset.
    """
    timezones_us = [
        "US/Eastern", "US/Central", "US/Mountain", "US/Pacific",
        "US/Alaska", "US/Hawaii", "America/New_York", "America/Chicago",
        "America/Denver", "America/Los_Angeles", "America/Anchorage",
        "America/Honolulu", "America/Phoenix"
    ]
    nb_total = df.count()
    nb_ko = df.filter(
        F.col("Timezone").isNotNull() &
        ~F.col("Timezone").isin(timezones_us)
    ).count()

    # Distribution réelle des timezones
    df_tz = (
        df.groupBy("Timezone").count()
          .orderBy(F.col("count").desc())
          .toPandas()
    )
    print("\n  Distribution Timezone (toutes valeurs) :")
    print(df_tz.to_string(index=False))

    pct_ok = round((1 - nb_ko / nb_total) * 100, 4)
    return pd.DataFrame([{
        "regle":   "R10d_conformite_timezone",
        "colonne": "Timezone",
        "nb_total": nb_total,
        "nb_ko":   nb_ko,
        "pct_ok":  pct_ok,
        "statut":  "OK" if nb_ko == 0 else "WARNING",
    }])


def score_qualite_global(df_all: pd.DataFrame) -> dict:
    """
    Calcule un score de qualité global (moyenne pondérée des pct_ok).
    Retourne un dict avec le score global et par dimension.
    """
    score_global = df_all["pct_ok"].mean().round(4)
    scores_dim   = df_all.groupby(
        df_all["regle"].str.extract(r"^(R\d+[a-z]?)")[0]
    )["pct_ok"].mean().round(4)

    print(f"\n{'='*50}")
    print(f"  SCORE QUALITÉ GLOBAL : {score_global:.2f} %")
    print(f"{'='*50}")

    return {"score_global": score_global, "scores_par_regle": scores_dim.to_dict()}


def run(df: DataFrame) -> pd.DataFrame:
    """Point d'entrée : exécute toutes les règles de conformité."""
    rules = charger_config()
    DIR_METRICS.mkdir(parents=True, exist_ok=True)

    print("\n[R10a — Country = US]")
    df_r10a = regle_country(df, rules)
    print(df_r10a[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))

    print("\n[R10b — Sunrise_Sunset valide]")
    df_r10b = regle_sunrise_sunset(df, rules)
    print(df_r10b[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))

    print("\n[R10c — Side valide]")
    df_r10c = regle_side(df, rules)
    if not df_r10c.empty:
        print(df_r10c[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))

    print("\n[R10d — Timezone US valide]")
    df_r10d = regle_timezone(df)
    print(df_r10d[["colonne", "nb_ko", "pct_ok", "statut"]].to_string(index=False))

    frames = [df_r10a, df_r10b, df_r10d]
    if not df_r10c.empty:
        frames.append(df_r10c)

    df_all = pd.concat(frames, ignore_index=True)
    chemin = DIR_METRICS / "conformite.csv"
    df_all.to_csv(chemin, index=False, encoding="utf-8")
    print(f"\n[OK] Résultats conformité → {chemin}")
    return df_all
