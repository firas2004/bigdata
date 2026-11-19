"""
prepare_powerbi.py
------------------
Partie 5 — Préparation des données pour le Dashboard Power BI.

Génère des CSVs "prêts à importer" dans Power BI depuis tous les métriques
produits par les Parties 1 à 4. Chaque CSV est aplati, nettoyé et documenté.

Outputs dans data/metrics/powerbi/ :
  - pbi_vue_globale.csv           : KPIs globaux (1 ligne)
  - pbi_qualite_dimensions.csv    : scores par dimension qualité
  - pbi_completude_colonnes.csv   : taux de complétude par colonne
  - pbi_outliers.csv              : outliers par colonne (Z-score + IQR)
  - pbi_doublons.csv              : doublons ID vs composite
  - pbi_drift_psi.csv             : PSI par colonne × mois
  - pbi_anomalies_mensuel.csv     : anomalies IF par mois
  - pbi_anomalies_zscore.csv      : anomalies Z-score par colonne × mois
  - pbi_performance_batches.csv   : débit et temps par batch
  - pbi_top_etats.csv             : accidents + sévérité + durée par État
  - pbi_stats_numeriques.csv      : stats descriptives des variables numériques
  - pbi_distribution_severity.csv : distribution de la sévérité
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# ── Chemins ─────────────────────────────────────────────────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
METRICS      = _PROJET_ROOT / "data" / "metrics"
OUT_DIR      = METRICS / "powerbi"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def sauvegarder(df: pd.DataFrame, nom: str):
    """Sauvegarde un DataFrame en CSV Power BI et affiche un résumé."""
    out = OUT_DIR / nom
    df.to_csv(out, index=False, encoding="utf-8-sig")   # utf-8-sig pour Excel/PBI
    print(f"  ✓ {nom:45s} → {len(df):>6,} lignes, {len(df.columns):>2} colonnes")
    return df


def charger_csv(chemin: Path, nom_affiche: str) -> pd.DataFrame:
    """Charge un CSV métrique avec gestion d'erreur."""
    if not chemin.exists():
        print(f"  ⚠ Fichier absent : {nom_affiche}")
        return pd.DataFrame()
    return pd.read_csv(chemin)


# ════════════════════════════════════════════════════════════════════════════════
# 1. Vue globale — KPIs sur 1 ligne
# ════════════════════════════════════════════════════════════════════════════════

def prep_vue_globale():
    """KPIs consolidés en une seule ligne pour les cartes Power BI."""
    scores = charger_csv(METRICS / "rules" / "scores_par_dimension.csv", "scores_par_dimension")
    manq   = charger_csv(METRICS / "profiling" / "valeurs_manquantes.csv", "valeurs_manquantes")
    doub   = charger_csv(METRICS / "profiling" / "doublons.csv", "doublons")
    perf   = charger_csv(METRICS / "performance" / "comparaison_modes.csv", "comparaison_modes")
    drift  = charger_csv(METRICS / "drift" / "drift_resume_colonnes.csv", "drift_resume_colonnes")
    if_csv = charger_csv(METRICS / "anomalies" / "anomalies_isolation_forest.csv", "anomalies_if")

    score_global = scores["score_moyen_pct"].mean() if not scores.empty else None
    nb_colonnes_manquantes_critiques = (
        manq[manq["pct_manquants"] > 20].shape[0] if not manq.empty else None
    )
    nb_doublons_id        = int(doub["nb_doublons_id"].values[0])        if not doub.empty else 0
    nb_doublons_composite = int(doub["nb_doublons_cle_composite"].values[0]) if not doub.empty else 0
    t_seq = perf.loc[perf["mode"] == "séquentiel", "t_total_s"].values[0] if not perf.empty else None
    t_par = perf.loc[perf["mode"] == "parallèle",  "t_total_s"].values[0] if not perf.empty else None
    gain  = round((t_seq - t_par) / t_seq * 100, 1) if t_seq and t_par else None
    nb_colonnes_drift = drift[drift["label_global"].isin(["WARNING","ALERT"])].shape[0] if not drift.empty else None
    pct_anomalies_if  = if_csv["pct_anomalies"].mean() if not if_csv.empty else None

    df = pd.DataFrame([{
        "nb_lignes_total":               7_728_394,
        "nb_colonnes":                   46,
        "score_qualite_global_pct":      round(score_global, 2) if score_global else None,
        "nb_colonnes_incompletes_20pct": nb_colonnes_manquantes_critiques,
        "nb_doublons_ID":                nb_doublons_id,
        "nb_doublons_composite":         nb_doublons_composite,
        "pct_doublons_composite":        round(nb_doublons_composite / 7_728_394 * 100, 2) if nb_doublons_composite else None,
        "nb_colonnes_drift":             nb_colonnes_drift,
        "pct_anomalies_if_moyen":        round(pct_anomalies_if, 2) if pct_anomalies_if else None,
        "temps_traitement_seq_s":        t_seq,
        "temps_traitement_par_s":        t_par,
        "gain_parallelisme_pct":         gain,
        "debit_max_rows_par_sec":        1_491_627,
    }])
    sauvegarder(df, "pbi_vue_globale.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 2. Qualité — scores par dimension
# ════════════════════════════════════════════════════════════════════════════════

def prep_qualite_dimensions():
    df = charger_csv(METRICS / "rules" / "scores_par_dimension.csv", "scores_par_dimension")
    if df.empty:
        return
    # Ajout label couleur pour Power BI (conditional formatting)
    df["label_couleur"] = df["score_moyen_pct"].apply(
        lambda s: "Rouge" if s < 95 else "Orange" if s < 98 else "Vert"
    )
    sauvegarder(df, "pbi_qualite_dimensions.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 3. Complétude par colonne
# ════════════════════════════════════════════════════════════════════════════════

def prep_completude():
    df = charger_csv(METRICS / "profiling" / "valeurs_manquantes.csv", "valeurs_manquantes")
    if df.empty:
        return
    df["pct_complet"] = (100 - df["pct_manquants"]).round(2)
    df["statut"] = df["pct_manquants"].apply(
        lambda p: "Critique" if p > 30 else "Warning" if p > 10 else "OK"
    )
    sauvegarder(df, "pbi_completude_colonnes.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 4. Outliers
# ════════════════════════════════════════════════════════════════════════════════

def prep_outliers():
    df = charger_csv(METRICS / "profiling" / "outliers.csv", "outliers")
    if df.empty:
        return
    df["statut"] = df.get("pct_outliers", pd.Series(dtype=float)).apply(
        lambda p: "Critique" if p > 5 else "Warning" if p > 2 else "OK"
        if not pd.isna(p) else "N/A"
    )
    sauvegarder(df, "pbi_outliers.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 5. Doublons
# ════════════════════════════════════════════════════════════════════════════════

def prep_doublons():
    df = charger_csv(METRICS / "profiling" / "doublons.csv", "doublons")
    if not df.empty:
        sauvegarder(df, "pbi_doublons.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 6. Drift PSI par colonne × mois
# ════════════════════════════════════════════════════════════════════════════════

def prep_drift():
    df = charger_csv(METRICS / "drift" / "drift_psi_ks.csv", "drift_psi_ks")
    if df.empty:
        return
    df["periode"] = df.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
    df["psi_categorie"] = df["label_psi"].map(
        {"STABLE": "Stable", "WARNING": "Attention", "ALERT": "Alerte", "N/A": "N/A"}
    )
    sauvegarder(df, "pbi_drift_psi.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 7. Anomalies Isolation Forest par mois
# ════════════════════════════════════════════════════════════════════════════════

def prep_anomalies_if():
    df = charger_csv(METRICS / "anomalies" / "anomalies_isolation_forest.csv", "anomalies_if")
    if df.empty:
        return
    df["periode"] = df.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
    df["statut"] = df["pct_anomalies"].apply(
        lambda p: "Élevé" if p > 5.03 else "Normal"
    )
    sauvegarder(df, "pbi_anomalies_mensuel.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 8. Anomalies Z-score par colonne × mois
# ════════════════════════════════════════════════════════════════════════════════

def prep_anomalies_zscore():
    df = charger_csv(METRICS / "anomalies" / "anomalies_zscore.csv", "anomalies_zscore")
    if df.empty:
        return
    df["periode"] = df.apply(lambda r: f"{int(r.year)}-{int(r.month):02d}", axis=1)
    sauvegarder(df, "pbi_anomalies_zscore.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 9. Performance des batches
# ════════════════════════════════════════════════════════════════════════════════

def prep_performance():
    seq = charger_csv(METRICS / "performance" / "perf_sequentiel.csv", "perf_sequentiel")
    par = charger_csv(METRICS / "performance" / "perf_parallele.csv",  "perf_parallele")

    if not seq.empty:
        seq["mode"] = "Séquentiel"
    if not par.empty:
        par["mode"] = "Parallèle"

    df_all = pd.concat([seq, par], ignore_index=True)
    if not df_all.empty:
        # Garde les colonnes utiles pour PBI
        cols = [c for c in [
            "mode","batch_id","annee","nb_lignes",
            "t_lecture_s","t_filtrage_s","t_enrichissement_s","t_agregation_s",
            "t_total_s","debit_rows_par_sec"
        ] if c in df_all.columns]
        sauvegarder(df_all[cols], "pbi_performance_batches.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 10. Top États
# ════════════════════════════════════════════════════════════════════════════════

def prep_top_etats():
    df = charger_csv(METRICS / "performance" / "agregation_par_state.csv", "agregation_par_state")
    if df.empty:
        return
    df["pct_accidents"] = (df["nb_accidents"] / df["nb_accidents"].sum() * 100).round(3)
    df["rang"] = df["nb_accidents"].rank(ascending=False, method="min").astype(int)
    sauvegarder(df, "pbi_top_etats.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 11. Stats numériques
# ════════════════════════════════════════════════════════════════════════════════

def prep_stats_numeriques():
    df = charger_csv(METRICS / "profiling" / "stats_numeriques.csv", "stats_numeriques")
    if not df.empty:
        sauvegarder(df, "pbi_stats_numeriques.csv")


# ════════════════════════════════════════════════════════════════════════════════
# 12. Distribution Severity
# ════════════════════════════════════════════════════════════════════════════════

def prep_severity():
    df = charger_csv(METRICS / "profiling" / "distribution_severity.csv", "distribution_severity")
    if df.empty:
        return
    df["pct"] = (df["count"] / df["count"].sum() * 100).round(2) if "count" in df.columns else None
    sauvegarder(df, "pbi_distribution_severity.csv")


# ════════════════════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("PARTIE 5 — Préparation données Power BI")
    print("=" * 60)
    print(f"\n  Destination : {OUT_DIR}\n")

    prep_vue_globale()
    prep_qualite_dimensions()
    prep_completude()
    prep_outliers()
    prep_doublons()
    prep_drift()
    prep_anomalies_if()
    prep_anomalies_zscore()
    prep_performance()
    prep_top_etats()
    prep_stats_numeriques()
    prep_severity()

    # Liste des fichiers générés
    fichiers = sorted(OUT_DIR.glob("pbi_*.csv"))
    print(f"\n  {'=' * 50}")
    print(f"  {len(fichiers)} fichiers Power BI générés dans :")
    print(f"  {OUT_DIR}")
    print(f"  {'=' * 50}")
    taille_totale = sum(f.stat().st_size for f in fichiers) / 1024
    print(f"  Taille totale : {taille_totale:.1f} Ko")
    print("\n✅ Partie 5 — Préparation PBI terminée")


if __name__ == "__main__":
    main()
