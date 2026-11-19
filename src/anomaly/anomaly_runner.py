"""
anomaly_runner.py
-----------------
Orchestrateur Partie 3 — lance le drift detector puis l'anomaly detector
et génère un rapport de synthèse consolidé.
"""

import os
import sys
import time
from pathlib import Path

# ── Hadoop doit être défini AVANT tout import pyspark ──────────────────────────
_PROJET_ROOT = Path(__file__).resolve().parents[2]
_HADOOP_HOME  = str(_PROJET_ROOT / "hadoop")
os.environ["HADOOP_HOME"]    = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = str(Path(_HADOOP_HOME) / "bin") + os.pathsep + os.environ.get("PATH", "")

sys.path.insert(0, str(_PROJET_ROOT / "src"))

import pandas as pd
import yaml

from anomaly.drift_detector    import main as run_drift
from anomaly.anomaly_detector  import main as run_anomaly


METRICS_DIR = _PROJET_ROOT / "data" / "metrics"


def generer_synthese_partie3():
    """
    Lit les CSVs de Partie 3 et génère un rapport de synthèse consolidé.
    """
    print("\n" + "=" * 60)
    print("SYNTHÈSE — Partie 3")
    print("=" * 60)

    lignes = []

    # ── Drift ────────────────────────────────────────────────────────────────────
    drift_csv = METRICS_DIR / "drift" / "drift_resume_colonnes.csv"
    if drift_csv.exists():
        df_drift = pd.read_csv(drift_csv)
        lignes.append("\n## Data Drift (PSI + KS)")
        lignes.append(f"Colonnes analysées : {len(df_drift)}")
        alertes = df_drift[df_drift["label_global"].isin(["WARNING", "ALERT"])]
        lignes.append(f"Colonnes avec drift : {len(alertes)} / {len(df_drift)}")
        if not alertes.empty:
            lignes.append("\nTop colonnes en drift :")
            for _, r in alertes.iterrows():
                lignes.append(
                    f"  • {r['colonne']} → PSI moyen={r['psi_moyen']:.4f} "
                    f"({r['label_global']}), {r['pct_mois_drift']:.1f}% des mois"
                )

    # ── Anomalies Z-score ────────────────────────────────────────────────────────
    zs_csv = METRICS_DIR / "anomalies" / "anomalies_zscore.csv"
    if zs_csv.exists():
        df_zs = pd.read_csv(zs_csv)
        resume_zs = (
            df_zs.groupby("colonne")["pct_anomalies"]
            .mean()
            .sort_values(ascending=False)
        )
        lignes.append("\n## Anomalies Z-score")
        for col, pct in resume_zs.items():
            flag = "🔴" if pct > 5 else "🟡" if pct > 2 else "🟢"
            lignes.append(f"  {flag} {col} : {pct:.2f}% d'anomalies en moyenne")

    # ── Isolation Forest ─────────────────────────────────────────────────────────
    if_csv = METRICS_DIR / "anomalies" / "anomalies_isolation_forest.csv"
    if if_csv.exists():
        df_if = pd.read_csv(if_csv)
        pct_moy = df_if["pct_anomalies"].mean()
        pct_max = df_if["pct_anomalies"].max()
        mois_max = df_if.loc[df_if["pct_anomalies"].idxmax(), "month"] if not df_if.empty else "?"
        an_max   = df_if.loc[df_if["pct_anomalies"].idxmax(), "year"]  if not df_if.empty else "?"
        lignes.append("\n## Isolation Forest")
        lignes.append(f"  • % anomalies moyen : {pct_moy:.2f}%")
        lignes.append(f"  • % anomalies max   : {pct_max:.2f}% (mois {int(an_max)}-{int(mois_max):02d})")

    # ── Affichage console ────────────────────────────────────────────────────────
    for l in lignes:
        print(l)

    # ── Export synthèse CSV ──────────────────────────────────────────────────────
    out_synthese = METRICS_DIR / "drift" / "synthese_partie3.txt"
    with open(out_synthese, "w", encoding="utf-8") as f:
        f.write("\n".join(lignes))
    print(f"\n✅ Synthèse Partie 3 exportée : {out_synthese}")


def main():
    t_total = time.time()

    print("╔══════════════════════════════════════════════════════════╗")
    print("║     PARTIE 3 — Anomaly Detection & Data Drift           ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # ── 3a : Drift ───────────────────────────────────────────────────────────────
    print("\n\n── 3a : Data Drift ──────────────────────────────────────")
    run_drift()

    # ── 3b : Anomalies ───────────────────────────────────────────────────────────
    print("\n\n── 3b : Anomaly Detection ───────────────────────────────")
    run_anomaly()

    # ── Synthèse ─────────────────────────────────────────────────────────────────
    generer_synthese_partie3()

    print(f"\n✅ Partie 3 complète en {time.time() - t_total:.1f}s")


if __name__ == "__main__":
    main()
