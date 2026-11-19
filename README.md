# Big Data Observability — US Accidents (2016-2023)

## Objectif
Plateforme de surveillance de la qualité des données construite sur le dataset Kaggle
**US Accidents (2016-2023)** (~7,7 M de lignes, 46 colonnes, ~3 Go).

Le projet couvre 5 parties :
| # | Partie | Description |
|---|--------|-------------|
| 1 | Data Profiling | Analyse exploratoire de la qualité des données |
| 2 | Data Quality Rules | Règles de complétude, unicité, validité, cohérence, conformité |
| 3 | Anomaly Detection & Data Drift | Détection d'anomalies et de dérives temporelles |
| 4 | Big Data Processing | Micro-batches, PySpark, mesures de performance |
| 5 | Dashboard Power BI | Visualisation interactive des métriques de qualité |

## Structure du projet
```
project/
├── data/
│   ├── raw/          ← CSV brut Kaggle (non versionné)
│   ├── parquet/      ← Dataset converti en Parquet partitionné
│   ├── batches/      ← Micro-batches pour les tests de performance
│   └── metrics/      ← Résultats des analyses (CSV)
│       └── profiling/
├── notebooks/        ← Jupyter Notebooks par partie
├── src/
│   ├── profiling/    ← Module de profilage des données
│   ├── rules/        ← Règles de qualité (Partie 2)
│   ├── anomaly/      ← Détection d'anomalies (Partie 3)
│   └── pipeline/     ← Conversion et traitement PySpark (Partie 4)
├── dashboard/        ← Fichiers Power BI (.pbix)
├── reports/
│   └── figures/      ← Graphiques générés (PNG)
├── config/
│   ├── rules.yaml    ← Paramétrage des règles de qualité
│   └── thresholds.yaml ← Seuils d'alerte
├── requirements.txt
└── README.md
```

## Installation

### Prérequis
- **Python** ≥ 3.10 ([python.org](https://www.python.org/downloads/))
- **Java** 11 ou 17 (requis par PySpark) — [Adoptium](https://adoptium.net/)
- **Git** ([git-scm.com](https://git-scm.com/))
- Un compte **Kaggle** avec un token API (`~/.kaggle/kaggle.json`)

### Mise en place

```bash
# 1. Cloner le dépôt
git clone <url-du-repo>
cd bigdata

# 2. Créer l'environnement virtuel
python -m venv .venv

# 3. Activer l'environnement
# Windows :
.venv\Scripts\activate
# Linux/macOS :
source .venv/bin/activate

# 4. Installer les dépendances
pip install -r requirements.txt

# 5. Télécharger le dataset
kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw --unzip
```

### Démarrer le notebook de profilage

```bash
.venv\Scripts\activate        # Windows
jupyter notebook notebooks/01_profiling.ipynb
```

## Dataset
- **Source :** [US Accidents (Kaggle)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- **Auteur :** Sobhan Moosavi
- **Période :** Février 2016 – Mars 2023
- **Volume :** ~7,7 M lignes, 46 colonnes, ~3 Go (CSV)
- **Licence :** CC BY-SA 4.0

## Auteur
Projet Big Data Observability — 2024
