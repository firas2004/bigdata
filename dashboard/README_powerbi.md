# Dashboard Power BI — Big Data Observability (US Accidents)

## 📁 Fichiers à importer

Tous les fichiers sont dans `data/metrics/powerbi/` (encodage UTF-8 BOM, séparateur `,`).

### Import dans Power BI Desktop
1. `Accueil` → `Obtenir des données` → `Texte/CSV`
2. Sélectionner chaque fichier `pbi_*.csv`
3. Vérifier l'encodage : **UTF-8**
4. Cliquer `Transformer les données` pour vérifier les types

---

## 📊 Fichiers et visualisations suggérées

| Fichier | Description | Visualisation suggérée |
|---------|-------------|----------------------|
| `pbi_vue_globale.csv` | KPIs globaux (1 ligne) | **Cartes** : score qualité, nb lignes, nb anomalies |
| `pbi_qualite_dimensions.csv` | Score par dimension qualité | **Graphique en anneau** ou **jauge** par dimension |
| `pbi_completude_colonnes.csv` | Complétude par colonne | **Bar chart horizontal** trié par `pct_manquants` |
| `pbi_outliers.csv` | Outliers par colonne | **Bar chart** `nb_outliers` par `colonne` et `methode` |
| `pbi_doublons.csv` | Doublons ID vs composite | **Cartes** : nb_doublons_id, nb_doublons_cle_composite |
| `pbi_drift_psi.csv` | PSI par colonne × mois | **Heatmap** (Matrice) : colonne en ligne, période en colonne, valeur PSI |
| `pbi_anomalies_mensuel.csv` | Anomalies IF par mois | **Courbe** `pct_anomalies` sur `periode` |
| `pbi_anomalies_zscore.csv` | Anomalies Z-score par colonne × mois | **Matrice** ou **graphique en courbes** multiples |
| `pbi_performance_batches.csv` | Débit et temps par batch | **Bar chart groupé** : seq vs parallèle par `batch_id` |
| `pbi_top_etats.csv` | Accidents par État US | **Carte choroplèthe** (champ `State`, valeur `nb_accidents`) |
| `pbi_stats_numeriques.csv` | Stats descriptives numériques | **Tableau** avec mise en forme conditionnelle |
| `pbi_distribution_severity.csv` | Distribution sévérité 1-4 | **Graphique en anneau** ou **treemap** |

---

## 🗺️ Pages de dashboard suggérées

### Page 1 — Vue d'ensemble
- Carte : **Score qualité global** (99.33%)
- Carte : **Nombre de lignes** (7 728 394)
- Carte : **Anomalies IF** (5.01% en moyenne)
- Carte : **Colonnes en drift** (6/7)
- Graphique anneau : **Distribution Severity**
- Carte choroplèthe : **Accidents par État**

### Page 2 — Qualité des données
- Jauges : **Score par dimension** (Complétude, Unicité, Validité, Cohérence, Conformité)
- Bar chart horizontal : **Taux de complétude par colonne** (coloré par statut)
- Bar chart : **Outliers par colonne**
- Tableau : **Règles KO / WARNING** (depuis `pbi_qualite_dimensions.csv`)

### Page 3 — Data Drift
- Matrice / Heatmap : **PSI par colonne × période** (couleur : vert < 0.1, orange 0.1-0.2, rouge > 0.2)
- Courbe : **Nombre de colonnes en drift par mois**
- Filtre segment : **Colonne numérique**

### Page 4 — Anomalies
- Courbe : **% Anomalies IF par mois** (axe X = période, Y = pct_anomalies)
- Matrice : **% Anomalies Z-score par colonne × mois**
- Bar chart : **Anomalies Z-score moyennes par colonne**

### Page 5 — Performance Big Data
- Bar chart groupé : **Débit (rows/s) séquentiel vs parallèle** par batch
- Bar chart empilé : **Décomposition du temps** (lecture / filtrage / enrichissement / agrégation)
- Carte : **Gain du parallélisme** : 33.4%
- Tableau : **Top 10 États** avec sévérité et durée moyenne

---

## 🔗 Relations entre tables (modèle de données)

```
pbi_drift_psi          ──(colonne)──► pbi_stats_numeriques
pbi_anomalies_zscore   ──(colonne)──► pbi_stats_numeriques
pbi_completude_colonnes ─(colonne)──► pbi_stats_numeriques
pbi_performance_batches ─(batch_id)─► (pas de jointure directe)
```

> Dans Power BI : `Modélisation` → `Vue du modèle` → glisser les colonnes pour créer les relations.

---

## 📐 Mesures DAX utiles

```dax
// Score qualité global
Score Global = AVERAGE(pbi_qualite_dimensions[score_moyen_pct])

// % colonnes en drift
Pct Colonnes Drift =
  DIVIDE(
    COUNTROWS(FILTER(pbi_drift_psi, pbi_drift_psi[label_psi] IN {"WARNING","ALERT"})),
    COUNTROWS(pbi_drift_psi)
  ) * 100

// Gain parallélisme
Gain Parallelisme =
  VAR t_seq = CALCULATE(SUM(pbi_performance_batches[t_total_s]),
               pbi_performance_batches[mode] = "Séquentiel")
  VAR t_par = CALCULATE(SUM(pbi_performance_batches[t_total_s]),
               pbi_performance_batches[mode] = "Parallèle")
  RETURN DIVIDE(t_seq - t_par, t_seq) * 100
```

---

## ⚙️ Mise à jour des données

Pour régénérer les fichiers PBI après une nouvelle exécution :
```powershell
cd c:\Users\user\Desktop\bigdata
.venv\Scripts\python.exe src\pipeline\prepare_powerbi.py
```

Puis dans Power BI Desktop : `Actualiser` (Ctrl+Alt+F5)
