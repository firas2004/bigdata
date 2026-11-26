/* ==========================================================================
   Big Data Observability — Logique interactive du Dashboard Web
   ========================================================================== */

// Catalogue complet des 18 figures avec légendes structurées
const FIGURES = [
  // ── Partie 1 : Data Profiling ──
  {
    id: "fig-1",
    part: "profiling",
    partLabel: "Partie 1 • Data Profiling",
    file: "../../reports/figures/valeurs_manquantes.png",
    title: "Taux de valeurs manquantes par attribut",
    caption: "Analyse exhaustive des 7 728 394 lignes : End_Lat et End_Lng affichent 44,0 % de valeurs nulles, suivis par Precipitation(in) (28,5 %) et Wind_Chill(F) (25,9 %). Les champs critiques (ID, Severity, Start_Time, State) sont complets à 100 %."
  },
  {
    id: "fig-2",
    part: "profiling",
    partLabel: "Partie 1 • Data Profiling",
    file: "../../reports/figures/histogrammes_numeriques.png",
    title: "Distribution des variables numériques (échantillon 5 %)",
    caption: "Histogrammes des 7 attributs météorologiques et routiers. On observe une forte asymétrie à droite pour Precipitation(in) et Distance(mi), tandis que Temperature(F) et Humidity(%) suivent des distributions plus étalées."
  },
  {
    id: "fig-3",
    part: "profiling",
    partLabel: "Partie 1 • Data Profiling",
    file: "../../reports/figures/boxplots_numeriques.png",
    title: "Boîtes à moustaches et dispersion des valeurs extrêmes",
    caption: "Visualisation des quartiles et des valeurs extrêmes. Met en évidence des anomalies physiques brutes : Wind_Speed(mph) jusqu'à 1 087 mph, Temperature(F) à 207 °F et Pressure(in) à 58,63 inHg."
  },

  // ── Partie 3 : Anomaly Detection & Data Drift ──
  {
    id: "fig-4",
    part: "anomaly",
    partLabel: "Partie 3 • Data Drift",
    file: "../../reports/figures/drift_psi_heatmap.png",
    title: "Heatmap du Population Stability Index (PSI) par mois",
    caption: "Comparaison mensuelle (86 périodes) contre les 3 premiers mois de référence. Le rouge intense (PSI ≥ 0,2) sur Temperature(F), Wind_Speed(mph) et Pressure(in) illustre la forte saisonnalité météorologique."
  },
  {
    id: "fig-5",
    part: "anomaly",
    partLabel: "Partie 3 • Data Drift",
    file: "../../reports/figures/drift_ks_pvalue.png",
    title: "Test statistique de Kolmogorov-Smirnov (p-values mensuelles)",
    caption: "Évolution de la p-value du test KS par variable. Les barres rouges (p < 0,05) signalent un rejet de l'hypothèse nulle de distribution identique, confirmant l'alerte PSI sur la majorité des mois."
  },
  {
    id: "fig-6",
    part: "anomaly",
    partLabel: "Partie 3 • Data Drift",
    file: "../../reports/figures/drift_resume_mensuel.png",
    title: "Nombre de colonnes en drift détecté par mois",
    caption: "Synthèse temporelle : 6 colonnes sur 7 présentent régulièrement une dérive saisonnière, particulièrement marquée entre les saisons hivernales et estivales."
  },
  {
    id: "fig-7",
    part: "anomaly",
    partLabel: "Partie 3 • Anomalies",
    file: "../../reports/figures/anomalies_zscore_heatmap.png",
    title: "Heatmap des anomalies univariées Z-Score (|Z| ≥ 3σ)",
    caption: "Pourcentage mensuel d'observations dépassant 3 écarts-types par variable. Pressure(in) concentre les pics les plus fréquents (jusqu'à 4,36 %)."
  },
  {
    id: "fig-8",
    part: "anomaly",
    partLabel: "Partie 3 • Anomalies",
    file: "../../reports/figures/anomalies_zscore_par_colonne.png",
    title: "Taux moyen d'anomalies Z-Score par attribut",
    caption: "Classement des variables les plus touchées par des écarts extrêmes : Pressure(in) (2,25 %), Visibility(mi) (1,83 %), Distance(mi) (1,44 %) et Precipitation(in) (1,39 %)."
  },
  {
    id: "fig-9",
    part: "anomaly",
    partLabel: "Partie 3 • Anomalies",
    file: "../../reports/figures/anomalies_isolation_forest.png",
    title: "Suivi mensuel des anomalies multivariées (Isolation Forest)",
    caption: "Modèle non supervisé (100 arbres, contamination 5 %). Le taux reste maîtrisé autour de 5,01 % avec un pic notable à 5,05 % en juillet 2020 (période post-confinement COVID)."
  },

  // ── Partie 4 : Big Data Processing ──
  {
    id: "fig-10",
    part: "processing",
    partLabel: "Partie 4 • Big Data Processing",
    file: "../../reports/figures/perf_lignes_par_batch.png",
    title: "Volumétrie des 8 micro-batches annuels (2016–2023)",
    caption: "Répartition des 7 728 394 lignes en batches Parquet annuels : croissance continue de 410 821 lignes (2016) jusqu'à 1 762 452 lignes (2022), puis 246 633 lignes partielles pour début 2023."
  },
  {
    id: "fig-11",
    part: "processing",
    partLabel: "Partie 4 • Big Data Processing",
    file: "../../reports/figures/perf_debit_par_batch.png",
    title: "Débit par batch : exécution séquentielle vs parallèle",
    caption: "Comparaison du débit (lignes/seconde) par année. Le mode séquentiel atteint 1,49 M rows/s sur 2022, tandis que le mode parallèle réduit le temps total de 33,4 % grâce au chevauchement des tâches."
  },
  {
    id: "fig-12",
    part: "processing",
    partLabel: "Partie 4 • Big Data Processing",
    file: "../../reports/figures/perf_decomposition_temps.png",
    title: "Décomposition du temps par étape Spark",
    caption: "Ventilation du coût d'exécution : lecture Parquet, filtrage temporel, enrichissement et agrégation spatiale par État. L'agrégation (shuffle) représente la majorité du temps de calcul."
  },
  {
    id: "fig-13",
    part: "processing",
    partLabel: "Partie 4 • Big Data Processing",
    file: "../../reports/figures/perf_top_states.png",
    title: "Top 15 des États américains par volume d'accidents",
    caption: "Concentration géographique issue de l'agrégation PySpark : la Californie (CA, 1,74 M), la Floride (FL, 880 k) et le Texas (TX, 583 k) regroupent plus de 41 % des accidents nationaux."
  },

  // ── Section 6 : Expérimentations & Évaluation ──
  {
    id: "fig-14",
    part: "experiments",
    partLabel: "Expérience 1 • Scalabilité",
    file: "../../reports/figures/exp1_temps_vs_volume.png",
    title: "Expérience 1 : Temps d'exécution selon le volume (V1 → V4)",
    caption: "Mesure après warmup JIT : V1 (10 % = 773k) en 2,43 s, V2 (25 % = 1,93M) en 2,60 s, V3 (50 % = 3,87M) en 3,97 s, V4 (100 % = 7,73M) en 7,36 s. Scalabilité quasi-linéaire maîtrisée."
  },
  {
    id: "fig-15",
    part: "experiments",
    partLabel: "Expérience 1 • Scalabilité",
    file: "../../reports/figures/exp1_debit_vs_volume.png",
    title: "Expérience 1 : Évolution du débit (lignes/seconde)",
    caption: "Montée en charge du débit PySpark : de 318 432 rows/s sur V1 jusqu'à 1 049 806 rows/s sur V4 (100 %), démontrant l'amortissement de l'overhead fixe sur gros volumes."
  },
  {
    id: "fig-16",
    part: "experiments",
    partLabel: "Expérience 2 • Concurrence",
    file: "../../reports/figures/exp2_seq_vs_concurrent.png",
    title: "Expérience 2 : Traitement Séquentiel vs Soumission Concurrente",
    caption: "Mesure corrigée sans biais de lazy evaluation : 11,23 s en séquentiel (687k rows/s) contre 5,22 s en soumission concurrente à 2 workers (1,48 M rows/s), soit un gain net de +53,5 %."
  },
  {
    id: "fig-17",
    part: "experiments",
    partLabel: "Expérience 3 • Règles Qualité",
    file: "../../reports/figures/exp3_quality_eval.png",
    title: "Expérience 3 : Évaluation sur 165 erreurs contrôlées injectées",
    caption: "Validation rigoureuse sur copie saine : 50 erreurs Complétude, 40 Validité, 30 Cohérence, 25 Unicité et 20 Conformité injectées. Résultat : 100,0 % de taux de détection et 0 faux positif."
  },
  {
    id: "fig-18",
    part: "experiments",
    partLabel: "Expérience 4 • Anomalies & Drift",
    file: "../../reports/figures/exp4_anomaly_and_drift.png",
    title: "Expérience 4 : Drift 2016–2019 vs 2020–2023 & Précision/Rappel",
    caption: "À gauche : Wind_Speed(mph) (PSI=3,23) et Distance(mi) (PSI=0,66) révèlent un drift pluriannuel réel tandis que Temperature reste stable. À droite : Isolation Forest surpasse Z-Score en précision (47,2 % vs 17,0 %)."
  }
];

let currentFilter = "all";
let currentLightboxIdx = 0;

// ── Rendu d'une grille de figures ──────────────────────────────────────────
function createFigureCardHTML(fig) {
  return `
    <article class="figure-card" data-part="${fig.part}">
      <div class="figure-img-wrap" onclick="openLightbox('${fig.id}')">
        <img src="${fig.file}" alt="${fig.title}" loading="lazy" />
        <span class="figure-zoom-hint">Agrandir</span>
      </div>
      <div class="figure-body">
        <div class="figure-meta">
          <span>${fig.partLabel}</span>
          <span class="mono">${fig.file.split("/").pop()}</span>
        </div>
        <h4 class="figure-title">${fig.title}</h4>
        <p class="figure-caption">${fig.caption}</p>
      </div>
    </article>
  `;
}

function renderSectionFigures() {
  const mapping = {
    "grid-profiling": "profiling",
    "grid-anomaly": "anomaly",
    "grid-processing": "processing",
    "grid-experiments": "experiments"
  };

  Object.entries(mapping).forEach(([containerId, partName]) => {
    const el = document.getElementById(containerId);
    if (!el) return;
    const list = FIGURES.filter(f => f.part === partName);
    el.innerHTML = list.map(createFigureCardHTML).join("");
  });
}

function renderGallery(filter = "all") {
  currentFilter = filter;
  const grid = document.getElementById("grid-all-figures");
  if (!grid) return;
  const filtered = filter === "all" ? FIGURES : FIGURES.filter(f => f.part === filter);
  grid.innerHTML = filtered.map(createFigureCardHTML).join("");
}

// ── Lightbox ───────────────────────────────────────────────────────────────
function openLightbox(figId) {
  const idx = FIGURES.findIndex(f => f.id === figId);
  if (idx === -1) return;
  currentLightboxIdx = idx;
  updateLightbox();
  document.getElementById("lightbox").classList.add("open");
}

function closeLightbox() {
  document.getElementById("lightbox").classList.remove("open");
}

function stepLightbox(delta) {
  currentLightboxIdx = (currentLightboxIdx + delta + FIGURES.length) % FIGURES.length;
  updateLightbox();
}

function updateLightbox() {
  const fig = FIGURES[currentLightboxIdx];
  document.getElementById("lb-title").textContent = `${fig.partLabel} — ${fig.title}`;
  document.getElementById("lb-img").src = fig.file;
  document.getElementById("lb-img").alt = fig.title;
  document.getElementById("lb-caption").textContent = fig.caption;
}

// ── Onglets Expériences ────────────────────────────────────────────────────
function initExperimentTabs() {
  const tabs = document.querySelectorAll(".exp-tab");
  const panels = document.querySelectorAll(".exp-panel");

  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.target;
      tabs.forEach(t => t.classList.remove("active"));
      panels.forEach(p => p.classList.remove("active"));
      tab.classList.add("active");
      document.getElementById(target)?.classList.add("active");
    });
  });
}

// ── Filtres Galerie Complète ───────────────────────────────────────────────
function initGalleryFilters() {
  const btns = document.querySelectorAll(".filter-btn");
  btns.forEach(btn => {
    btn.addEventListener("click", () => {
      btns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      renderGallery(btn.dataset.filter);
    });
  });
}

// ── Copie Presse-papiers (DAX / CLI) ───────────────────────────────────────
function initCopyButtons() {
  document.querySelectorAll(".btn-copy").forEach(btn => {
    btn.addEventListener("click", () => {
      const code = btn.parentElement.querySelector("pre").innerText;
      navigator.clipboard.writeText(code).then(() => {
        const prev = btn.textContent;
        btn.textContent = "Copié ✓";
        setTimeout(() => { btn.textContent = prev; }, 1600);
      });
    });
  });
}

// ── Thème Clair / Sombre ───────────────────────────────────────────────────
function initThemeToggle() {
  const btn = document.getElementById("btn-theme");
  const saved = localStorage.getItem("bdo-theme") || "dark";
  document.documentElement.setAttribute("data-theme", saved);
  btn.textContent = saved === "dark" ? "☀️ Mode Clair" : "🌙 Mode Sombre";

  btn.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme");
    const next = current === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("bdo-theme", next);
    btn.textContent = next === "dark" ? "☀️ Mode Clair" : "🌙 Mode Sombre";
  });
}

// ── Scroll Spy Sidebar & Mobile Menu ───────────────────────────────────────
function initScrollSpy() {
  const sections = document.querySelectorAll("section[id]");
  const navLinks = document.querySelectorAll(".nav-link");

  const obs = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const id = entry.target.getAttribute("id");
        navLinks.forEach(l => {
          l.classList.toggle("active", l.getAttribute("href") === `#${id}`);
        });
      }
    });
  }, { rootMargin: "-20% 0px -65% 0px" });

  sections.forEach(s => obs.observe(s));

  const sidebar = document.getElementById("sidebar");
  document.getElementById("btn-menu")?.addEventListener("click", () => {
    sidebar.classList.toggle("open");
  });
  navLinks.forEach(l => l.addEventListener("click", () => {
    sidebar.classList.remove("open");
  }));
}

// ── Raccourcis Clavier Lightbox ────────────────────────────────────────────
document.addEventListener("keydown", e => {
  const lb = document.getElementById("lightbox");
  if (!lb.classList.contains("open")) return;
  if (e.key === "Escape") closeLightbox();
  if (e.key === "ArrowRight") stepLightbox(1);
  if (e.key === "ArrowLeft") stepLightbox(-1);
});

document.addEventListener("DOMContentLoaded", () => {
  renderSectionFigures();
  renderGallery("all");
  initExperimentTabs();
  initGalleryFilters();
  initCopyButtons();
  initThemeToggle();
  initScrollSpy();
});
