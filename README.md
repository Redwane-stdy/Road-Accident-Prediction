<p align="center">
  <img src="https://img.icons8.com/fluency/96/car-accident.png" alt="Road Accident Prediction Logo" width="96"/>
</p>

<h1 align="center">Road Accident Severity Prediction</h1>

<p align="center">
  <em>CSC5003 – Analyse et Prédiction des Accidents de la Route</em>
</p>

<p align="center">
  A distributed machine-learning pipeline that predicts the severity of road accidents<br/>
  from their initial characteristics (location, weather, time of day)<br/>
  to help emergency services allocate resources more effectively.
</p>

<p align="center">
  <!-- Core stack -->
  <img src="https://img.shields.io/badge/Scala-2.12.15-DC322F?style=for-the-badge&logo=scala&logoColor=white" alt="Scala"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-3.3.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/Java-8-007396?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 8"/>
  <img src="https://img.shields.io/badge/SBT-1.x-22BAF1?style=for-the-badge&logo=scala&logoColor=white" alt="SBT"/>
  <br/>
  <!-- Python stack -->
  <img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Plotly-5.x-3F4F75?style=for-the-badge&logo=plotly&logoColor=white" alt="Plotly"/>
  <img src="https://img.shields.io/badge/pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="pandas"/>
  <img src="https://img.shields.io/badge/scikit--learn-1.x-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white" alt="scikit-learn"/>
  <br/>
  <!-- Meta -->
  <img src="https://img.shields.io/badge/Dataset-2023%20French%20Road%20Accidents-blue?style=for-the-badge" alt="Dataset"/>
  <img src="https://img.shields.io/badge/Accidents-~62%2C000-important?style=for-the-badge" alt="~62,000 accidents"/>
  <img src="https://img.shields.io/badge/Models-Random%20Forest%20%7C%20LR%20%7C%20DT-success?style=for-the-badge" alt="Models"/>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Research Questions](#research-questions)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [ML Pipeline](#ml-pipeline)
- [Model Results](#model-results)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Visualizations](#visualizations)
- [Known Limitations](#known-limitations)

---

## Overview

This project processes the **2023 French national road-accident dataset** (~62 000 accidents, ~40 MB across 4 CSV files) using **Apache Spark on Scala** for the heavy lifting and **Python/Plotly** for interactive dashboards.

The target variable is `accident_grave` — a binary label indicating whether an accident resulted in at least one hospitalization or fatality (severity ≤ 3 on the French scale).

---

## Research Questions

1. Which conditions (weather, lighting, time of day) are most correlated with severe accidents?
2. Are there geographic areas (departments) with a disproportionately high rate of severe accidents?
3. Does the type of vehicle involved act as a predictive factor for accident severity?

---

## Architecture

```
Raw CSVs (4 files)
       │
       ▼
┌─────────────────────────────────────────────────┐
│              Scala / Apache Spark               │
│                                                 │
│  DataLoader ──► DataCleaner ──► FeatureEng ──► ModelTrainer │
│                                                 │
│  Output: cleaned_data.csv, metrics.csv,         │
│          *_predictions.csv, prepared_for_viz.csv│
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│              Python / Plotly                    │
│                                                 │
│  dashboard.py → 6 interactive HTML dashboards   │
└─────────────────────────────────────────────────┘
```

---

## Tech Stack

<p>
  <img src="https://img.shields.io/badge/Scala-2.12.15-DC322F?style=flat-square&logo=scala&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-3.3.0-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Spark%20MLlib-3.3.0-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Java-8-007396?style=flat-square&logo=openjdk&logoColor=white"/>
  <img src="https://img.shields.io/badge/SBT-build%20tool-22BAF1?style=flat-square&logo=scala&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Plotly-interactive%20viz-3F4F75?style=flat-square&logo=plotly&logoColor=white"/>
  <img src="https://img.shields.io/badge/pandas-data%20wrangling-150458?style=flat-square&logo=pandas&logoColor=white"/>
  <img src="https://img.shields.io/badge/scikit--learn-ML%20utils-F7931E?style=flat-square&logo=scikit-learn&logoColor=white"/>
  <img src="https://img.shields.io/badge/seaborn-statistical%20plots-4C72B0?style=flat-square"/>
</p>

| Layer | Technology |
|---|---|
| Distributed processing | Apache Spark 3.3.0 (MLlib, SQL) |
| Language | Scala 2.12.15 |
| Build tool | SBT |
| Runtime | Java 8 |
| Visualization | Python 3, Plotly, pandas, seaborn |
| ML models | Random Forest · Logistic Regression · Decision Tree |

---

## Dataset

Four semicolon-delimited CSV files from [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/) (2023):

| File | Description | Size |
|---|---|---|
| `caracteristiques-2023.csv` | Accident characteristics (time, weather, location) | 6.6 MB |
| `lieux-2023.csv` | Road & location details | 7.2 MB |
| `usagers-2023.csv` | Persons involved (severity per person) | 13.9 MB |
| `vehicules-2023.csv` | Vehicles involved | 6.4 MB |

All four files are joined on the `Num_Acc` key.

**Feature groups used:**

- **Temporal** — hour, time-of-day bucket, weekend flag
- **Location** — department, GPS coordinates, agglomeration type
- **Road** — category, number of lanes, speed limit, surface, profile
- **Weather / Lighting** — luminosity level, atmospheric conditions
- **Collision** — type of collision, intersection type
- **Vehicle** — counts of light vehicles, trucks, motorcycles, bicycles; heavy-vehicle presence flag

---

## ML Pipeline

```
Raw data
  └─ DataLoader     : load CSVs with type-safe schemas
  └─ DataCleaner    : French decimal conversion, null handling, table merge
  └─ FeatureEng     : correlation analysis, geographic aggregation,
                      VectorAssembler (27 numeric + 1 categorical features)
  └─ ModelTrainer   : 80/20 split → train 3 models → evaluate → export
```

**Models trained:**

| Model | Key Hyperparameters |
|---|---|
| Random Forest | 100 trees, max depth 10 |
| Logistic Regression | 100 iterations, L2 regularization |
| Decision Tree | max depth 10 |

Metrics recorded: Accuracy, Precision, Recall, F1-Score, AUC-ROC.

---

## Model Results

| Metric | Value |
|---|---|
| Overall accuracy | 77.1 % |
| Precision (severe class) | 71.5 % |
| Recall (severe class) | **31 %** |
| Class distribution | 71.9 % non-severe / 28.1 % severe |

> **Note:** The high accuracy is misleading due to class imbalance. The model misses ~69 % of genuinely severe accidents. See [Known Limitations](#known-limitations).

---

## Quick Start

### Prerequisites

```bash
# Java 8 (required by Spark)
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
java -version   # should print 1.8.x

# SBT
brew install sbt

# Python virtual environment
python3 -m venv venv
source venv/bin/activate
pip install pandas plotly numpy scikit-learn matplotlib seaborn pyspark
```

Place the four 2023 CSV files in `data/` before running.

### Run the full pipeline

```bash
./run_project.sh
```

This script runs 7 sequential steps: environment check → data validation → Spark pipeline → Python dashboard → open results.

### Run steps manually

```bash
# 1. Spark pipeline (compile + run)
sbt clean compile run

# 2. Generate dashboards
python dashboard.py

# 3. Open results
open data/visualizations/01_synthese_dashboard.html
```

---

## Project Structure

```
Road-Accident-Prediction/
├── build.sbt                              # SBT / Spark dependencies
├── run_project.sh                         # End-to-end execution script
├── dashboard.py                           # Python visualization script
├── src/main/scala/com/roadaccidents/
│   ├── Main.scala                         # Pipeline orchestrator
│   ├── DataLoader.scala                   # CSV ingestion
│   ├── DataCleaner.scala                  # Cleaning & table merging
│   ├── FeatureEngineering.scala           # Feature creation & analysis
│   └── ModelTrainer.scala                 # Model training & evaluation
├── data/
│   ├── caracteristiques-2023.csv
│   ├── lieux-2023.csv
│   ├── usagers-2023.csv
│   ├── vehicules-2023.csv
│   └── visualizations/                    # Generated HTML dashboards
├── report/
│   └── modelPasUtile.md                  # Confusion matrix analysis
└── project/
    └── build.properties
```

---

## Visualizations

Six interactive Plotly dashboards are generated in `data/visualizations/`:

| File | Content |
|---|---|
| `01_synthese_dashboard.html` | General statistics overview |
| `02_comparaison_modeles.html` | Model-by-model performance comparison |
| `03_facteurs_risque.html` | Risk factor analysis (weather, lighting…) |
| `04_analyse_temporelle.html` | Temporal distribution of accidents |
| `05_impact_vehicules.html` | Severity by vehicle type |
| `06_classement_risques.html` | Department-level risk ranking |

---

## Known Limitations

| Issue | Impact | Suggested Fix |
|---|---|---|
| Class imbalance (72/28) | Model biased toward non-severe class | SMOTE oversampling or class weighting |
| Recall on severe class = 31 % | Misses 69 % of genuinely dangerous accidents | Cost-sensitive learning (higher penalty for FN) |
| Uniform cost function | FP and FN treated equally | Custom threshold tuning for safety context |

In a safety-critical context, **false negatives are far more costly than false positives**. The next iteration should prioritize maximizing recall for the severe class, even at the expense of overall accuracy.

---

## License

Academic project — CSC5003. Not licensed for production use.
