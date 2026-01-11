# Projet CSC5003 - Analyse et Prédiction des Accidents de la Route

## Objectif Principal
Étudier la gravité potentielle d'un accident de la route en fonction de ses premières caractéristiques (lieu, météo, heure) pour mieux allouer les ressources d'urgence.

## Questions Intermédiaires
1. Quelles conditions sont le plus souvent corrélées aux accidents graves ?
2. Existe-t-il des zones géographiques avec une proportion élevée d'accidents graves ?
3. Le type de véhicule est-il un facteur prédictif de la gravité ?

## Architecture du Projet

```
Projet/
├── build.sbt                    # Configuration SBT
├── src/main/scala/com/roadaccidents/
│   ├── DataLoader.scala         # Chargement des données
│   ├── DataCleaner.scala        # Nettoyage des données
│   ├── FeatureEngineering.scala # Création des features
│   ├── ModelTrainer.scala       # Entraînement des modèles
│   └── Main.scala               # Point d'entrée principal
├── dashboard.py                 # Visualisation Python
├── report/                      # Rapports et analyses
└── data/                        # Données CSV
```

## Configuration Environnement

### Java
```bash
# Lister les versions disponibles
/usr/libexec/java_home -V

# Définir Java 8 (requis pour Spark) (Prendre un version compatible)
export JAVA_HOME=/Users/red1/Library/Java/JavaVirtualMachines/corretto-1.8.0_402/Contents/Home
```

### Python
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas plotly numpy scikit-learn matplotlib seaborn pyspark
```

## Étapes du Projet

1. **Nettoyage des données** (Scala/Spark)
2. **Exploration et visualisation** (Python)
3. **Feature Engineering** (Scala/Spark)
4. **Modélisation** (Scala/Spark - Random Forest, Logistic Regression, Decision Tree)
5. **Prédiction et évaluation** (Scala/Spark)
6. **Dashboard de visualisation** (Python/Plotly)

## Exécution

```bash
# Compiler et exécuter le pipeline Spark
sbt clean compile run

# Lancer le dashboard
python dashboard.py

# Pour tout faire d'un coup

./run_project.sh
```
