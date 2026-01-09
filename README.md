# Projet CSC5003 - Analyse et Prédiction des Accidents de la Route

## Objectif Principal
Prédire la gravité potentielle d'un accident de la route en fonction de ses premières caractéristiques (lieu, météo, heure) pour mieux allouer les ressources d'urgence.

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
└── data/                        # Données CSV
```

## Configuration Environnement

### Java
```bash
# Lister les versions disponibles
/usr/libexec/java_home -V

# Définir Java 8 (requis pour Spark)
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
```






### FIN ---------------------------------------------------------------------------------------


# Lire la description de la base de données
/Projet/description-des-bases-de-donnees-annuelles.pdf

Prompt:
Je vais de te donner un fichier qui décrit la base de donnée des accidents de la route.
Je veux me focaliser sur les données de 2023.
On aura 4 fichiers csv, usagers, véhicules, lieux et caractéristiques et 1 fichier 2023.csv
Mon but est de répondre à la question suivante: comment 
peut-on prédire la gravité potentielle d'un accident de la route en fonction de ses premières caractéristiques (lieu, météo, heure) pour mieux allouer les ressources d'urgence ? 

On pourra aussi répondre à plusieurs questions intermédiaires:
Quelles sont les conditions qui sont le plus souvent corrélées aux accidents graves (blessés graves ou tués) ?
Existe-t-il des zones géographiques spécifiques où la proportion d'accidents graves est significativement plus élevée ?
Le type de véhicule impliqué est-il un facteur prédictif de la gravité de l'accident ?

Je dois faire le projet avec Spark et Scala. Je vais te donner l'enoncé en pdf.
Je veux suivre les étapes suivantes nettoyages des données. Affichage des données. Création de plusieurs modèles.
Prédiction des accidents. Et Plot de tous les résultats.

Je peux utiliser python pour l'affichage des données et des résultats. Mais pour le reste je dois le faire en Scala/Spark.


Comment mettre la bonne version de java. Java 21 n'est pas supporté par spark donc il faut le faire avec une autre version.
Lister toutes les versions : /usr/libexec/java_home -V        
export JAVA_HOME=<ta-version>
export JAVA_HOME=/Users/red1/Library/Java/JavaVirtualMachines/corretto-1.8.0_402/Contents/Home

Environnement python
python3 -m venv venv
source venv/bin/activate
pip install pandas plotly dash scikit-learn numpy matplotlib seaborn


# Projet CSC5003 : Prédiction de la Gravité des Accidents de la Route

## Objectif
Prédire la gravité potentielle d'un accident de la route en fonction de ses premières caractéristiques (lieu, météo, heure) pour mieux allouer les ressources d'urgence.

## Questions de Recherche
1. Quelles sont les conditions les plus souvent corrélées aux accidents graves ?
2. Existe-t-il des zones géographiques spécifiques où la proportion d'accidents graves est significativement plus élevée ?
3. Le type de véhicule impliqué est-il un facteur prédictif de la gravité de l'accident ?

## Structure du Projet

### Données
- `data/caracteristiques-2023.csv` : Caractéristiques des accidents
- `data/lieux-2023.csv` : Informations sur les lieux
- `data/vehicules-2023.csv` : Informations sur les véhicules
- `data/usagers-2023.csv` : Informations sur les usagers

### Code Scala/Spark
- `Main.scala` : Point d'entrée principal
- `DataCleaner.scala` : Nettoyage et préparation des données
- `DataExplorer.scala` : Exploration et statistiques
- `ModelBuilder.scala` : Construction des modèles ML
- `Predictor.scala` : Prédictions et évaluation

### Dashboard Python
- `dashboard.py` : Visualisation interactive des résultats

## Prérequis

### Java
```bash
export JAVA_HOME=/Users/red1/Library/Java/JavaVirtualMachines/corretto-1.8.0_402/Contents/Home