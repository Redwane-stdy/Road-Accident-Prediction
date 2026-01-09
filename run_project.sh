#!/bin/bash
# ============================================================================
# Script d'exécution du projet CSC5003 - Accidents Routiers
# ============================================================================

set -e  # Arrêt en cas d'erreur

echo "============================================================"
echo "  PROJET CSC5003 - ANALYSE ACCIDENTS ROUTIERS"
echo "============================================================"
echo ""

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonction pour afficher les étapes
step() {
    echo -e "${GREEN}>>> ÉTAPE $1${NC}"
}

error() {
    echo -e "${RED}❌ ERREUR: $1${NC}"
    exit 1
}

warning() {
    echo -e "${YELLOW}⚠️  ATTENTION: $1${NC}"
}

info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# ============================================================================
# VÉRIFICATIONS PRÉALABLES
# ============================================================================

step "1/7 - Vérification de Java"
if [ -z "$JAVA_HOME" ]; then
    warning "JAVA_HOME n'est pas défini"
    info "Versions Java disponibles:"
    /usr/libexec/java_home -V 2>&1 || true
    echo ""
    error "Définissez JAVA_HOME avec: export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8)"
fi

JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
info "Java version: $JAVA_VERSION"
info "JAVA_HOME: $JAVA_HOME"

if [[ ! "$JAVA_VERSION" =~ ^1\.8 ]]; then
    warning "Spark nécessite Java 8. Version actuelle: $JAVA_VERSION"
    error "Changez vers Java 8 avec: export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8)"
fi
success "Java 8 configuré correctement"

# ============================================================================
step "2/7 - Vérification de SBT"
if ! command -v sbt &> /dev/null; then
    error "SBT n'est pas installé. Installez-le avec: brew install sbt"
fi
SBT_VERSION=$(sbt --version 2>/dev/null | grep "sbt script version" | head -n1)
success "SBT installé: $SBT_VERSION"

# ============================================================================
step "3/7 - Configuration de l'environnement Python"

if [ ! -d "venv" ]; then
    info "Création de l'environnement virtuel Python..."
    python3 -m venv venv || error "Échec de la création de l'environnement virtuel"
    success "Environnement virtuel créé"
fi

info "Activation de l'environnement virtuel..."
source venv/bin/activate || error "Échec de l'activation de l'environnement virtuel"

info "Installation/mise à jour des dépendances Python..."
pip install --quiet --upgrade pip
pip install --quiet pandas plotly numpy scikit-learn matplotlib seaborn

success "Environnement Python configuré"

# ============================================================================
step "4/7 - Vérification des données"

if [ ! -d "data" ]; then
    error "Le dossier 'data' n'existe pas. Créez-le et placez-y les fichiers CSV."
fi

REQUIRED_FILES=(
    "caracteristiques-2023.csv"
    "lieux-2023.csv"
    "vehicules-2023.csv"
    "usagers-2023.csv"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "data/$file" ]; then
        error "Fichier manquant: data/$file"
    fi
done

success "Tous les fichiers de données sont présents"

# Afficher la taille des fichiers
info "Taille des fichiers:"
for file in "${REQUIRED_FILES[@]}"; do
    size=$(du -h "data/$file" | cut -f1)
    echo "  • $file: $size"
done

# ============================================================================
step "5/7 - Compilation et exécution du pipeline Spark"

info "⏳ Cette étape peut prendre 3-5 minutes..."
info "Le pipeline va:"
info "  1. Charger et nettoyer les données"
info "  2. Effectuer l'analyse exploratoire"
info "  3. Entraîner 3 modèles ML (RandomForest, LogisticRegression, DecisionTree)"
info "  4. Générer les prédictions et métriques"
echo ""

# Exécuter le pipeline Spark
if sbt "clean; compile; run"; then
    success "Pipeline Spark exécuté avec succès"
else
    error "Échec de l'exécution du pipeline Spark"
fi

# ============================================================================
step "6/7 - Vérification des fichiers générés"

EXPECTED_OUTPUTS=(
    "data/cleaned_data.csv"
    "data/prepared_for_viz.csv"
    "data/predictions/randomforest_predictions.csv"
    "data/predictions/logisticregression_predictions.csv"
    "data/predictions/decisiontree_predictions.csv"
    "data/metrics.csv"
)

missing=0
for output in "${EXPECTED_OUTPUTS[@]}"; do
    if [ -d "$output" ] && [ -n "$(ls -A "$output" 2>/dev/null)" ]; then
        success "$output"
    else
        warning "Non trouvé: $output"
        missing=$((missing + 1))
    fi
done

if [ $missing -gt 0 ]; then
    warning "$missing fichier(s) de sortie manquant(s)"
else
    success "Tous les fichiers générés avec succès"
fi

# ============================================================================
step "7/7 - Génération des visualisations"

info "Création des visualisations interactives..."
if python dashboard.py; then
    success "Visualisations générées"
else
    warning "Erreur lors de la génération des visualisations (non bloquant)"
fi

# ============================================================================
# RÉSUMÉ FINAL
# ============================================================================

echo ""
echo "============================================================"
echo "  ✨ EXÉCUTION TERMINÉE AVEC SUCCÈS! ✨"
echo "============================================================"
echo ""
echo "📊 FICHIERS GÉNÉRÉS:"
echo ""
echo "📁 Données traitées:"
echo "  ├── data/cleaned_data.csv/          (Données nettoyées)"
echo "  ├── data/prepared_for_viz.csv/      (Données pour visualisation)"
echo ""
echo "🤖 Modèles & Prédictions:"
echo "  ├── data/predictions/"
echo "  │   ├── randomforest_predictions.csv/"
echo "  │   ├── logisticregression_predictions.csv/"
echo "  │   └── decisiontree_predictions.csv/"
echo "  └── data/metrics.csv/               (Performance des modèles)"
echo ""
echo "📈 Visualisations:"
echo "  └── data/visualizations/"
echo "      ├── 01_synthese_dashboard.html"
echo "      ├── 02_comparaison_modeles.html"
echo "      ├── 03_facteurs_risque.html"
echo "      ├── 04_analyse_temporelle.html"
echo "      ├── 05_impact_vehicules.html"
echo "      └── 06_classement_risques.html"
echo ""
echo "🎯 PROCHAINES ÉTAPES:"
echo ""
echo "  1. Ouvrez les fichiers HTML pour explorer les résultats:"
echo "     open data/visualizations/01_synthese_dashboard.html"
echo ""
echo "  2. Consultez les métriques des modèles:"
echo "     cat data/metrics.csv/part-*.csv"
echo ""
echo "  3. Analysez les prédictions détaillées:"
echo "     cat data/predictions/randomforest_predictions.csv/part-*.csv | head"
echo ""
echo "============================================================"
echo ""

# Afficher les métriques si disponibles
if [ -d "data/metrics.csv" ] && [ -n "$(ls -A data/metrics.csv/*.csv 2>/dev/null)" ]; then
    echo "📊 RÉSULTATS DES MODÈLES:"
    echo ""
    cat data/metrics.csv/part-*.csv 2>/dev/null | head -n 5
    echo ""
fi

echo "🎉 Pipeline terminé! Consultez les visualisations dans votre navigateur."
echo ""