#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rapport rapide des résultats - Affichage terminal
Usage: python report.py
"""

import pandas as pd
import glob
from pathlib import Path

# Couleurs ANSI pour le terminal
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_header(text):
    """Affiche un en-tête coloré"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}\n")

def print_section(text):
    """Affiche un titre de section"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}▶ {text}{Colors.END}")
    print(f"{Colors.BLUE}{'-' * 80}{Colors.END}")

def print_metric(label, value, unit="", color=Colors.GREEN):
    """Affiche une métrique formatée"""
    print(f"  {Colors.BOLD}{label:<40}{color}{value:>15}{unit}{Colors.END}")

def print_success(text):
    """Affiche un message de succès"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_warning(text):
    """Affiche un avertissement"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_error(text):
    """Affiche une erreur"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def load_csv(pattern):
    """Charge un CSV Spark avec gestion d'erreur"""
    try:
        files = glob.glob(pattern)
        if not files:
            return None
        return pd.read_csv(files[0], sep=';')
    except Exception as e:
        return None

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================

print_header("RAPPORT D'ANALYSE - ACCIDENTS ROUTIERS 2023")

print_section("1. Chargement des résultats")

metrics = load_csv("data/metrics.csv/part-*.csv")
predictions_rf = load_csv("data/predictions/randomforest_predictions.csv/part-*.csv")
viz_data = load_csv("data/prepared_for_viz.csv/part-*.csv")

if metrics is not None:
    print_success(f"Métriques chargées ({len(metrics)} modèles)")
else:
    print_error("Métriques non trouvées")

if predictions_rf is not None:
    print_success(f"Prédictions chargées ({len(predictions_rf)} lignes)")
else:
    print_warning("Prédictions non trouvées")

if viz_data is not None:
    print_success(f"Données de visualisation chargées ({len(viz_data)} accidents)")
else:
    print_warning("Données de visualisation non trouvées")

# ============================================================================
# MÉTRIQUES DES MODÈLES
# ============================================================================

if metrics is not None:
    print_section("2. Performance des Modèles ML")
    
    print(f"\n{Colors.BOLD}{'Modèle':<25} {'Accuracy':>10} {'Precision':>10} {'Recall':>10} {'F1-Score':>10} {'AUC':>10}{Colors.END}")
    print(f"{Colors.BLUE}{'-' * 80}{Colors.END}")
    
    for _, row in metrics.sort_values('accuracy', ascending=False).iterrows():
        # Couleur basée sur l'accuracy
        if row['accuracy'] > 0.76:
            color = Colors.GREEN
        elif row['accuracy'] > 0.74:
            color = Colors.YELLOW
        else:
            color = Colors.RED
        
        print(f"{color}{row['model']:<25} "
              f"{row['accuracy']:>10.4f} "
              f"{row['precision']:>10.4f} "
              f"{row['recall']:>10.4f} "
              f"{row['f1']:>10.4f} "
              f"{row['auc']:>10.4f}{Colors.END}")
    
    # Meilleur modèle
    best_model = metrics.loc[metrics['accuracy'].idxmax()]
    print(f"\n{Colors.BOLD}{Colors.GREEN}🏆 Meilleur modèle: {best_model['model']} "
          f"(Accuracy: {best_model['accuracy']:.2%}){Colors.END}")

# ============================================================================
# STATISTIQUES DES PRÉDICTIONS
# ============================================================================

if predictions_rf is not None:
    print_section("3. Statistiques des Prédictions (Random Forest)")
    
    # Calculer les statistiques
    total = len(predictions_rf)
    correct = len(predictions_rf[predictions_rf['label'] == predictions_rf['prediction']])
    incorrect = total - correct
    
    graves_reels = len(predictions_rf[predictions_rf['label'] == 1])
    graves_predits = len(predictions_rf[predictions_rf['prediction'] == 1])
    
    # Matrice de confusion
    tp = len(predictions_rf[(predictions_rf['label'] == 1) & (predictions_rf['prediction'] == 1)])
    tn = len(predictions_rf[(predictions_rf['label'] == 0) & (predictions_rf['prediction'] == 0)])
    fp = len(predictions_rf[(predictions_rf['label'] == 0) & (predictions_rf['prediction'] == 1)])
    fn = len(predictions_rf[(predictions_rf['label'] == 1) & (predictions_rf['prediction'] == 0)])
    
    print_metric("Total de prédictions", f"{total:,}")
    print_metric("Prédictions correctes", f"{correct:,}", f" ({correct/total:.1%})", Colors.GREEN)
    print_metric("Prédictions incorrectes", f"{incorrect:,}", f" ({incorrect/total:.1%})", Colors.RED)
    
    print(f"\n  {Colors.BOLD}Matrice de confusion:{Colors.END}")
    print(f"    True Positives (TP):  {tp:>6,} {Colors.GREEN}✓{Colors.END}")
    print(f"    True Negatives (TN):  {tn:>6,} {Colors.GREEN}✓{Colors.END}")
    print(f"    False Positives (FP): {fp:>6,} {Colors.YELLOW}⚠{Colors.END}")
    print(f"    False Negatives (FN): {fn:>6,} {Colors.RED}✗{Colors.END}")

# ============================================================================
# STATISTIQUES GLOBALES
# ============================================================================

if viz_data is not None:
    print_section("4. Statistiques Globales des Accidents")
    
    total_accidents = len(viz_data)
    accidents_graves = len(viz_data[viz_data['accident_grave'] == 1])
    taux_gravite = (accidents_graves / total_accidents) * 100
    
    print_metric("Total d'accidents analysés", f"{total_accidents:,}")
    print_metric("Accidents graves", f"{accidents_graves:,}", f" ({taux_gravite:.1f}%)", Colors.RED)
    print_metric("Accidents non graves", f"{total_accidents - accidents_graves:,}", 
                 f" ({100-taux_gravite:.1f}%)", Colors.GREEN)
    
    # Statistiques par département (top 5)
    if 'dep' in viz_data.columns:
        print(f"\n  {Colors.BOLD}Top 5 départements (nombre d'accidents):{Colors.END}")
        top_deps = viz_data['dep'].value_counts().head(5)
        for i, (dep, count) in enumerate(top_deps.items(), 1):
            print(f"    {i}. Département {dep}: {count:>6,} accidents")

# ============================================================================
# INSIGHTS CLÉS
# ============================================================================

print_section("5. Insights Clés")

insights = [
    ("🌫️ Brouillard/Fumée", "Risque x1.7", "47.89% de gravité (vs 28.37% normal)"),
    ("❄️ Neige", "Risque x1.6", "45.71% de gravité"),
    ("🌆 Crépuscule", "Risque x1.4", "40.64% de gravité"),
    ("🏘️ Hors agglomération", "Risque x1.8", "40.58% vs 22.05% en agglo"),
    ("🌙 Nuit", "Risque +24%", "33.11% vs 26.67% en journée"),
    ("🚛 Poids lourd présent", "Risque +30%", "36.11% vs 27.77% sans PL"),
    ("📅 Weekend", "Risque +21%", "32.27% vs 26.67% en semaine"),
    ("🚗 1 seul véhicule", "Risque x1.6", "38.10% vs 23.70% pour 2 véhicules"),
]

for emoji, titre, detail in insights:
    print(f"  {Colors.BOLD}{emoji} {titre:<30}{Colors.END} {Colors.YELLOW}{detail}{Colors.END}")

# ============================================================================
# RECOMMANDATIONS
# ============================================================================

print_section("6. Recommandations")

recommendations = [
    "Renforcer la prévention lors de conditions météo difficiles (brouillard, neige)",
    "Surveillance accrue hors agglomération (40.6% de gravité vs 22% en ville)",
    "Campagnes ciblées pour les périodes nocturnes et week-ends",
    "Attention particulière aux accidents impliquant des poids lourds",
    "Modèle Random Forest déployable pour prédiction en temps réel (76.5% accuracy)",
]

for i, rec in enumerate(recommendations, 1):
    print(f"  {Colors.GREEN}{i}.{Colors.END} {rec}")

# ============================================================================
# FICHIERS GÉNÉRÉS
# ============================================================================

print_section("7. Fichiers Disponibles")

files_to_check = [
    ("data/cleaned_data.csv/", "Données nettoyées"),
    ("data/prepared_for_viz.csv/", "Données pour visualisation"),
    ("data/metrics.csv/", "Métriques des modèles"),
    ("data/predictions/randomforest_predictions.csv/", "Prédictions Random Forest"),
    ("data/predictions/logisticregression_predictions.csv/", "Prédictions Régression Logistique"),
    ("data/predictions/decisiontree_predictions.csv/", "Prédictions Arbre de Décision"),
    ("data/visualizations/01_synthese_dashboard.html", "Dashboard de synthèse"),
    ("data/visualizations/02_comparaison_modeles.html", "Comparaison des modèles"),
]

for filepath, description in files_to_check:
    if Path(filepath).exists():
        print_success(f"{description:<50} {filepath}")
    else:
        print_warning(f"{description:<50} {filepath} (non trouvé)")

# ============================================================================
# FOOTER
# ============================================================================

print_header("FIN DU RAPPORT")

print(f"{Colors.BOLD}Pour explorer les visualisations interactives:{Colors.END}")
print(f"  {Colors.CYAN}open data/visualizations/*.html{Colors.END}")
print()
print(f"{Colors.BOLD}Pour consulter les prédictions détaillées:{Colors.END}")
print(f"  {Colors.CYAN}cat data/predictions/randomforest_predictions.csv/part-*.csv | head{Colors.END}")
print()