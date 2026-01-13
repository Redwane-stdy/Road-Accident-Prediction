#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard d'Analyse des Accidents de la Route 2023
Projet CSC5003 - Big Data & Machine Learning

ENTIÈREMENT DYNAMIQUE - Toutes les statistiques calculées depuis les données Spark

Usage:
    python dashboard.py

Génère des visualisations HTML interactives dans data/visualizations/
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import glob
from pathlib import Path

# ============================================================================
# CONFIGURATION ET CHARGEMENT DES DONNÉES
# ============================================================================

print("=" * 80)
print("CHARGEMENT DES DONNÉES DU PIPELINE SPARK")
print("=" * 80)

def load_spark_csv(pattern):
    """Charge un CSV généré par Spark (format part-00000-*.csv)"""
    files = glob.glob(pattern)
    if not files:
        return None
    print(f"✓ Chargement: {pattern}")
    return pd.read_csv(files[0], sep=';')

# Charger les données générées par le pipeline
try:
    # Métriques des modèles
    metrics = load_spark_csv("data/metrics.csv/part-*.csv")
    
    # Prédictions (on prend RandomForest comme exemple car c'est le modèle qui a les meilleurs résultats)
    predictions_rf = load_spark_csv("data/predictions/randomforest_predictions.csv/part-*.csv")
    
    # Données préparées pour visualisation 
    viz_data = load_spark_csv("data/prepared_for_viz.csv/part-*.csv")
    
    if viz_data is None or metrics is None:
        raise Exception("Fichiers principaux non trouvés")
    
    print(f"\n✓ Données chargées avec succès!")
    print(f"  - Métriques: {len(metrics)} modèles")
    print(f"  - Prédictions: {len(predictions_rf) if predictions_rf is not None else 0} lignes")
    print(f"  - Données viz: {len(viz_data)} accidents")
    
except Exception as e:
    print(f"Erreur de chargement: {e}")
    print("Les fichiers générés par Spark doivent être présents.")
    exit(1)

# ============================================================================
# CALCUL DYNAMIQUE DES STATISTIQUES DE CORRÉLATION
# ============================================================================

print("\n" + "=" * 80)
print("CALCUL DES STATISTIQUES DE CORRÉLATION")
print("=" * 80)

def calculate_severity_stats(df, column, labels_dict=None):
    """
    Calcule le taux de gravité en fonction d'un paramètre
    
    Args:
        df: DataFrame
        column: nom de la colonne
        labels_dict: dictionnaire de mapping valeur -> label (optionnel)
    
    Returns:
        DataFrame avec colonnes: valeur, label, taux_gravite, total
    """
    stats = df.groupby(column).agg(
        total=('accident_grave', 'count'),
        graves=('accident_grave', 'sum')
    ).reset_index()
    
    stats['taux_gravite'] = (stats['graves'] / stats['total'] * 100).round(2)
    stats = stats.sort_values('taux_gravite', ascending=False)
    
    # Ajouter les labels si fournis
    if labels_dict:
        stats['label'] = stats[column].map(labels_dict)
    else:
        stats['label'] = stats[column].astype(str)
    
    print(f" Statistiques calculées pour '{column}'")
    return stats

# Dictionnaires de labels (basés sur la documentation BAAC)
labels_luminosite = {
    1: 'Plein jour',
    2: 'Aube/Crépuscule',
    3: 'Nuit sans éclairage',
    4: 'Nuit avec éclairage public non allumé',
    5: 'Nuit avec éclairage public allumé'
}

labels_atm = {
    1: 'Normale',
    2: 'Pluie légère',
    3: 'Pluie forte',
    4: 'Neige/Grêle',
    5: 'Brouillard/fumée',
    6: 'Vent fort/tempête',
    7: 'Temps éblouissant',
    8: 'Temps couvert',
    9: 'Autre'
}

labels_agg = {
    1: 'Hors agglomération',
    2: 'En agglomération'
}

labels_route = {
    1: 'Autoroute',
    2: 'Route nationale',
    3: 'Route départementale',
    4: 'Voie communale',
    5: 'Hors réseau public',
    6: 'Parc de stationnement',
    7: 'Routes métropolitaines',
    9: 'Autre'
}

labels_weekend = {
    0: 'Semaine',
    1: 'Weekend'
}

labels_has_pl = {
    0: 'Sans poids lourd',
    1: 'Avec poids lourd'
}

# Calculer toutes les corrélations
correlation_luminosite = calculate_severity_stats(viz_data, 'lum', labels_luminosite)
correlation_atm = calculate_severity_stats(viz_data, 'atm', labels_atm)
correlation_horaire = calculate_severity_stats(viz_data, 'tranche_horaire')
correlation_agg = calculate_severity_stats(viz_data, 'agg', labels_agg)
correlation_route = calculate_severity_stats(viz_data, 'catr', labels_route)
correlation_vehicules = calculate_severity_stats(viz_data, 'nb_vehicules')
correlation_pl = calculate_severity_stats(viz_data, 'has_pl', labels_has_pl)
correlation_weekend = calculate_severity_stats(viz_data, 'weekend', labels_weekend)

print(f"\n Toutes les corrélations calculées dynamiquement")

# ============================================================================
# VISUALISATIONS
# ============================================================================

def create_model_comparison():
    """Comparaison des performances des modèles ML"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Métriques de Performance', 'Radar Chart de Performance'),
        specs=[[{"type": "bar"}, {"type": "polar"}]]
    )
    
    # Graphique 1: Barres groupées
    for metric in ['accuracy', 'precision', 'recall', 'f1']:
        fig.add_trace(
            go.Bar(
                name=metric.capitalize(),
                x=metrics['model'],
                y=metrics[metric],
                text=metrics[metric].round(4),
                textposition='auto',
            ),
            row=1, col=1
        )
    
    # Graphique 2: Radar chart
    categories = ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'AUC']
    colors = ['#27ae60', '#3498db', '#e74c3c']
    
    for idx, (_, row) in enumerate(metrics.iterrows()):
        fig.add_trace(
            go.Scatterpolar(
                r=[row['accuracy'], row['precision'], row['recall'], row['f1'], row['auc']],
                theta=categories,
                fill='toself',
                name=row['model'],
                line=dict(color=colors[idx % len(colors)], width=2)
            ),
            row=1, col=2
        )
    
    fig.update_layout(
        title_text="<b>Comparaison des Modèles de Prédiction</b>",
        title_font_size=20,
        height=500,
        showlegend=True,
        barmode='group',
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )
        )
    )
    
    return fig

def create_severity_factors():
    """Facteurs de risque de gravité - Vue d'ensemble"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Impact de la Luminosité',
            'Impact des Conditions Météo (Top 5)',
            'Impact du Lieu (Agglomération)',
            'Impact du Type de Route (Top 5)'
        ),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Luminosité
    fig.add_trace(
        go.Bar(
            x=correlation_luminosite['label'],
            y=correlation_luminosite['taux_gravite'],
            marker_color=['#e74c3c' if x > 30 else '#3498db' 
                         for x in correlation_luminosite['taux_gravite']],
            text=[f"{x:.1f}%" for x in correlation_luminosite['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=correlation_luminosite['total']
        ),
        row=1, col=1
    )
    
    # Conditions météo (top 5)
    top_meteo = correlation_atm.head(5)
    fig.add_trace(
        go.Bar(
            x=top_meteo['label'],
            y=top_meteo['taux_gravite'],
            marker_color=['#e74c3c' if x > 35 else '#f39c12' 
                         for x in top_meteo['taux_gravite']],
            text=[f"{x:.1f}%" for x in top_meteo['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=top_meteo['total']
        ),
        row=1, col=2
    )
    
    # Agglomération
    fig.add_trace(
        go.Bar(
            x=correlation_agg['label'],
            y=correlation_agg['taux_gravite'],
            marker_color=['#e74c3c', '#27ae60'],
            text=[f"{x:.1f}%" for x in correlation_agg['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=correlation_agg['total']
        ),
        row=2, col=1
    )
    
    # Type de route (top 5)
    top_routes = correlation_route.head(5)
    fig.add_trace(
        go.Bar(
            x=top_routes['label'],
            y=top_routes['taux_gravite'],
            marker_color=['#e74c3c' if x > 35 else '#3498db' 
                         for x in top_routes['taux_gravite']],
            text=[f"{x:.1f}%" for x in top_routes['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=top_routes['total']
        ),
        row=2, col=2
    )
    
    fig.update_xaxes(title_text="", row=1, col=1)
    fig.update_xaxes(title_text="", row=1, col=2, tickangle=-45)
    fig.update_xaxes(title_text="", row=2, col=1)
    fig.update_xaxes(title_text="", row=2, col=2, tickangle=-45)
    
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=1)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=2)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=2, col=1)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=2, col=2)
    
    fig.update_layout(
        title_text="<b>Facteurs de Risque de Gravité des Accidents</b>",
        title_font_size=20,
        height=800,
        showlegend=False
    )
    
    return fig

def create_temporal_analysis():
    """Analyse temporelle de la gravité"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Gravité par Tranche Horaire', 'Weekend vs Semaine'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Tranche horaire - trier par ordre logique
    order = ['nuit', 'matin_trafic', 'matinee', 'midi', 'apres_midi', 'soir_trafic', 'soiree', 'inconnu']
    horaire_sorted = correlation_horaire.set_index('tranche_horaire').reindex(order).reset_index()
    
    colors = ['#e74c3c' if x > 30 else '#f39c12' if x > 27 else '#27ae60' 
              for x in horaire_sorted['taux_gravite']]
    
    fig.add_trace(
        go.Bar(
            x=horaire_sorted['tranche_horaire'],
            y=horaire_sorted['taux_gravite'],
            marker_color=colors,
            text=[f"{x:.1f}%" for x in horaire_sorted['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=horaire_sorted['total']
        ),
        row=1, col=1
    )
    
    # Weekend
    fig.add_trace(
        go.Bar(
            x=correlation_weekend['label'],
            y=correlation_weekend['taux_gravite'],
            marker_color=['#e74c3c', '#3498db'],
            text=[f"{x:.1f}%" for x in correlation_weekend['taux_gravite']],
            textposition='outside',
            width=[0.5, 0.5],
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=correlation_weekend['total']
        ),
        row=1, col=2
    )
    
    fig.update_xaxes(title_text="Tranche horaire", row=1, col=1, tickangle=-45)
    fig.update_xaxes(title_text="", row=1, col=2)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=1)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=2)
    
    fig.update_layout(
        title_text="<b>Analyse Temporelle de la Gravité</b>",
        title_font_size=20,
        height=500
    )
    
    return fig

def create_vehicle_analysis():
    """Analyse de l'impact des véhicules"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Impact du Nombre de Véhicules', 'Impact des Poids Lourds'),
        specs=[[{"type": "scatter"}, {"type": "bar"}]]
    )
    
    # Nombre de véhicules (courbe) - limiter à 5 véhicules max pour la lisibilité
    vehicules_limited = correlation_vehicules[correlation_vehicules['nb_vehicules'] <= 5]
    
    fig.add_trace(
        go.Scatter(
            x=vehicules_limited['nb_vehicules'],
            y=vehicules_limited['taux_gravite'],
            mode='lines+markers',
            line=dict(color='#e74c3c', width=3),
            marker=dict(size=12, color='#c0392b', 
                       line=dict(color='white', width=2)),
            text=[f"{x:.1f}%" for x in vehicules_limited['taux_gravite']],
            textposition='top center',
            showlegend=False,
            hovertemplate='<b>%{x} véhicules</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=vehicules_limited['total']
        ),
        row=1, col=1
    )
    
    # Poids lourd
    fig.add_trace(
        go.Bar(
            x=correlation_pl['label'],
            y=correlation_pl['taux_gravite'],
            marker_color=['#e74c3c', '#3498db'],
            text=[f"{x:.1f}%" for x in correlation_pl['taux_gravite']],
            textposition='outside',
            showlegend=False,
            hovertemplate='<b>%{x}</b><br>Gravité: %{y:.2f}%<br>Accidents: %{customdata}<extra></extra>',
            customdata=correlation_pl['total']
        ),
        row=1, col=2
    )
    
    fig.update_xaxes(title_text="Nombre de véhicules impliqués", row=1, col=1)
    fig.update_xaxes(title_text="", row=1, col=2)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=1)
    fig.update_yaxes(title_text="Taux de gravité (%)", row=1, col=2)
    
    fig.update_layout(
        title_text="<b>Impact des Véhicules sur la Gravité</b>",
        title_font_size=20,
        height=500
    )
    
    return fig

def create_risk_heatmap():
    """Carte de chaleur des combinaisons de risques"""
    # Créer une matrice de risque à partir des données réelles
    risk_factors = []
    
    # Top facteurs de chaque catégorie
    risk_factors.append({
        'Facteur': f"Nuit ({correlation_horaire[correlation_horaire['tranche_horaire']=='nuit']['label'].iloc[0]})",
        'Taux': correlation_horaire[correlation_horaire['tranche_horaire']=='nuit']['taux_gravite'].iloc[0],
        'Catégorie': 'Temporel'
    })
    
    risk_factors.append({
        'Facteur': correlation_luminosite.iloc[0]['label'],
        'Taux': correlation_luminosite.iloc[0]['taux_gravite'],
        'Catégorie': 'Luminosité'
    })
    
    risk_factors.append({
        'Facteur': correlation_agg.iloc[0]['label'],
        'Taux': correlation_agg.iloc[0]['taux_gravite'],
        'Catégorie': 'Lieu'
    })
    
    risk_factors.append({
        'Facteur': correlation_pl[correlation_pl['has_pl']==1]['label'].iloc[0],
        'Taux': correlation_pl[correlation_pl['has_pl']==1]['taux_gravite'].iloc[0],
        'Catégorie': 'Véhicule'
    })
    
    risk_factors.append({
        'Facteur': correlation_weekend[correlation_weekend['weekend']==1]['label'].iloc[0],
        'Taux': correlation_weekend[correlation_weekend['weekend']==1]['taux_gravite'].iloc[0],
        'Catégorie': 'Temporel'
    })
    
    risk_factors.append({
        'Facteur': f"1 véhicule",
        'Taux': correlation_vehicules[correlation_vehicules['nb_vehicules']==1]['taux_gravite'].iloc[0],
        'Catégorie': 'Véhicule'
    })
    
    # Top 2 météo
    for i in range(min(2, len(correlation_atm))):
        risk_factors.append({
            'Facteur': correlation_atm.iloc[i]['label'],
            'Taux': correlation_atm.iloc[i]['taux_gravite'],
            'Catégorie': 'Météo'
        })
    
    risk_matrix = pd.DataFrame(risk_factors).sort_values('Taux', ascending=True)
    
    fig = go.Figure()
    
    colors = ['#27ae60' if x < 30 else '#f39c12' if x < 38 else '#e74c3c' 
              for x in risk_matrix['Taux']]
    
    fig.add_trace(go.Bar(
        y=risk_matrix['Facteur'],
        x=risk_matrix['Taux'],
        orientation='h',
        marker_color=colors,
        text=[f"{x:.1f}%" for x in risk_matrix['Taux']],
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>Gravité: %{x:.2f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title_text="<b>Classement des Facteurs de Risque</b>",
        title_font_size=20,
        xaxis_title="Taux de gravité (%)",
        yaxis_title="",
        height=600,
        showlegend=False
    )
    
    # Calculer dynamiquement la moyenne
    taux_moyen = viz_data['accident_grave'].mean() * 100
    
    fig.add_vline(x=taux_moyen, line_dash="dash", line_color="gray", 
                  annotation_text=f"Moyenne globale ({taux_moyen:.1f}%)", 
                  annotation_position="top")
    fig.add_vline(x=38, line_dash="dash", line_color="red", 
                  annotation_text="Risque élevé", annotation_position="top")
    
    return fig

def create_summary_dashboard():
    """Dashboard de synthèse avec KPIs"""
    # Calculer dynamiquement les statistiques globales
    total_accidents = len(viz_data)
    accidents_graves = viz_data['accident_grave'].sum()
    taux_global = (accidents_graves / total_accidents) * 100
    
    # Top 3 facteurs (dynamique)
    top_factors = []
    top_factors.append((correlation_atm.iloc[0]['label'], correlation_atm.iloc[0]['taux_gravite']))
    top_factors.append((correlation_atm.iloc[1]['label'], correlation_atm.iloc[1]['taux_gravite']))
    top_factors.append((correlation_luminosite.iloc[0]['label'], correlation_luminosite.iloc[0]['taux_gravite']))
    
    fig = go.Figure()
    
    # KPIs en forme de cartes
    fig.add_trace(go.Indicator(
        mode="number+delta",
        value=taux_global,
        title={"text": "Taux de Gravité Global"},
        #delta={'reference': 25, 'suffix': ' pts'},
        domain={'x': [0, 0.25], 'y': [0.7, 1]},
        number={'suffix': '%', 'valueformat': '.1f'}
    ))
    
    fig.add_trace(go.Indicator(
        mode="number",
        value=metrics['accuracy'].max() * 100,
        title={"text": f"Meilleure Précision ({metrics.loc[metrics['accuracy'].idxmax(), 'model']})"},
        domain={'x': [0.25, 0.5], 'y': [0.7, 1]},
        number={'suffix': '%', 'valueformat': '.2f'}
    ))
    
    fig.add_trace(go.Indicator(
        mode="number",
        value=correlation_agg.iloc[0]['taux_gravite'],
        title={"text": f"Risque {correlation_agg.iloc[0]['label']}"},
        domain={'x': [0.5, 0.75], 'y': [0.7, 1]},
        number={'suffix': '%', 'valueformat': '.1f'}
    ))
    
    fig.add_trace(go.Indicator(
        mode="number",
        value=correlation_atm.iloc[0]['taux_gravite'],
        title={"text": f"Risque Max ({correlation_atm.iloc[0]['label']})"},
        domain={'x': [0.75, 1], 'y': [0.7, 1]},
        number={'suffix': '%', 'valueformat': '.1f'}
    ))
    
    # Top 3 facteurs
    fig.add_trace(go.Bar(
        x=[f[0] for f in top_factors],
        y=[f[1] for f in top_factors],
        marker_color=['#8e44ad', '#c0392b', '#e67e22'],
        text=[f'{f[1]:.1f}%' for f in top_factors],
        textposition='outside',
        name='Top 3 Facteurs'
    ))
    
    fig.update_layout(
        title_text=f"<b>Dashboard de Synthèse - Accidents Routiers 2023 ({total_accidents:,} accidents analysés)</b>",
        title_font_size=22,
        height=700,
        showlegend=False,
        xaxis=dict(domain=[0, 1], anchor='y', title='Facteur de risque'),
        yaxis=dict(domain=[0, 0.6], anchor='x', title='Taux de gravité (%)', 
                  range=[0, max([f[1] for f in top_factors]) * 1.2])
    )
    
    return fig

# ============================================================================
# GÉNÉRATION ET SAUVEGARDE DES VISUALISATIONS
# ============================================================================

print("\n" + "=" * 80)
print("GÉNÉRATION DES VISUALISATIONS")
print("=" * 80)

output_dir = Path("data/visualizations")
output_dir.mkdir(parents=True, exist_ok=True)


#"02_comparaison_modeles.html": create_model_comparison(),
# Les modèles predisent plus souent non grave pour améliorer leur précision.
# L'étude des données ne permet pas d'extraire des tendances pour savoir si les accidents
# sont plus ou moins graves. 
# Voir les détails dans modelPasUtile.md
visualizations = {
    "01_synthese_dashboard.html": create_summary_dashboard(),
    "02_comparaison_modeles.html": create_model_comparison(),
    "03_facteurs_risque.html": create_severity_factors(),
    "04_analyse_temporelle.html": create_temporal_analysis(),
    "05_impact_vehicules.html": create_vehicle_analysis(),
    "06_classement_risques.html": create_risk_heatmap()
}

for filename, fig in visualizations.items():
    filepath = output_dir / filename
    fig.write_html(str(filepath))
    print(f" {filename}")

print("\n" + "=" * 80)
print("VISUALISATIONS GÉNÉRÉES AVEC SUCCÈS!")
print("=" * 80)
print(f"\n Fichiers disponibles dans: {output_dir}/")
print("\nOuvrez les fichiers HTML dans votre navigateur pour explorer les résultats.")
print("\n Pour tout voir utiliser la commande : open data/visualizations/*.html ")

# Afficher les insights clés calculés dynamiquement
print("\n Insights clés (calculés dynamiquement):")
print(f"  • {metrics.loc[metrics['accuracy'].idxmax(), 'model']} est le meilleur modèle ({metrics['accuracy'].max()*100:.2f}% accuracy)")
print(f"  • {correlation_atm.iloc[0]['label']}: risque le plus élevé ({correlation_atm.iloc[0]['taux_gravite']:.2f}%)")
print(f"  • {correlation_agg.iloc[0]['label']}: {correlation_agg.iloc[0]['taux_gravite']:.2f}% vs {correlation_agg.iloc[1]['taux_gravite']:.2f}% {correlation_agg.iloc[1]['label']}")
print(f"  • {correlation_horaire.iloc[0]['label']}: {correlation_horaire.iloc[0]['taux_gravite']:.2f}% de gravité")
print(f"  • {correlation_vehicules.iloc[0]['nb_vehicules']:.0f} véhicule = accident grave dans {correlation_vehicules.iloc[0]['taux_gravite']:.1f}% des cas")
print("\n" + "=" * 80)