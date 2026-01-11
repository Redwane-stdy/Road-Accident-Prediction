# Mon interprétation des résultats

## Matrice de Confusion

### Vue d'ensemble

```
                    PRÉDICTION
                    Non-Grave    Grave
         
         Non-Grave    48,479      2,459     Total: 50,938
RÉALITÉ              (TN)        (FP)       (71.9%)
         
         Grave        13,772      6,150     Total: 19,922
                      (FN)        (TP)       (28.1%)
                      
         Total        62,251      8,609      70,860
```

---

## 🔍 Analyse Détaillée

### 1. (True Negative) = 48,479 ✅

**Ce que ça signifie** :
- Accidents **réellement non-graves** ET **prédits non-graves**

**Proportion** :
- 48,479 / 50,938 = **95.2%** des accidents non-graves bien identifiés
- 48,479 / 70,860 = **68.4%** du total des prédictions

**Interprétation** :
**Excellent** - Le modèle est très bon pour reconnaître les accidents non-graves

---

### 2. FP (False Positive) = 2,459 ⚠️

**Ce que ça signifie** :
- Accidents **réellement non-graves** mais **prédits graves**
- Fausse alarme - le modèle se trompe en étant trop prudent

**Proportion** :
- 2,459 / 50,938 = **4.8%** des accidents non-graves mal classés
- 2,459 / 8,609 = **28.5%** des prédictions "grave" sont fausses

**Conséquences** :
- Mobilisation inutile de secours
- Coûts opérationnels
- Perte de crédibilité du système

**Interprétation** :
**Acceptable** - Peu de fausses alarmes (4.8%)

**Pour la sécurité routière** :
C'est le "moindre mal" - Mieux vaut mobiliser les secours pour rien que de ne pas les envoyer
Et le modèle le fait très peu, ce qui est acceptable. Mais il faudrait travailler pour
la baisser afin de ne pas manquer de ressources pour les accidents réellement graves.
---

### 3. FN (False Negative) = 13,772 ❌

**Ce que ça signifie** :
- Accidents **réellement graves** mais **prédits non-graves**
- **PROBLÈME MAJEUR** - Le modèle rate des accidents dangereux

**Conséquences** :
- Secours non alertés ou en retard
- Risque vital pour les victimes
- Sous-estimation du danger


**Pourquoi c'est grave ?** :
- Dans le contexte de la sécurité, les FN sont plus dangereux que les FP
- Un accident grave non détecté peut coûter des vies
- C'est le principal défaut du modèle

---

### 4. TP (True Positive) = 6,150 ✅

**Ce que ça signifie** :
- Accidents **réellement graves** ET **prédits graves**
- Interprétation : C'est BIEN - le modèle détecte correctement un accident dangereux
---

## Métriques Calculées

### Precision (Classe Grave)

```
Precision = TP / (TP + FP)
Precision = 6,150 / (6,150 + 2,459) = 71.5%
```

**Question** : Quand le modèle dit "grave", a-t-il raison ?
**Réponse** : Oui, dans 71.5% des cas

**C'est bien ou pas ?**
✅ **Plutôt bien** - 7 alertes sur 10 sont justifiées
- Évite trop de fausses alarmes
- Les secours sont mobilisés à bon escient dans la majorité des cas

---

### Recall (Classe Grave)

```
Recall = TP / (TP + FN)
Recall = 6,150 / (6,150 + 13,772) = 30.9%
```

**Question** : Parmi tous les accidents graves, quel % est détecté ?
**Réponse** : Seulement 30.9%

**C'est bien ou pas ?**
❌ **Très mauvais** - Le modèle rate 69% des accidents graves !
- Inacceptable pour un système de sécurité
- Beaucoup trop de victimes non secourues à temps

---

### F1-Score (Classe Grave)

```
F1 = 2 × (Precision × Recall) / (Precision + Recall)
F1 = 2 × (0.715 × 0.309) / (0.715 + 0.309) = 43.1%
```

**Question** : Quel est l'équilibre global ?
**Réponse** : 43.1% - Score moyen

**Interprétation** :
Le déséquilibre entre Precision (71.5%) et Recall (30.9%) est trop important
- Modèle trop conservateur
- Privilégie la fiabilité au détriment de la couverture

---

### Accuracy Globale

```
Accuracy = (TP + TN) / Total
Accuracy = (6,150 + 48,479) / 70,860 = 77.1%
```

**Question** : Quelle proportion de prédictions est correcte ?
**Réponse** : 77.1%

**Interprétation** :
**Bonne accuracy globale**
Mais trompeuse ! Le modèle réussit surtout grâce aux TN (68.4% du total)
En prédisant tout le temps non-grave il peut atteindre les 70% de précisions.

---

## Diagnostic : Pourquoi ce Déséquilibre ?

### 1. Classes déséquilibrées

```
Non-Graves : 50,938 (71.9%)
Graves     : 19,922 (28.1%)
```

**Conséquence** :
Le modèle apprend qu'il est "rentable" de prédire "non-grave" :
- Prédire toujours "non-grave" → 72% d'accuracy !
- Le modèle optimise l'accuracy, pas le recall

---

### 2. Fonction de coût uniforme

Le modèle traite toutes les erreurs de la même façon :
- FP (fausse alarme) = FN (accident grave raté) = même pénalité

**Or dans la réalité** :
- FP : Coût financier (secours mobilisés)
- FN : Coût humain (vies en danger)

**Solution** : Pondération des classes

--> Peut être pour continuer le projet.


### Faiblesses
- **Recall critique (31%)** - Le vrai problème
- Trop d'accidents graves ratés (69%)

### Prochaine étape
- Implémenter la pondération des classes
- Tester et comparer les performances