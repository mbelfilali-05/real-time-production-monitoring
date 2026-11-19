# 🏭 Système de Supervision en Temps Réel d'une Chaîne de Production

**Projet Big Data — Groupe Gr6 — Projet 3**

**Réalisé par :** Mohamed Belfilali Mimoun

Technologies : Apache Kafka · Apache Spark Structured Streaming · Delta Lake · Python · Streamlit · Docker

## 1. Présentation

Ce projet consiste à concevoir et implémenter un système de supervision quasi temps réel d'une chaîne de production industrielle en utilisant Apache Kafka, Apache Spark Structured Streaming et Delta Lake.

Le système simule les mesures de quatre machines industrielles, collecte les événements en continu, détecte les anomalies et calcule des indicateurs de performance affichés dans un dashboard interactif développé avec Streamlit.

## 2. Problématique

Dans une chaîne de production industrielle, les machines génèrent continuellement des données relatives à leur fonctionnement : température, vibrations, pression, cadence de production et défauts.

L'exploitation de ces données permet d'identifier rapidement les comportements anormaux et de suivre les performances de production.

**Comment concevoir un pipeline Big Data capable de collecter, traiter, stocker et visualiser ces données de manière continue ?**

## 3. Objectifs

- Simuler une chaîne de production industrielle composée de quatre machines.
- Collecter les mesures en continu avec Apache Kafka.
- Traiter les événements avec Spark Structured Streaming.
- Stocker les données dans une architecture Medallion Bronze, Silver et Gold avec Delta Lake.
- Détecter les anomalies de température et de vibration.
- Calculer des KPI par machine.
- Visualiser les résultats dans un dashboard Streamlit.
- Simplifier le déploiement avec Docker Compose et PowerShell.

## 4. Architecture du système

```text
                 SIMULATEUR PYTHON
                   4 machines
                        |
                        v
                  APACHE KAFKA
               production-events
                        |
                        v
              SPARK STRUCTURED STREAMING
                        |
                        v
                DELTA LAKE BRONZE
                  Données brutes
                        |
                        v
              SPARK STRUCTURED STREAMING
                   foreachBatch
                        |
                        v
                DELTA LAKE SILVER
          Validation et détection d'anomalies
                        |
                        v
                DELTA LAKE GOLD
                KPI par machine
                        |
                        v
                  STREAMLIT
             Dashboard de supervision
```

### Rôle des composants

| Technologie | Rôle |
|---|---|
| Python | Simulation des capteurs industriels |
| Apache Kafka | Transport et conservation des événements |
| Apache Spark | Traitement distribué et streaming |
| Delta Lake | Stockage transactionnel des données |
| Streamlit | Interface de supervision |
| Plotly | Visualisation interactive |
| Docker Compose | Orchestration locale des services |

## 5. Machines simulées

| Identifiant | Machine |
|---|---|
| M01 | Découpe |
| M02 | Assemblage |
| M03 | Soudage |
| M04 | Emballage |

Chaque machine génère une mesure environ une fois par seconde.

Les événements contiennent notamment :

- Identifiant de l'événement et de la machine
- Horodatage
- Température
- Vibration
- Pression
- Cadence de production
- Nombre de défauts
- État de fonctionnement

## 6. Architecture Medallion

### Bronze — Données brutes

La couche Bronze reçoit les événements depuis Kafka et conserve les messages JSON ainsi que leurs métadonnées techniques, notamment le topic, la partition, l'offset et l'horodatage d'ingestion.

### Silver — Données nettoyées

La couche Silver transforme les événements JSON en colonnes structurées.

Elle réalise la validation des mesures, la suppression des doublons par identifiant et la détection des anomalies.

**Règles de détection :**

- Température supérieure à 90 °C : `HIGH_TEMPERATURE`
- Vibration supérieure à 6 : `HIGH_VIBRATION`
- Dépassement des deux seuils : `TEMPERATURE_AND_VIBRATION`
- Aucun dépassement : `NORMAL`

### Gold — Indicateurs de performance

La couche Gold regroupe les données par machine et calcule :

- Nombre total d'événements
- Température moyenne
- Vibration moyenne
- Nombre total d'anomalies
- Taux d'anomalies
- Nombre de défauts
- Cadence moyenne de production
- Date de la dernière mesure

Les couches Silver et Gold sont mises à jour automatiquement par micro-lots Spark. Pour ce prototype, Gold est recalculée à partir de Silver à chaque micro-lot contenant de nouvelles données.

## 7. Installation et exécution

### Prérequis

- Windows 10 ou Windows 11
- Docker Desktop avec WSL2
- Python et environnement virtuel
- Dépendances Python du simulateur et du dashboard

### Installation des dépendances

Depuis la racine du projet :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r simulator/requirements.txt
.\.venv\Scripts\python.exe -m pip install -r dashboard/requirements.txt
```

### Démarrer la plateforme

```powershell
.\scripts\start-demo.ps1
```

Cette commande démarre Kafka, les workers Spark et le dashboard.

Le dashboard est accessible à l'adresse :

**http://localhost:8501**

### Démarrer le simulateur

Dans un deuxième terminal :

```powershell
.\scripts\start-producer.ps1
```

Le simulateur produit des mesures jusqu'à son arrêt avec `Ctrl+C`.

### Arrêter la plateforme

Après avoir arrêté le simulateur :

```powershell
.\scripts\stop-demo.ps1
```

Les données Delta sont conservées entre les exécutions.

## 8. Dashboard de supervision

Le dashboard présente :

- Le nombre total d'événements
- Le nombre d'anomalies détectées
- Le taux global d'anomalies
- Le nombre de machines supervisées
- Les anomalies par machine
- La température moyenne par machine
- Un tableau des KPI
- Les dernières anomalies détectées

L'interface relit périodiquement les tables Delta afin d'afficher les résultats actualisés.

## 9. Résultats expérimentaux

Lors d'un scénario de simulation, le système a produit les résultats suivants :

| Indicateur | Résultat |
|---|---:|
| Événements traités | 964 |
| Anomalies détectées | 95 |
| Taux global d'anomalies | 9,85 % |
| Machines supervisées | 4 |

Ces résultats proviennent de données synthétiques. Ils permettent de valider le fonctionnement du prototype, mais ne constituent pas une évaluation sur une chaîne de production réelle.

## 10. Limites

- Les mesures proviennent d'un simulateur et non de capteurs industriels réels.
- Les anomalies sont détectées par des seuils fixes, sans modèle de machine learning.
- Le traitement s'effectue par micro-lots, avec une latence variable.
- La couche Gold est recalculée entièrement à chaque micro-lot, une approche adaptée au faible volume de cette démonstration.
- Le déploiement est local et ne comprend pas de système d'authentification ou d'alertes externes.

## 11. Perspectives

Plusieurs améliorations sont envisageables :

- Ajouter des capteurs IoT réels.
- Introduire un modèle de maintenance prédictive.
- Mettre en place des alertes automatiques.
- Utiliser des agrégations incrémentales et des fenêtres temporelles.
- Déployer le système dans un environnement cloud.
- Ajouter des mécanismes de supervision et de reprise plus avancés.

## 12. Conclusion

Ce projet démontre comment Kafka, Spark Structured Streaming et Delta Lake peuvent être combinés pour construire un pipeline de supervision industrielle quasi temps réel.

L'architecture Medallion facilite la séparation entre les données brutes, les données validées et les indicateurs métier, tandis que Streamlit rend les résultats accessibles à travers une interface interactive.

Le prototype constitue ainsi une première approche pratique des architectures modernes de traitement de données en continu.