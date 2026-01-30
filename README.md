# Cloud Data Pipeline – GCP

## Objectif
Construire un pipeline data end-to-end sur Google Cloud Platform.

## Architecture
- Source : OpenWeather API
- Ingestion : Python
- Orchestration : Apache Airflow
- Stockage :
  - Data Lake : Google Cloud Storage
  - Data Warehouse : BigQuery
- Visualisation : Looker Studio

## Stack technique
- Python
- GCP (GCS, BigQuery)
- Apache Airflow
- SQL

## Résultats
- Données météo collectées automatiquement
- Pipeline orchestré quotidiennement
- Tables analytiques dans BigQuery
- Dashboard météo interactif