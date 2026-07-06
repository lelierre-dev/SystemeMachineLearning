# SystemeMachineLearning — End-to-End MLOps Pipeline

This repository contains practical assignments completed for a Machine Learning Systems course, progressively building an end-to-end MLOps pipeline for churn prediction.

The project covers containerization, data ingestion, validation, feature stores, model training, model registry, serving, observability, drift detection, automated retraining, and CI/CD.

## Reports

| TP  | Topic                                                                     | Report                                  |
| --- | ------------------------------------------------------------------------- | --------------------------------------- |
| TP1 | Docker, FastAPI and PostgreSQL with Docker Compose                        | [Open report](./reports/rapport_tp1.md) |
| TP2 | Data Ingestion with Prefect, PostgreSQL, Great Expectations and Snapshots | [Open report](./reports/rapport_tp2.md) |
| TP3 | Feature Store with Feast: Offline and Online Features                     | [Open report](./reports/rapport_tp3.md) |
| TP4 | Model Training, MLflow Tracking, Registry and Serving                     | [Open report](./reports/rapport_tp4.md) |
| TP5 | Observability with Prometheus/Grafana and Drift Detection with Evidently  | [Open report](./reports/rapport_tp5.md) |
| TP6 | CI/CD, Automated Retraining and MLflow Model Promotion                    | [Open report](./reports/rapport_tp6.md) |

## Repository Structure

```text
SystemeMachineLearning/
├── api/              # FastAPI serving API
├── data/             # Raw and processed datasets
├── db/init/          # PostgreSQL initialization scripts
├── mlartifacts/      # MLflow artifacts
├── reports/          # Markdown reports for each TP
├── services/         # Prefect, Feast, MLflow and pipeline services
├── tests/unit/       # Unit tests
└── docker-compose.yml
```

## Project Overview

The project simulates a production-oriented machine learning system around a churn prediction use case.

The pipeline includes:

* Data ingestion from monthly CSV snapshots
* PostgreSQL storage and historical snapshots
* Data validation with Great Expectations
* Feature management with Feast
* Model training and experiment tracking with MLflow
* Model registry and promotion workflow
* FastAPI model serving
* Monitoring with Prometheus and Grafana
* Drift detection with Evidently
* Automated retraining and promotion logic
* CI smoke tests with GitHub Actions

## Notes

Each report documents one step of the MLOps pipeline, from the initial Docker setup to a complete drift → retrain → promote → serve workflow.
