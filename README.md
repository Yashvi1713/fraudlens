# fraudlens

FraudLens is a Databricks-focused fraud investigation platform built around a Lakehouse medallion pipeline.

This repository contains the production-style pipeline logic for:

- Bronze data ingestion
- Silver cleansing and standardization
- Gold data preparation for fraud analysis
- explainable fraud risk scoring
- Databricks auth and workspace configuration
- validation and testing for the pipeline

## Architecture

```text
Raw banking data
   ↓
Bronze layer
   ↓
Silver layer
   ↓
Feature engineering + risk scoring
   ↓
Gold dataset
   ↓
Fraud investigation workflow
```

## Repo structure

```text
fraudlens/
├── README.md
├── pyproject.toml
├── requirements.txt
├── .env.example
├── .gitignore
├── Makefile
├── config/
├── databricks/
├── src/
├── tests/
└──
```

## Databricks usage

This repo is designed to be pulled into a Databricks workspace and run as a repo-backed pipeline. The main orchestration entrypoint is:

- databricks/run_pipeline.py

The project includes domain-specific Bronze/Silver/Gold logic for:

- transactions
- customers
- accounts
- devices

## Environment configuration

Copy the example environment file and set the Databricks values:

```bash
cp .env.example .env
```

Required settings include:

- Databricks host
- warehouse or cluster info
- catalog name
- OAuth or PAT credentials

## Local validation

```bash
pytest -q
```

Optional local pipeline execution:

```bash
python databricks/run_pipeline.py --base-dir data
```

## Notes

- This repo is Databricks-first and repo-ready.
- Local execution is mainly for validation and development when cluster-backed compute is unavailable.
- The project uses synthetic banking data for learning and portfolio demonstration.

## License

This project is intended for learning and portfolio demonstration.
