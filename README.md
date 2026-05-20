# Pollen Cloud Data Pipeline

A cloud-based end-to-end data engineering pipeline that ingests pollen forecast data from the Open-Meteo Air Quality API, stores raw JSON in Azure Blob Storage, loads it into Snowflake, and transforms it into analytics-ready marts using dbt. The workflow is orchestrated by Apache Airflow and deployed on Astronomer Cloud.

## Overview

This project demonstrates a modern ELT data pipeline for pollen forecast analytics.

The pipeline:

1. Extracts pollen forecast data from an external API
2. Stores raw JSON files in Azure Blob Storage
3. Loads raw files into Snowflake
4. Transforms raw data into staging and mart models using dbt
5. Runs dbt tests to validate data quality
6. Executes automatically through a scheduled Airflow DAG on Astronomer Cloud

The final mart table is designed for analytics and dashboarding use cases.

---

## Project Architecture

### Technology Stack

- **Workflow Orchestration**: Apache Airflow
- **Cloud Orchestration Platform**: Astronomer Cloud
- **Cloud Storage**: Azure Blob Storage
- **Data Warehouse**: Snowflake
- **Data Transformation**: dbt
- **Programming Language**: Python

---

## Pipeline Flow

```text
Open-Meteo Air Quality API
        ↓
Python ingestion script
        ↓
Azure Blob Storage
        ↓
Snowflake RAW layer
        ↓
dbt STAGING models
        ↓
dbt MART models
        ↓
Analytics-ready dataset
