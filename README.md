# Pollen Cloud Data Pipeline

A cloud-based end-to-end data engineering pipeline that ingests pollen forecast data from the Open-Meteo Air Quality API, stores raw JSON in Azure Blob Storage, loads it into Snowflake, and transforms it into analytics-ready marts using dbt. The workflow is orchestrated by Apache Airflow and deployed on Astronomer Cloud.

---

## Overview

This project demonstrates a modern **ELT data pipeline** for pollen forecast analytics.

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
```

---

## Data Pipeline

This project follows an **ELT pattern**.

### 1. Extract

Fetch pollen forecast data from the **Open-Meteo Air Quality API** for selected Swedish regions.

### 2. Raw Storage

Wrap the API response with ingestion metadata and upload raw JSON files to **Azure Blob Storage**.

### 3. Load

Read raw JSON files from Azure Blob Storage and load them into the Snowflake raw table:

```text
POLLEN_DB.RAW.RAW_POLLEN
```

### 4. Transform

Use dbt to build:

- **Staging models** for cleaning and flattening raw JSON data
- **Mart models** for analytics-ready pollen summaries

### 5. Validate

Run dbt tests to confirm model quality and required fields.

---

## Airflow DAG

The pipeline is orchestrated through:

```text
dags/pollen_pipeline_dag.py
```

### DAG Tasks

```text
ingest_pollen_api
        ↓
load_blob_to_snowflake
        ↓
dbt_run
        ↓
dbt_test
```

### Schedule

The DAG is configured to run daily:

```cron
0 6 * * *
```

This means the pipeline is scheduled once per day at **06:00 UTC**.

---

## Project Structure

```text
pollen-batch-ingestion/
├── dags/
│   └── pollen_pipeline_dag.py
│
├── include/
│   └── pollen_pipeline/
│       ├── ingest_pollen_api.py
│       ├── load_blob_to_snowflake.py
│       │
│       └── pollen_dbt/
│           ├── models/
│           │   ├── staging/
│           │   └── marts/
│           ├── profiles.yml
│           └── dbt_project.yml
│
├── Dockerfile
├── requirements.txt
├── packages.txt
├── airflow_settings.yaml
├── .dockerignore
├── .gitignore
└── README.md
```

### Key Files

| File | Purpose |
|---|---|
| `pollen_pipeline_dag.py` | Defines the Airflow workflow and task dependencies |
| `ingest_pollen_api.py` | Fetches API data, applies change detection, and uploads raw JSON to Azure Blob Storage |
| `load_blob_to_snowflake.py` | Loads raw JSON files from Blob Storage into Snowflake |
| `pollen_dbt/` | Contains dbt staging models, mart models, schemas, and tests |
| `requirements.txt` | Python dependencies installed in the Astro runtime |
| `.dockerignore` | Prevents unnecessary local files from being packaged during deployment |

---

## Environment Configuration

Create a `.env` file in the project root for local development:

```bash
# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_STORAGE_CONTAINER_NAME=your_container_name

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

**Note:**  
The `.env` file is excluded through `.gitignore` and should never be committed to GitHub.

For Astronomer Cloud deployment, the same variables are configured as **Deployment Environment Variables** in the Astronomer UI.

---

## Prerequisites

Before running or deploying this project, ensure you have:

- Docker Desktop
- Astronomer CLI
- A Snowflake account with:
  - Database
  - Schema
  - Warehouse
  - User credentials
- Azure Blob Storage container and connection string
- An Astronomer Cloud workspace and deployment

---

## Local Development

### 1. Start Airflow locally

From the project root:

```bash
astro dev start
```

This starts a local Airflow environment in Docker.

### 2. Open the Airflow UI

```text
http://localhost:8080
```

### 3. Stop the local environment

```bash
astro dev stop
```

### 4. Restart the local environment

```bash
astro dev restart
```

### 5. Reset the local environment

```bash
astro dev kill
```

---

## Running dbt Manually

To run dbt transformations:

```bash
dbt run \
  --profiles-dir include/pollen_pipeline/pollen_dbt \
  --project-dir include/pollen_pipeline/pollen_dbt
```

To run dbt tests:

```bash
dbt test \
  --profiles-dir include/pollen_pipeline/pollen_dbt \
  --project-dir include/pollen_pipeline/pollen_dbt
```

---

## Deployment to Astronomer Cloud

Deploy the project from the root directory:

```bash
astro deploy
```

After deployment, the DAG becomes available in the Astronomer-hosted Airflow UI.

The deployment process packages:

- DAG definitions
- Python ingestion and loading scripts
- dbt project files
- Runtime dependencies

into an Astro-compatible image and deploys it to the selected cloud environment.

---

## Data Quality

The pipeline includes dbt tests to validate transformed datasets.

Examples include:

- Required fields are not null
- Staging and mart model outputs are structurally valid
- Analytics-ready tables are built successfully after each run

---

## Current Output

The final analytics-ready mart table is:

```text
POLLEN_DB.MARTS.MART_POLLEN_DAILY_SUMMARY
```

This table is designed for reporting, trend analysis, and potential dashboard development.

---

## Development Workflow

1. Update or create DAG logic in `dags/`
2. Update ingestion or loading scripts in `include/pollen_pipeline/`
3. Update dbt models and tests in `include/pollen_pipeline/pollen_dbt/`
4. Test locally with:

   ```bash
   astro dev start
   ```

5. Deploy to Astronomer Cloud with:

   ```bash
   astro deploy
   ```

6. Trigger or monitor the DAG in the Airflow UI

---

## Monitoring and Troubleshooting

### Check DAG Task Status

In the Airflow UI, verify that all tasks succeed:

```text
ingest_pollen_api
load_blob_to_snowflake
dbt_run
dbt_test
```

### Common Deployment Issue: Large Docker Context

To avoid large or unnecessary deployment images, `.dockerignore` excludes files such as:

```text
dbt_env/
logs/
__pycache__/
*.pyc
```

This prevents local environments and generated artifacts from being included in the deployment image.

---

## Future Improvements

Potential enhancements include:

- Moving watermark and ingestion log state to persistent cloud storage
- Adding automated alerting for DAG failures
- Building a dashboard on top of the mart table
- Expanding the pipeline to additional regions or environmental variables
- Adding CI/CD checks for DAG parsing and dbt tests

---

## References

- [Apache Airflow](https://airflow.apache.org/)
- [Astronomer](https://www.astronomer.io/)
- [dbt](https://docs.getdbt.com/)
- [Snowflake](https://docs.snowflake.com/)
- [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/)
- [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api)

---

**Last Updated:** May 2026
