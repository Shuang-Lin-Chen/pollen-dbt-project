# Pollen Cloud Data Pipeline

A cloud-based end-to-end data engineering pipeline that ingests pollen forecast data from the Open-Meteo Air Quality API, stores raw JSON in Azure Blob Storage, loads it into Snowflake, and transforms it into analytics-ready marts using dbt.

The workflow is orchestrated by Apache Airflow and deployed on Astronomer Cloud.

---

## Overview

This project demonstrates a modern **ELT data pipeline** for pollen forecast analytics.

The pipeline:

1. Extracts pollen forecast data from an external API
2. Stores raw JSON files in Azure Blob Storage
3. Loads raw JSON into Snowflake using key-pair authentication
4. Transforms raw data into staging and mart models using dbt
5. Runs dbt tests to validate data quality
6. Executes through an Airflow DAG deployed on Astronomer Cloud

The final mart table is designed for analytics, reporting, and dashboarding use cases.

---

## Project Architecture

### Technology Stack

- **Workflow Orchestration**: Apache Airflow
- **Cloud Deployment Platform**: Astronomer Cloud
- **Cloud Storage**: Azure Blob Storage
- **Data Warehouse**: Snowflake
- **Data Transformation**: dbt
- **Programming Language**: Python
- **Version Control**: GitHub

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

The ingestion script fetches pollen forecast data from the **Open-Meteo Air Quality API** for selected Swedish regions.

### 2. Raw Storage

The API response is wrapped with ingestion metadata and uploaded as raw JSON files to **Azure Blob Storage**.

### 3. Load

The loading script reads the latest raw JSON file from Azure Blob Storage and inserts it into the Snowflake raw table:

```text
POLLEN_DB.RAW.RAW_POLLEN
```

Snowflake authentication is handled through **key-pair authentication** using:

```text
SNOWFLAKE_PRIVATE_KEY
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
```

The private key is stored securely as an Astronomer deployment environment variable and is not committed to GitHub.

### 4. Transform

dbt is used to build:

- **Staging models** for cleaning and flattening raw JSON data
- **Mart models** for analytics-ready pollen summaries

### 5. Validate

dbt tests are executed after transformation to validate model quality and required fields.

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
write_private_key_file
        ↓
load_blob_to_snowflake
        ↓
dbt_run
        ↓
dbt_test
```

### Task Description

| Task | Purpose |
|---|---|
| `ingest_pollen_api` | Fetches pollen forecast data and uploads raw JSON to Azure Blob Storage |
| `write_private_key_file` | Prepares the Snowflake private key for runtime authentication |
| `load_blob_to_snowflake` | Loads the latest JSON blob into the Snowflake RAW layer |
| `dbt_run` | Runs dbt transformations for staging and mart models |
| `dbt_test` | Runs dbt data quality tests |

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
├── .astro/
│
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

---

## Key Files

| File | Purpose |
|---|---|
| `dags/pollen_pipeline_dag.py` | Defines the Airflow DAG, task order, and schedule |
| `include/pollen_pipeline/ingest_pollen_api.py` | Extracts pollen forecast data and uploads raw JSON to Azure Blob Storage |
| `include/pollen_pipeline/load_blob_to_snowflake.py` | Loads raw JSON from Azure Blob Storage into Snowflake |
| `include/pollen_pipeline/pollen_dbt/` | Contains dbt staging models, mart models, schemas, and tests |
| `include/pollen_pipeline/pollen_dbt/profiles.yml` | Defines dbt connection configuration for Snowflake |
| `requirements.txt` | Python dependencies installed in the Astro runtime |
| `packages.txt` | System-level packages installed in the Astro runtime |
| `.dockerignore` | Prevents unnecessary local files from being packaged during deployment |
| `.gitignore` | Prevents secrets, keys, logs, and local files from being committed |

---

## Environment Configuration

For local development, create a `.env` file in the project root.

```bash
# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_STORAGE_CONTAINER_NAME=your_container_name

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_user
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Snowflake key-pair authentication
SNOWFLAKE_PRIVATE_KEY=your_private_key
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_private_key_passphrase
```

The `.env` file is excluded through `.gitignore` and should never be committed to GitHub.

For Astronomer Cloud deployment, these variables are configured as **Deployment Environment Variables** in the Astronomer UI.

---

## Snowflake Authentication

This project uses **Snowflake key-pair authentication** instead of password authentication.

The Snowflake user must have a matching RSA public key configured.

To inspect the configured public key fingerprint:

```sql
DESC USER SHUANGLIN;
```

The `RSA_PUBLIC_KEY_FP` value in Snowflake must match the fingerprint generated from the private key used by Astronomer.

The private key itself should never be committed to GitHub.

The following files are intentionally ignored:

```text
.env
rsa_key.p8
rsa_key.pub
*.pem
*.key
```

---

## Prerequisites

Before running or deploying this project, ensure you have:

- Docker Desktop
- Astronomer CLI
- A Snowflake account with:
  - Database
  - Schema
  - Warehouse
  - User with key-pair authentication configured
- Azure Blob Storage container and connection string
- An Astronomer Cloud workspace and deployment
- GitHub repository for version control

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
astro deploy -f
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
- Staging model output is structurally valid
- Mart model output is analytics-ready
- dbt transformations complete successfully after each run

---

## Current Output

The final analytics-ready mart table is:

```text
POLLEN_DB.MARTS.MART_POLLEN_DAILY_SUMMARY
```

This table is designed for:

- Pollen trend analysis
- Regional pollen risk comparison
- Analytics and dashboarding
- Downstream reporting

---

## Example Snowflake Checks

Check raw ingestion dates:

```sql
SELECT
    TO_DATE(RAW_JSON:metadata:ingested_at::timestamp) AS ingestion_date,
    COUNT(*) AS records_loaded
FROM POLLEN_DB.RAW.RAW_POLLEN
GROUP BY 1
ORDER BY ingestion_date DESC;
```

Check latest staging rows:

```sql
SELECT *
FROM POLLEN_DB.STAGING.STG_POLLEN_FORECAST
ORDER BY LOADED_AT DESC
LIMIT 100;
```

Check mart output:

```sql
SELECT *
FROM POLLEN_DB.MARTS.MART_POLLEN_DAILY_SUMMARY
ORDER BY FORECAST_DATE DESC
LIMIT 100;
```

---

## Development Workflow

1. Update DAG logic in `dags/`
2. Update ingestion or loading scripts in `include/pollen_pipeline/`
3. Update dbt models and tests in `include/pollen_pipeline/pollen_dbt/`
4. Test locally with:

   ```bash
   astro dev start
   ```

5. Deploy to Astronomer Cloud with:

   ```bash
   astro deploy -f
   ```

6. Trigger or monitor the DAG in the Airflow UI
7. Confirm Snowflake RAW, STAGING, and MART outputs
8. Commit and push clean code to GitHub

---

## GitHub Workflow

After making changes locally, check the modified files:

```bash
git status
```

Stage the relevant files:

```bash
git add README.md
git add .gitignore
git add dags/pollen_pipeline_dag.py
git add include/pollen_pipeline/load_blob_to_snowflake.py
git add include/pollen_pipeline/pollen_dbt/profiles.yml
git add requirements.txt
```

Commit the changes:

```bash
git commit -m "Update pipeline documentation and configuration"
```

If the remote branch has newer commits, pull with rebase:

```bash
git pull --rebase origin main
```

Push to GitHub:

```bash
git push origin main
```

---

## Monitoring and Troubleshooting

### Check DAG Task Status

In the Airflow UI, verify that all tasks succeed:

```text
ingest_pollen_api
write_private_key_file
load_blob_to_snowflake
dbt_run
dbt_test
```

### Common Issue: Snowflake JWT Token Is Invalid

This usually means the private key used by Astronomer does not match the RSA public key configured on the Snowflake user.

To verify:

1. Check the Snowflake user:

   ```sql
   DESC USER SHUANGLIN;
   ```

2. Confirm that `RSA_PUBLIC_KEY_FP` matches the public key fingerprint generated from the private key used in Astronomer.

3. If the fingerprints do not match, update the Snowflake user with the correct public key.

### Common Deployment Issue: Large Docker Context

To avoid large or unnecessary deployment images, `.dockerignore` and `.gitignore` exclude files such as:

```text
dbt_env/
logs/
raw_data/
__pycache__/
*.pyc
.env
rsa_key.p8
rsa_key.pub
```

This prevents local environments, generated artifacts, and secrets from being included in the deployment image or committed to GitHub.

---

## Security Notes

The following files should never be committed to GitHub:

```text
.env
rsa_key.p8
*.pem
*.key
```

The private key should be stored securely as an environment variable in Astronomer Cloud.

The repository should only contain source code, configuration templates, dbt models, and documentation.

---

## Future Improvements

Potential enhancements include:

- Removing the separate `write_private_key_file` task if the loading script handles private key parsing directly
- Moving watermark and ingestion log state to persistent cloud storage
- Adding automated alerting for DAG failures
- Building a dashboard on top of the mart table
- Expanding the pipeline to additional regions or environmental variables
- Adding CI/CD checks for DAG parsing and dbt tests
- Adding automated GitHub Actions for dbt validation

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
