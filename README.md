# Pollen dbt Project

A modern data engineering pipeline that transforms pollen API data into analytics-ready datasets using Apache Airflow and dbt on Snowflake.

## Overview

This project orchestrates the collection and transformation of pollen data from external APIs, processes it through staging and mart models, and makes it available for analytics and dashboarding. It leverages Apache Airflow for workflow orchestration and dbt (data build tool) for data transformation on Snowflake.

## Project Architecture

### Technology Stack
- **Orchestration**: Apache Airflow (via Astronomer)
- **Transformation**: dbt with Snowflake
- **Data Warehouse**: Snowflake
- **Cloud Storage**: Azure Blob Storage
- **Runtime**: Astronomer Runtime (Astro)

### Project Structure

```
pollen-dbt-project/
├── dags/                      # Airflow DAG definitions
├── include/                   # Additional project files (models, configs, etc.)
├── plugins/                   # Custom Airflow plugins
├── logs/                      # Airflow logs (generated at runtime)
├── Dockerfile                 # Custom Docker image configuration
├── requirements.txt           # Python dependencies
├── packages.txt               # OS-level dependencies
├── airflow_settings.yaml      # Local Airflow configuration
└── README.md                  # This file
```

## Data Pipeline

The pipeline follows a typical ETL pattern:

1. **Extract**: Fetch pollen data from external APIs
2. **Stage**: Load raw data into Snowflake staging tables
3. **Transform**: Apply dbt transformations to create mart models
4. **Load**: Make transformed data available for analytics and dashboards

## Prerequisites

Before running this project locally, ensure you have:

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (for local development)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (v1.0+)
- Snowflake account with database and warehouse provisioned
- Azure Blob Storage credentials (for data storage)

## Environment Configuration

Create a `.env` file in the project root with the following variables:

```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse

# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string

# Pollen API (if required)
POLLEN_API_KEY=your_api_key
```

**Note**: The `.env` file is ignored by git (see `.gitignore`) to protect sensitive credentials.

## Getting Started

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Airflow locally**:
   ```bash
   astro dev start
   ```
   
   This command spins up five Docker containers:
   - **Postgres**: Metadata database for Airflow
   - **Scheduler**: Monitors and triggers DAG tasks
   - **DAG Processor**: Parses DAG files
   - **API Server**: Serves Airflow UI and API
   - **Triggerer**: Manages deferred tasks

3. **Access the Airflow UI**:
   Open your browser to `http://localhost:8080/`

4. **Access Postgres Database** (if needed):
   Connection string: `postgresql://localhost:5432/postgres`

### Common Commands

```bash
# View logs for a specific DAG
astro dev logs -f <dag_id>

# Stop Airflow
astro dev stop

# Restart Airflow
astro dev restart

# Reset Airflow (WARNING: clears all data)
astro dev kill
```

## Dependencies

Key Python packages used in this project:

- `apache-airflow`: Workflow orchestration
- `dbt-snowflake`: dbt adapter for Snowflake
- `snowflake-connector-python`: Snowflake connection
- `azure-storage-blob`: Azure storage access
- `python-dotenv`: Environment variable management
- `requests`: HTTP requests for API calls

See `requirements.txt` for the complete list.

## Project Configuration

### airflow_settings.yaml

Use this file to define Airflow Connections, Variables, and Pools during local development:

```yaml
connections:
  - conn_id: snowflake_default
    conn_type: snowflake
    host: <account>.snowflakecomputing.com
    schema: <schema>
    login: <username>
    password: <password>
    extra: '{"warehouse": "<warehouse>", "database": "<database>"}'
```

## Deployment

### Deploy to Astronomer

If you have an Astronomer account, deploy this project by pushing code to a Deployment:

```bash
astro deploy
```

For detailed deployment instructions, refer to the [Astronomer deployment guide](https://www.astronomer.io/docs/astro/deploy-code).

## Development Workflow

1. Create a new branch for your changes
2. Update or create DAGs in the `dags/` directory
3. Update dbt models and tests as needed
4. Test locally with `astro dev start`
5. Verify DAGs parse correctly and transformations succeed
6. Push code and create a pull request for review

## Monitoring & Troubleshooting

### Port Conflicts

If Airflow fails to start due to port conflicts:

```bash
# Check which services are using ports
lsof -i :8080
lsof -i :5432

# Or change ports in `.env`
```

Refer to [Astronomer troubleshooting docs](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally) for more help.

### dbt Tests

Run dbt tests to validate data quality:

```bash
dbt test --profiles-dir include/profiles --project-dir include/dbt
```

## Contributing

When contributing to this project:

1. Follow the existing code structure
2. Add or update documentation as needed
3. Test changes locally before submitting
4. Ensure dbt tests pass
5. Keep commits atomic and descriptive

## Support

For issues or questions:

- Check the [Astronomer documentation](https://www.astronomer.io/docs/)
- Review [dbt documentation](https://docs.getdbt.com/)
- Refer to [Snowflake guides](https://docs.snowflake.com/)

---

**Last Updated**: May 2026
