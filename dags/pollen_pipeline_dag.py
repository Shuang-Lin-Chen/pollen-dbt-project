from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


# Astro project root inside the Airflow runtime container
AIRFLOW_PROJECT_DIR = Path("/usr/local/airflow")

# Supporting pipeline files deployed with the Astro project
PIPELINE_DIR = AIRFLOW_PROJECT_DIR / "include" / "pollen_pipeline"
DBT_PROJECT_DIR = PIPELINE_DIR / "pollen_dbt"


with DAG(
    dag_id="pollen_batch_pipeline",
    start_date=datetime(2026, 5, 19),
    schedule="0 6 * * *",
    catchup=False,
    tags=["pollen", "azure", "snowflake", "dbt", "astronomer"],
) as dag:

    ingest_pollen_api = BashOperator(
        task_id="ingest_pollen_api",
        bash_command=f'''
        cd "{PIPELINE_DIR}" &&
        python ingest_pollen_api.py
        ''',
    )

    load_blob_to_snowflake = BashOperator(
        task_id="load_blob_to_snowflake",
        bash_command=f'''
        cd "{PIPELINE_DIR}" &&
        python load_blob_to_snowflake.py
        ''',
    )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f'''
    cd "{DBT_PROJECT_DIR}" &&
    dbt clean --profiles-dir "{DBT_PROJECT_DIR}" &&
    dbt run --profiles-dir "{DBT_PROJECT_DIR}"
    ''',
)

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'''
        cd "{DBT_PROJECT_DIR}" &&
        dbt test --profiles-dir "{DBT_PROJECT_DIR}"
        ''',
    )

    ingest_pollen_api >> load_blob_to_snowflake >> dbt_run >> dbt_test