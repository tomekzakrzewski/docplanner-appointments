import pendulum
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import (
    SQLValueCheckOperator,
    SQLTableCheckOperator,
)
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from include.data_processing import (
    find_source_file,
    clean_appointment_data,
    save_cleaned_data,
)
from include.validators import validate_csv_file
from include.database import load_stg_table, cleanup_temp_file, load_agg_table

logger = logging.getLogger("airflow.task")

DB_CONN_ID = "postgres_default"
DATA_PATH = "/opt/airflow/data"
AGG_TABLE = "agg_daily_appointments"
STG_TABLE = "stg_daily_appointments"
EXPECTED_COLUMNS = ["appointment_id", "clinic_id", "patient_id", "created_at"]


@contextmanager
def get_db_connection(conn_id: str):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    engine = pg_hook.get_sqlalchemy_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@dag(
    dag_id="daily_clinic_appointment",
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/Warsaw"),
    schedule="@daily",
    catchup=False,
    # doc_md=__doc__,
    tags=["data-pipeline"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # "email_on_failure": True,
    },
)
def daily_clinic_appointment_dag():
    @task
    def extract_source_file(ds: str) -> str:
        # Assume that file is expected to land daily, naming convention won't change
        """File extraction for yesterdays date"""
        logger.info(f"Starting file extraction task for date: {ds}")

        try:
            date_str = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y_%m_%d")
            file_path = find_source_file(DATA_PATH, date_str)

            logger.info(f"File extraction completed successfully: {file_path}")
            return file_path

        except FileNotFoundError as e:
            logger.error(f"Source file not found for date {ds}: {e}")
            raise AirflowFailException(f"Source file missing for {ds}: {e}")

    @task
    def validate_source_file(filepath: str, ds: str) -> str:
        # Assume that CSV structure remains consistent
        """Validate source file structure"""
        logger.info(f"Starting validation for: {filepath}")

        try:
            validate_csv_file(filepath, EXPECTED_COLUMNS)
            logger.info("File validation completed successfully")
            return filepath

        except ValueError as e:
            logger.error(f"Validation failed: {e}")
            raise AirflowFailException(
                f"File validation failed: {e}"
            )  # could handle better, give more information
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise AirflowFailException(f"Validation task failed: {e}")

    @task
    def clean_source_data(filepath: str, ds: str) -> str:
        # Assume that files are small enough to process in-memory with pandas
        """Clean source data and save to temporary file"""
        logger.info(f"Starting data cleaning for: {filepath}")

        try:
            cleaned_df = clean_appointment_data(filepath, EXPECTED_COLUMNS)
            temp_file = save_cleaned_data(cleaned_df, DATA_PATH, ds)

            logger.info("Data cleaning completed successfully")
            return temp_file

        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            raise AirflowFailException(f"Data cleaning failed: {e}")

    @task
    def load_stg_daily_appointments(temp_file: str, ds: str) -> int:
        # Assume assume that each CSV file (appointments_YYYY_MM_DD.csv) contains only
        # records where the created_at date is the same the date in the filename
        """Load data to staging table"""
        logger.info(f"Starting staging load for: {temp_file}")

        try:
            with get_db_connection(DB_CONN_ID) as conn:
                with conn.begin():
                    row_count = load_stg_table(temp_file, STG_TABLE, conn, ds)

            logger.info(f"Staging load completed successfully: {row_count} records")
            return row_count

        except Exception as e:
            logger.error(f"Staging load failed: {e}")
            raise AirflowFailException(f"Staging load failed: {e}")

        finally:
            cleanup_temp_file(temp_file)

    dq_staging = SQLTableCheckOperator(  # Queries could be moved to different file
        task_id="dq_checks_staging",
        conn_id=DB_CONN_ID,
        table=STG_TABLE,
        checks={
            "row_count_check": {
                "check_statement": f"""
                    (SELECT COUNT(*)
                    FROM {STG_TABLE} 
                    WHERE created_at = '{{{{ ds }}}}') = {{{{ ti.xcom_pull(task_ids='load_stg_daily_appointments') }}}}
                """.strip(),
            },
            "null_check": {
                "check_statement": f"""
                    (SELECT COUNT(*) 
                    FROM {STG_TABLE} 
                    WHERE created_at = '{{{{ ds }}}}' 
                        AND (appointment_id IS NULL OR clinic_id IS NULL)) = 0
                """.strip(),
            },
            "duplicate_check": {
                "check_statement": f"""
                    NOT EXISTS (
                        SELECT 1 
                        FROM {STG_TABLE} 
                        WHERE created_at = '{{{{ ds }}}}' 
                        GROUP BY appointment_id 
                        HAVING COUNT(*) > 1)
                """.strip(),
            },
        },
    )

    @task
    def load_agg_daily_appointments(ds: str):
        """Load agg table from staging data"""
        logger.info(f"Starting agg table load for date: {ds}")

        try:
            with get_db_connection(DB_CONN_ID) as conn:
                with conn.begin():
                    row_count = load_agg_table(STG_TABLE, AGG_TABLE, conn, ds)

            logger.info(f"Fact table load completed successfully: {row_count} records")

        except Exception as e:
            logger.error(f"Fact table load failed: {e}")
            raise AirflowFailException(f"Fact table load failed: {e}")

    # aggregation check
    # The sum of appointments_count in the agg table for a given day must equal
    # the total number of valid rows in the staging table for that same day
    dq_check_comparison = SQLValueCheckOperator(
        task_id="check_comparison",
        conn_id="postgres_default",
        sql="""
            SELECT ABS(
                COALESCE((
                    SELECT SUM(appointments_count) 
                    FROM {{ params.agg_table }}
                    WHERE appointment_date = '{{ ds }}'
                ), 0) -
                COALESCE((
                    SELECT COUNT(*) 
                    FROM {{ params.stg_table }}
                    WHERE DATE(created_at) = '{{ ds }}'
                ), 0)
            ) AS difference
        """,
        pass_value=0,
        params={"agg_table": AGG_TABLE, "stg_table": STG_TABLE},
    )

    end_pipeline = EmptyOperator(task_id="end")

    source_file = extract_source_file()
    validated_file = validate_source_file(source_file)
    cleaned_data = clean_source_data(validated_file)
    staged_data = load_stg_daily_appointments(cleaned_data)
    # dq_results = dq_checks_staging(staged_data)
    agg_load = load_agg_daily_appointments()

    (
        source_file
        >> validated_file
        >> cleaned_data
        >> staged_data
        # >> dq_results
        >> dq_staging
        >> agg_load
        >> dq_check_comparison
        >> end_pipeline
    )


daily_clinic_appointment = daily_clinic_appointment_dag()
