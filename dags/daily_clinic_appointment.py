import pendulum
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from include.data_utils import (
    clean_appointment_data,
    save_cleaned_data,
)
from include.file_utils import validate_csv_file, find_source_file
from include.db_utils import load_stg_table, cleanup_temp_file
from include.sql.queries import DB_QUERIES, STAGING_DQ_CHECKS, AGG_DQ_CHECKS

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

    with TaskGroup(
        group_id="stg_data_quality",
        prefix_group_id=False,
    ) as staging_dq_group:
        # Row count validation
        check_row_count = SQLCheckOperator(
            task_id="check_row_count",
            conn_id=DB_CONN_ID,
            sql=STAGING_DQ_CHECKS["row_count_check"],
            params={
                "stg_table": STG_TABLE,
            },
            doc_md="Validates that staged row count matches loaded count",
        )

        # Null value validation
        check_nulls = SQLCheckOperator(
            task_id="check_null_values",
            conn_id=DB_CONN_ID,
            sql=STAGING_DQ_CHECKS["null_check"],
            params={"stg_table": STG_TABLE},
            doc_md="Ensures no null values in critical columns",
        )

        # Duplicate validation
        check_duplicates = SQLCheckOperator(
            task_id="check_duplicates",
            conn_id=DB_CONN_ID,
            sql=STAGING_DQ_CHECKS["duplicate_check"],
            params={"stg_table": STG_TABLE},
            doc_md="Validates no duplicate appointment_ids exist",
        )

    load_agg_daily_appointments = SQLExecuteQueryOperator(
        task_id="load_agg_daily_appointments",
        conn_id=DB_CONN_ID,
        sql=DB_QUERIES["insert_data_agg"],
        params={"agg_table": AGG_TABLE, "stg_table": STG_TABLE},
    )

    # aggregation check
    # The sum of appointments_count in the agg table for a given day must equal
    # the total number of valid rows in the staging table for that same day
    dq_check_comparison = SQLCheckOperator(
        task_id="check_comparison",
        conn_id=DB_CONN_ID,
        sql=AGG_DQ_CHECKS["comparison_check"],
        params={"agg_table": AGG_TABLE, "stg_table": STG_TABLE},
        # doc_md="Validates ",
    )

    end_pipeline = EmptyOperator(task_id="end")

    source_file = extract_source_file()
    validated_file = validate_source_file(source_file)
    cleaned_data = clean_source_data(validated_file)
    staged_data = load_stg_daily_appointments(cleaned_data)

    (
        source_file
        >> validated_file
        >> cleaned_data
        >> staged_data
        >> staging_dq_group
        >> load_agg_daily_appointments
        >> dq_check_comparison
        >> end_pipeline
    )


daily_clinic_appointment = daily_clinic_appointment_dag()
