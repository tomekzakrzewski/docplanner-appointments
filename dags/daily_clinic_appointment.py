import os
import pendulum
import pandas as pd
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager
from sqlalchemy import text

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.empty import EmptyOperator

## Logger
logger = logging.getLogger("airflow.task")

# --- DAG Configuration ---
DB_CONN_ID = "postgres_default"  # The default connection ID for the backend DB
DATA_PATH = "/opt/airflow/data"  # This is the path inside the container
FACT_TABLE = "fct_daily_appointments"
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
        "retries":  2,
        "retry_delay": timedelta(minutes=5)
        # "email_on_failure": True,
    }
)

def daily_clinic_appointment_dag():
    
    @task
    def extract_file_for_processing(ds: str) -> str:
        date_str = datetime.strptime(ds, '%Y-%m-%d').strftime("%Y_%m_%d")
        filename = f"appointments_{date_str}.csv"
        filepath = os.path.join(DATA_PATH, filename)

        logger.info(f"Looking for file under path: {filepath}")

        if not os.path.exists(filepath):
            raise AirflowFailException(f"File: {filename} not found under path: {filepath}")

        logger.info(f"Found file: {filename} under path: {filepath}")

        return filepath


    @task
    def validate_file(filepath: str, ds: str) -> str:
        logger.info("Reading CSV file...")
        
        try:
            df = pd.read_csv(filepath)
            logger.info(f"Successfully read CSV with {len(df)} rows")
        except pd.errors.ParserError as e:
            raise AirflowFailException(f"Failed to parse CSV file. File may be corrupted: {e}")
        except Exception as e:
            raise AirflowFailException(f"Error reading CSV file: {e}")

        # Check if empty
        if df.empty:
            logger.error("CSV file contains no rows")
            raise AirflowFailException("CSV file is empty")
        
        # Check required columns
        missing_columns = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_columns:
            raise AirflowFailException(f"CSV missing required columns: {missing_columns}")
        
        # Log extra columns (non-blocking)
        extra_columns = set(df.columns) - set(EXPECTED_COLUMNS)
        if extra_columns:
            logger.warning(f"CSV file contains extra columns: {extra_columns}")

        return filepath


    @task
    def clean_data(filepath: str, ds: str) -> str:
        df = pd.read_csv(filepath)

        df['clinic_id'] = df['clinic_id'].str.lower().str.strip()
        df['created_at'] = pd.to_datetime(df['created_at'], format="mixed", errors="coerce")
        df.dropna(subset=['appointment_id', 'clinic_id'], inplace=True)

        temp_file = f"{DATA_PATH}/temp_validated_{ds}.parquet"
        df.to_parquet(temp_file)

        return temp_file

        # duplicates

    @task
    def load_to_staging(temp_file: str, ds: str):
        logger.info(f"Started loading {temp_file} to {STG_TABLE}")

        raw_df = pd.read_parquet(temp_file)

        with get_db_connection(DB_CONN_ID) as conn:
            logger.info("Connected to database...")
            with conn.begin():
                try:
                    logger.info(f"Deleting existing record for date: {ds}")
                    delete_result = conn.execute(
                        text(f"""
                             DELETE FROM {STG_TABLE}
                             WHERE
                                created_at = :ds
                             """),
                        {"ds": ds}
                    )
                    logger.info(f"Deleted {delete_result.rowcount} records")
                    
                    logger.info(f"Inserting new records...")
                    raw_df.to_sql(
                        STG_TABLE,
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                except Exception as e:
                    raise AirflowFailException(f"Error during DB operation, transaction will be rolled back: {e}")

        os.remove(temp_file)

        logger.info(f"Successfully loaded {len(raw_df)} record to {STG_TABLE}")
        return len(raw_df)

    @task
    def staging_dq_checks(row_count: int, ds: str):

        dq_checks = [
            {'name': 'row_count_check',
             'sql': f'SELECT COUNT(*) FROM {STG_TABLE} WHERE created_at = :ds;',
             'test':  lambda result: result == row_count,
             'error': 'Staging table is empty.'},

            {'name': 'null_check',
             'sql': f'SELECT COUNT(*) FROM {STG_TABLE} WHERE created_at = :ds AND (appointment_id IS NULL OR clinic_id IS NULL);',
             'test':  lambda result: result == 0,
             'error': 'Found NULL values in key columns.'},

            {'name': 'duplicate_check',
             'sql': f'SELECT COUNT(*) FROM (SELECT 1 FROM {STG_TABLE} WHERE created_at = :ds GROUP BY appointment_id HAVING COUNT(*) > 1) d;',
             'test':  lambda result: result == 0,
             'error': 'Found duplicate appointment_ids.'},
        ]

        failed_dqs = []
        with get_db_connection(DB_CONN_ID) as conn:
            for dq in dq_checks:
                name = dq['name']
                sql = dq['sql']

                result = conn.execute(text(sql), {'ds': ds}).scalar()

                if not dq['test'](result):
                    error_message = f"DQ check failed: {name}, {dq['error']}, expected: {dq['test']}, got: {result}"
                    logger.error(error_message)
                    failed_dqs.append(error_message)


        if failed_dqs:
            raise AirflowFailException(f"DQ check for ds: {ds}:\n" + "\n".join(failed_dqs))

        logger.info(f"DQ for ds: {ds} passed successfully")




    a = extract_file_for_processing()
    b = validate_file(a)
    c = clean_data(b)
    d = load_to_staging(c)
    e = staging_dq_checks(d)

    a >> b >> c >> d >> e

daily_clinic_appointment = daily_clinic_appointment_dag()

    





#
# @task
# def dq_checks_staging(row_count: int, ds: str):
#     logger.info(f"Running data quality checks on {STG_TABLE}")
#
#     with get_db_connection(DB_CONN_ID) as conn:
#             logger.info(f"Performing row count validation for ds: {ds}")
#             count_sql = f"""
#                 SELECT
#                     COUNT(*)
#                 FROM {STG_TABLE}
#                 WHERE
#                     created_at = :ds
#             """
#             count = conn.execute(
#                 text(f"""
#                     SELECT
#                         COUNT(*)
#                     FROM {STG_TABLE}
#                     WHERE
#                         created_at = :ds
#                     """),
#                 {"ds": ds}
#                 ).scalar()
#
#             if count != staging_rows:
#                 logger.info(f"Row count mismatch - Expected: {staging_rows}, Found: {count} ")
#                 raise AirflowFailException(f"Staging load failed: expected: {staging_rows}, got: {count}")
#
#
#
#
#
#
#
#
