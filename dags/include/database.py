import pandas as pd
import os
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def load_to_staging_table(temp_file: str, table_name: str, conn, ds: str):
    """Load data from parquet file to staging table"""
    logger.info(f"Loading data from {temp_file} to {table_name}")

    # Read data
    df = pd.read_parquet(temp_file)
    logger.info(f"Read {len(df)} records from parquet file")

    # Delete existing records for the date
    logger.info(f"Deleting existing records for date: {ds}")
    delete_result = conn.execute(
        text(f"DELETE FROM {table_name} WHERE created_at = :ds"), {"ds": ds}
    )
    logger.info(f"Deleted {delete_result.rowcount} existing records")

    # Insert new records
    logger.info("Inserting new records...")
    df.to_sql(table_name, conn, if_exists="append", index=False, method="multi")
    logger.info(f"Successfully inserted {len(df)} records")

    return len(df)


def cleanup_temp_file(temp_file: str):
    """Remove temporary file"""
    if os.path.exists(temp_file):
        os.remove(temp_file)
        logger.info(f"Cleaned up temp file: {temp_file}")


def load_fact_table(staging_table: str, fact_table: str, conn, ds: str):
    """Load aggregated data from staging to fact table"""
    logger.info(f"Loading data from {staging_table} to {fact_table} for date: {ds}")

    # Delete existing data for idempotency
    logger.info(f"Deleting existing fact data for date: {ds}")
    delete_result = conn.execute(
        text(f"""
            DELETE FROM {fact_table}
            WHERE appointment_date = :date
        """),
        {"date": ds},
    )
    logger.info(f"Deleted {delete_result.rowcount} existing fact records")

    # Insert aggregated data
    logger.info("Inserting aggregated appointment data...")
    insert_result = conn.execute(
        text(f"""
            INSERT INTO {fact_table} (clinic_id, appointment_date, appointments_count)
            SELECT
                clinic_id,
                DATE(created_at) as appointment_date,
                COUNT(*) as appointments_count
            FROM {staging_table}
            WHERE DATE(created_at) = :date
            GROUP BY clinic_id, DATE(created_at)
        """),
        {"date": ds},
    )

    logger.info(f"Inserted {insert_result.rowcount} records to {fact_table}")
    return insert_result.rowcount
