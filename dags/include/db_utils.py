import pandas as pd
import os
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def load_stg_table(temp_file: str, table_name: str, conn, ds: str):
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
