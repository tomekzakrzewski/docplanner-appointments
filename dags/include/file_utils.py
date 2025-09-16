import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


def validate_csv_file(filepath: str, expected_columns: list):
    """Validate CSV file structure"""
    logger.info(f"Validating CSV file: {filepath}")

    try:
        df = pd.read_csv(filepath)
        logger.info(f"Read CSV with {len(df)} rows")
    except pd.errors.ParserError as e:
        raise ValueError(
            f"CSV parsing failed - file may be corrupted: {filepath}. Error: {str(e)}"
        )

    if df.empty:
        raise ValueError("CSV file is empty")

    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    extra_columns = set(df.columns) - set(expected_columns)
    if extra_columns:
        logger.warning(f"Extra columns found: {extra_columns}")

    logger.info("CSV validation passed")
    return


def find_source_file(data_path: str, date_str: str) -> str:
    """Find and return path to source file for given date"""
    logger.info(f"Looking for appointments file for date: {date_str}")

    filename = f"appointments_{date_str}.csv"
    filepath = os.path.join(data_path, filename)

    logger.info(f"Checking file path: {filepath}")

    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"File: {filename} not found under path: {filepath}")

    logger.info(f"Found file: {filepath}")
    return filepath
