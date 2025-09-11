import pandas as pd
import logging

logger = logging.getLogger(__name__)


def validate_csv_file(filepath: str, expected_columns: list):
    """Validate CSV file structure"""
    logger.info(f"Validating CSV file: {filepath}")

    df = pd.read_csv(filepath)
    logger.info(f"Read CSV with {len(df)} rows")

    if df.empty:
        raise ValueError("CSV file is empty")

    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    extra_columns = set(df.columns) - set(expected_columns)
    if extra_columns:
        logger.warning(f"Extra columns found: {extra_columns}")

    logger.info("CSV validation passed")
    return df
