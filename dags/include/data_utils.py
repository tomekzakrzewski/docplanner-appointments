import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_appointment_data(filepath: str, expected_columns: list):
    """Clean and standardize appointment data"""
    logger.info(f"Cleaning data from: {filepath}")

    df = pd.read_csv(filepath)
    logger.info(f"Read {len(df)} rows for cleaning")

    # Clean data
    df["clinic_id"] = df["clinic_id"].str.lower().str.strip()
    df["created_at"] = pd.to_datetime(df["created_at"], format="mixed", errors="coerce")
    df.dropna(subset=["appointment_id", "clinic_id", "created_at"], inplace=True)
    df = df[df["clinic_id"].str.len() > 0]

    df = df[expected_columns]

    logger.info(f"Cleaned data: {len(df)} rows remaining")
    return df


def save_cleaned_data(df, output_path: str, ds: str):
    """Save cleaned DataFrame to parquet file"""
    temp_file = f"{output_path}/temp_validated_{ds}.parquet"
    df.to_parquet(temp_file)
    logger.info(f"Saved cleaned data to: {temp_file}")
    return temp_file
