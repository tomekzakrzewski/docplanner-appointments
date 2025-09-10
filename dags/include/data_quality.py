# src/data_quality.py
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_dq_checks(table_name: str, expected_row_count: int):
    """Define data quality checks for staging table"""
    return [
        {
            "name": "row_count_check",
            "sql": f"SELECT COUNT(*) FROM {table_name} WHERE created_at = :ds",
            "test": lambda result: result == expected_row_count,
            "error": f"Expected {expected_row_count} rows, got {{result}}",
        },
        {
            "name": "null_check",
            "sql": f"SELECT COUNT(*) FROM {table_name} WHERE created_at = :ds AND (appointment_id IS NULL OR clinic_id IS NULL)",
            "test": lambda result: result == 0,
            "error": "Found NULL values in key columns",
        },
        {
            "name": "duplicate_check",
            "sql": f"SELECT COUNT(*) FROM (SELECT 1 FROM {table_name} WHERE created_at = :ds GROUP BY appointment_id HAVING COUNT(*) > 1) d",
            "test": lambda result: result == 0,
            "error": "Found duplicate appointment_ids",
        },
    ]


def run_dq_checks(checks: list, conn, ds: str):
    """Execute data quality checks and return failed checks"""
    logger.info(f"Running {len(checks)} data quality checks for date: {ds}")

    failed_checks = []

    for check in checks:
        name = check["name"]
        sql = check["sql"]

        logger.info(f"Running DQ check: {name}")
        result = conn.execute(text(sql), {"ds": ds}).scalar()

        if not check["test"](result):
            error_message = (
                f"DQ check '{name}' failed: {check['error'].format(result=result)}"
            )
            logger.error(error_message)
            failed_checks.append(error_message)
        else:
            logger.info(f"DQ check '{name}' passed")

    logger.info(f"DQ checks completed. Failed: {len(failed_checks)}")
    return failed_checks
