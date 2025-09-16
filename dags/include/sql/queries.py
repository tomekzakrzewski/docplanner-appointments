DB_QUERIES = {
    "delete_stg_by_date": """
        DELETE FROM {{ params.stg_table }}
        WHERE created_at = '{{ ds }}'
    """,
    "delete_agg_by_date": """
        DELETE FROM {{ params.agg_table }}
        WHERE appointment_date = '{{ ds }}'
    """,
    "insert_to_agg": """
        INSERT INTO {{ params.agg_table }} (clinic_id, appointment_date, appointments_count)
        SELECT
            clinic_id,
            DATE(created_at) as appointment_date,
            COUNT(*) as appointments_count
        FROM {{ params.stg_table }}
        WHERE DATE(created_at) = '{{ ds }}'
        GROUP BY clinic_id, DATE(created_at)
    """,
    "insert_data_agg": """
        DELETE FROM {{ params.agg_table }}
        WHERE appointment_date = '{{ ds }}';
        
        INSERT INTO {{ params.agg_table }} (clinic_id, appointment_date, appointments_count)
        SELECT
            clinic_id,
            DATE(created_at) as appointment_date,
            COUNT(*) as appointments_count
        FROM {{ params.stg_table }}
        WHERE DATE(created_at) = '{{ ds }}'
        GROUP BY clinic_id, DATE(created_at);
    """,
}


STAGING_DQ_CHECKS = {
    "row_count_check": """
        SELECT COUNT(*) = {{ ti.xcom_pull(task_ids='load_stg_daily_appointments') }}
        FROM {{ params.stg_table }}
        WHERE created_at = '{{ ds }}'
    """,
    "null_check": """
        SELECT COUNT(*) = 0
        FROM {{ params.stg_table }}
        WHERE created_at = '{{ ds }}'
            AND (appointment_id IS NULL OR clinic_id IS NULL)
    """,
    "duplicate_check": """
        SELECT NOT EXISTS (
            SELECT 1
            FROM {{ params.stg_table }}
            WHERE created_at = '{{ ds }}'
            GROUP BY appointment_id
            HAVING COUNT(*) > 1
        )
    """,
}

AGG_DQ_CHECKS = {
    "comparison_check": """
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
        ) = 0
    """,
}
