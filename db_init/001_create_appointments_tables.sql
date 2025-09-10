    CREATE TABLE IF NOT EXISTS stg_daily_appointments (
        appointment_id BIGINT,
        clinic_id VARCHAR(255),
        patient_id BIGINT,
        created_at DATE
    );


    CREATE TABLE IF NOT EXISTS fct_daily_appointments (
        clinic_id VARCHAR(255) NOT NULL,
        appointment_date DATE NOT NULL,
        appointments_count INTEGER,
        PRIMARY KEY (clinic_id, appointment_date)
    );
