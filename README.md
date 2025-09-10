# Clinic Appointments Data Pipeline

## Quick Start
### 1. Clone the repository
```bash
git clone https://github.com/tomekzakrzewski/docplanner-appointments.git
cd docplanner-appointments
```

---

### 2. Set up environment
```bash
cp .env.example .env
```

---

### 3. Build and start services
Make sure you have **Docker** running:
```bash
docker-compose build
docker-compose up -d
```

---

### 4. Access Airflow UI
- URL: [http://localhost:8080](http://localhost:8080)  
- Username: `airflow`  
- Password: `airflow`  

---

### 5. Run tests
```bash
docker-compose --profile test run --rm test
```

---

## Pipeline workflow
1. **File discovery** - 'extract_source_file', pulling CSV file witch matching pattern 'appointments_YYYY_MM_DD.csv', for yesterdays date
2. **File validation** - 'validate_source_file', checking if csv file is corrupted, missing rows, empty
3. **Data cleaning** - 'clean_source_data',
    - normalize clinic ids (lowercase, trimmed)
    - convert timestamp to datetime format
    - remove invalid/null record
    - filter to exptected columns only
4. **Staging load** - 'load_data_to_staging', loading cleaned data to 'stg_daily_appointments' table
5. **Data quality checks** - 'staging_dq_checks',
    - row count
    - null values
    - duplicate appointment ID
6. **Load to fact** - 'load_fact_table_from_staging', 
aggregate data by clinic and date, load to final table fct_daily_appointments
7. **Reconciliation DQ check** - dq on final table, comparing sum of appointments from fact with sum of rows from staging. If passes, means aggregation was successfull.

**Tables schemas**
```
    CREATE TABLE IF NOT EXISTS stg_daily_appointments (
        appointment_id BIGINT,
        clinic_id VARCHAR(255),
        patient_id BIGINT,
        created_at DATE
    );
```
```
    CREATE TABLE IF NOT EXISTS fct_daily_appointments (
        clinic_id VARCHAR(255) NOT NULL,
        appointment_date DATE NOT NULL,
        appointments_count INTEGER,
        PRIMARY KEY (clinic_id, appointment_date)
    );
```

## Potential failures & solutions

### 1. File Discovery Issues
**Problem:** `FileNotFoundError: File: appointments_YYYY_MM_DD.csv not found`  
- **Cause:** Missing source file for the scheduled date  
- **Solutions:**  
  - Verify file naming follows exact pattern `appointments_YYYY_MM_DD.csv`  
  - Check file exists in `/opt/airflow/data/` directory  
  - Ensure file permissions allow read access  
  - For backfill runs, verify historical files are available  

---

### 2. Schema Validation Failures
**Problem:** `ValueError: Missing required columns`  
- **Cause:** CSV file missing expected columns: `appointment_id`, `clinic_id`, `patient_id`, `created_at`  
- **Solutions:**  
  - Verify CSV header matches expected schema exactly  
  - Check for typos in column names  
  - Ensure CSV is not corrupted or truncated  

---

### 3. Data Quality Check Failures
**Problem:** DQ check **row_count_check** failed  
- **Cause:** Mismatch between expected and actual row counts after cleaning  
- **Solutions:**  
  - Review data cleaning logic in `clean_appointment_data`  
  - Check for excessive null/invalid records being filtered out  
  - Verify timestamp parsing is working correctly  

**Problem:** Found duplicate `appointment_id`s  
- **Cause:** Same appointment appears multiple times  
- **Solutions:**  
  - Check source system for duplicate generation  

---

### 4. Reconciliation Check Failures
**Problem:** DQ check **check_reconciliation** failed  
- **Cause:** Fact table counts don't match staging table  
- **Solutions:**  
  - Check aggregation logic in `load_fact_table` function  
  - Verify date filtering is consistent between queries  
  - Look for timezone conversion issues  


## SQL queries
**highest average number of appointments per day**
```
SELECT 
    clinic_id,
    AVG(appointments_count) as avg_appointments_per_day
FROM fct_daily_appointments
GROUP BY clinic_id
ORDER BY avg_appointments_per_day DESC
LIMIT 1;
```
![query1](readme-utils/query-1.png)

**most appointments overall**
```
SELECT 
    TO_CHAR(appointment_date, 'Day') as day_name,
    SUM(appointments_count) as app_count
FROM fct_daily_appointments
GROUP BY day_name
ORDER BY app_count desc
LIMIT 1
```
![query2](readme-utils/query-2.png)
