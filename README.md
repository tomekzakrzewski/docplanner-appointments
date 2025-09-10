# Clinic Appointments Data Pipeline

## Quick Start

1. **Clone the repository:**
   git clone https://github.com/tomekzakrzewski/docplanner-appointments.git
   cd docplanner-appointments
2. **Set-up environment** 
    cp .env.example .env
3. **Build and start services:**
    Make sure you have docker running   
    docker-compose build
    docker-compose up -d
4. **Access Airflow UI:**
    URL: http://localhost:8080
    Username: airflow
    Password: airflow
5. **Run tests*
    docker-compose --profile test run --rm test


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