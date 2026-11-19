# rail-data-platform
German Rail Timetable Data Platform: An Automated Data Engineering Pipeline with Python, PostgreSQL, dbt and Airflow.

## Architecture (target)
GTFS zip -> Python -> RAW (PostgreSQL / Parquet)
         -> dbt: STAGING -> (Data Vault) -> MARTS -> Metabase
Airflow orchestrates everything, running in Docker Compose.