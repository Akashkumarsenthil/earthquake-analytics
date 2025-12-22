# 🌍 Earthquake Analytics Pipeline

An end-to-end data analytics platform for USGS earthquake data with real-time ingestion, transformation, and interactive visualization.

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌────────────────┐
│   USGS API      │────▶│   Airflow    │────▶│   Snowflake     │────▶│   Streamlit    │
│  (GeoJSON Feed) │     │  (DAGs)      │     │  (Warehouse)    │     │  (Dashboard)   │
└─────────────────┘     └──────────────┘     └─────────────────┘     └────────────────┘
                              │                      │
                              │                      ▼
                              │              ┌─────────────────┐
                              └─────────────▶│      dbt        │
                                             │ (Transformations)│
                                             └─────────────────┘
```

## Project Structure

```
earthquake_pipeline/
├── dags/
│   ├── earthquake_ingestion.py      # Airflow DAGs for data ingestion
│   └── earthquake_dbt_transform.py  # Airflow DAG to run dbt
├── dbt/
│   ├── profiles.yml                 # dbt Snowflake connection
│   └── earthquake_analytics/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml      # Source definitions
│           │   ├── schema.yml       # Model documentation
│           │   └── stg_earthquakes.sql
│           └── marts/
│               ├── schema.yml
│               ├── fct_earthquakes.sql
│               ├── agg_daily_summary.sql
│               ├── agg_regional_risk.sql
│               └── agg_hourly_heatmap.sql
├── dashboard/
│   └── earthquake_dashboard.py      # Streamlit dashboard
├── setup/
│   └── snowflake_setup.sql          # Snowflake schema setup
├── requirements.txt
├── .env.template
└── README.md
```

## Setup Instructions

### 1. Snowflake Setup

1. Log into Snowflake: https://sfedu02-lvb17920.snowflakecomputing.com
2. Run the SQL script in `setup/snowflake_setup.sql` to create schemas and tables
3. Note your credentials:
   - Account: `sfedu02-lvb17920`
   - User: `PLATYPUS`
   - Database: `USER_DB_PLATYPUS`
   - Warehouse: `PLATYPUS_QUERY_WH`

### 2. Environment Setup

```bash
# Copy the env template
cp .env.template .env

# Edit .env with your Snowflake password
nano .env

# Export environment variables
source .env
# Or on Windows: set SNOWFLAKE_PASSWORD=your_password
```

### 3. Airflow Setup

Copy the DAG files to your Airflow dags folder:

```bash
cp dags/*.py ~/DATA226/sjsu-data226-FA25/dags/
```

Set up Airflow Snowflake connection:
1. Go to Airflow UI → Admin → Connections
2. Add new connection:
   - Connection Id: `snowflake_conn`
   - Connection Type: `Snowflake`
   - Host: `sfedu02-lvb17920`
   - Schema: `RAW`
   - Login: `PLATYPUS`
   - Password: `[your password]`
   - Extra: `{"account": "sfedu02-lvb17920", "warehouse": "PLATYPUS_QUERY_WH", "database": "USER_DB_PLATYPUS", "role": "ACCOUNTADMIN"}`

### 4. dbt Setup

Copy dbt project to your dbt folder:

```bash
cp -r dbt/earthquake_analytics ~/DATA226/sjsu-data226-FA25/dbt/
cp dbt/profiles.yml ~/.dbt/profiles.yml  # Or merge with existing
```

Test dbt connection:

```bash
cd ~/DATA226/sjsu-data226-FA25/dbt/earthquake_analytics
export SNOWFLAKE_PASSWORD=your_password
dbt debug
```

Run dbt models:

```bash
dbt deps
dbt run
dbt test
```

### 5. Dashboard Setup

Install dependencies:

```bash
pip install streamlit pandas plotly snowflake-connector-python
```

Run the dashboard:

```bash
cd dashboard
export SNOWFLAKE_PASSWORD=your_password
streamlit run earthquake_dashboard.py
```

The dashboard will be available at `http://localhost:8501`

## Data Pipeline Flow

### Ingestion (Airflow)

1. **Real-time DAG** (`earthquake_realtime_ingestion`):
   - Runs every 5 minutes
   - Fetches from USGS GeoJSON feed: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`
   - Uses MERGE to handle duplicates (upsert)

2. **Historical Backfill DAG** (`earthquake_historical_backfill`):
   - Manual trigger
   - Fetches from USGS FDSNWS API with date range parameters
   - Use for initial data load or backfilling gaps

### Transformation (dbt)

**Staging Layer:**
- `stg_earthquakes`: Cleans raw data, converts timestamps, adds categories

**Marts Layer:**
- `fct_earthquakes`: Incremental fact table with all earthquake events
- `agg_daily_summary`: Pre-aggregated daily statistics
- `agg_regional_risk`: Risk scores by region
- `agg_hourly_heatmap`: Temporal pattern analysis

### Visualization (Streamlit)

Dashboard features:
- 🗺️ Interactive map with earthquake locations
- 📈 Time-series analysis
- 📊 Magnitude distribution
- ⚠️ Regional risk assessment
- 🕐 Temporal heatmap patterns
- 📋 Recent earthquakes table
- 🔄 Auto-refresh option (60s)

## Key Features

| Feature | Description |
|---------|-------------|
| Real-time ingestion | 5-minute refresh from USGS |
| Incremental processing | dbt incremental models for efficiency |
| MERGE upserts | Handles updated earthquake data |
| Risk scoring | Calculated regional risk scores |
| Interactive filters | Date range, magnitude thresholds |
| Auto-refresh | Optional 60-second dashboard refresh |

## Useful Commands

```bash
# Check Airflow DAGs
airflow dags list | grep earthquake

# Trigger historical backfill with parameters
airflow dags trigger earthquake_historical_backfill \
  --conf '{"start_date": "2024-01-01", "end_date": "2024-06-30", "min_magnitude": 3.0}'

# Run specific dbt model
dbt run --select stg_earthquakes
dbt run --select fct_earthquakes

# Run all marts
dbt run --select marts

# Full refresh (rebuild incremental)
dbt run --select fct_earthquakes --full-refresh

# Generate dbt docs
dbt docs generate
dbt docs serve
```

## Troubleshooting

**Snowflake connection issues:**
```bash
# Test connection
python -c "import snowflake.connector; c = snowflake.connector.connect(account='sfedu02-lvb17920', user='PLATYPUS', password='$SNOWFLAKE_PASSWORD'); print('Connected!')"
```

**dbt model failures:**
```bash
# Check compiled SQL
cat target/compiled/earthquake_analytics/models/staging/stg_earthquakes.sql

# Run with debug
dbt run --debug
```

**Airflow task failures:**
- Check Airflow logs in UI
- Verify Snowflake connection in Admin → Connections
- Ensure USGS API is accessible

## Data Sources

- **Real-time Feed**: https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
- **Historical API**: https://earthquake.usgs.gov/fdsnws/event/1/
- **Documentation**: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php

---

**Author**: Akash | DATA226 Project | SJSU Fall 2025
