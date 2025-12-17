# 🌍 Earthquake Analytics Platform

A production-grade end-to-end data engineering pipeline that ingests, transforms, and analyzes global earthquake data from the USGS API. Features real-time monitoring, historical analysis, and ML-powered risk assessment.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-green.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8.svg)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Pipeline](#-data-pipeline)
- [Machine Learning Models](#-machine-learning-models)
- [Dashboards](#-dashboards)
- [Setup & Installation](#-setup--installation)
- [Usage](#-usage)
- [Schema Design](#-schema-design)

---

## 🎯 Overview

This project demonstrates a complete modern data stack implementation for analyzing global seismic activity. It solves real-world challenges including API rate limiting, data quality issues, and scalable transformations.

### Key Highlights

| Metric | Value |
|--------|-------|
| **Total Records** | 1M+ earthquakes |
| **Historical Range** | 1900 - Present |
| **Update Frequency** | Real-time (5-min intervals) |
| **ML Models** | 4 production models |
| **Data Source** | USGS Earthquake API |

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EARTHQUAKE ANALYTICS PLATFORM                       │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────┐      ┌──────────────┐      ┌──────────────────────────────┐
     │  USGS    │      │   Apache     │      │         Snowflake            │
     │  API     │─────▶│   Airflow    │─────▶│      Data Warehouse          │
     │          │      │              │      │                              │
     └──────────┘      └──────────────┘      │  ┌─────────┐  ┌───────────┐  │
                              │              │  │   RAW   │  │ ANALYTICS │  │
                              │              │  │ Schema  │  │  Schema   │  │
                              ▼              │  └────┬────┘  └─────┬─────┘  │
                       ┌──────────────┐      │       │             │        │
                       │     dbt      │──────│───────┴─────────────┘        │
                       │  Transform   │      │                              │
                       └──────────────┘      └──────────────────────────────┘
                              │                            │
                              ▼                            ▼
                       ┌──────────────┐           ┌──────────────┐
                       │  ML Pipeline │           │  Dashboards  │
                       │  (sklearn)   │           │              │
                       └──────────────┘           │ • Streamlit  │
                              │                   │ • Preset     │
                              ▼                   └──────────────┘
                       ┌──────────────┐
                       │   ML_*      │
                       │   Tables    │
                       └──────────────┘
```

---

## ✨ Features

### 🔄 Data Ingestion
- **Auto-Chunking Pipeline**: Automatically splits large date ranges into 30-day chunks to handle USGS API's 20,000 record limit
- **MERGE/UPSERT Logic**: Safe to re-run anytime without creating duplicates
- **Fault Tolerant**: Failed chunks don't stop the pipeline; other chunks continue processing
- **Real-time Updates**: Scheduled DAG fetches new earthquakes every 5 minutes

### 🔧 Data Transformation (dbt)
- **3-Layer Architecture**: Staging → Intermediate → Marts
- **Incremental Models**: Only processes new/changed records
- **Data Quality Tests**: unique, not_null, accepted_values
- **Custom Macros**: Reusable transformations for magnitude/depth classification

### 🤖 Machine Learning
- **Magnitude Prediction**: Random Forest Regressor
- **Anomaly Detection**: Isolation Forest for unusual patterns
- **Regional Risk Clustering**: K-Means for risk zone identification
- **Seismic Forecasting**: Time series trend analysis

### 📊 Visualization
- **Streamlit Dashboard**: 6 interactive tabs with real-time data
- **Preset (Superset)**: Enterprise BI with cross-filtering
- **Interactive Maps**: Global earthquake visualization with Plotly

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Apache Airflow | DAG scheduling & monitoring |
| **Data Warehouse** | Snowflake | Cloud data storage & compute |
| **Transformation** | dbt Core | SQL-based ELT transformations |
| **Machine Learning** | scikit-learn | Predictive analytics |
| **Dashboard** | Streamlit | Interactive Python dashboard |
| **BI Tool** | Preset (Superset) | Enterprise analytics |
| **Containerization** | Docker | Local development environment |

---

## 📁 Project Structure

```
sjsu-data226-earthquake/
│
├── dags/                              # Airflow DAGs
│   ├── earthquake_ingestion_optimized.py   # Data ingestion with auto-chunking
│   ├── earthquake_dbt_transform.py         # dbt transformation trigger
│   └── earthquake_ml_pipeline.py           # ML model training & inference
│
├── dbt/                               # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_earthquakes.sql
│   │   ├── intermediate/
│   │   │   └── int_earthquakes_enriched.sql
│   │   └── marts/
│   │       ├── fct_earthquakes.sql
│   │       ├── agg_daily_summary.sql
│   │       └── agg_regional_risk.sql
│   ├── macros/
│   │   └── classify_magnitude.sql
│   ├── tests/
│   └── dbt_project.yml
│
├── dashboard/                         # Streamlit application
│   └── earthquake_dashboard.py
│
├── docker-compose.yaml                # Docker services configuration
├── requirements.txt                   # Python dependencies
└── README.md
```

---

## 🔄 Data Pipeline

### Pipeline 1: Historical Load (Auto-Chunking)

Handles the USGS API's 20,000 record limit by automatically chunking requests.

```
User Input: Date Range (1900-2025)
       │
       ▼
┌─────────────────────────────────┐
│  Generate 30-day chunks         │
│  (e.g., 1521 chunks for 125 yrs)│
└──────────────┬──────────────────┘
               │
       ┌───────▼───────┐
       │   For Each    │◀──────────────────┐
       │    Chunk      │                   │
       └───────┬───────┘                   │
               │                           │
       ┌───────▼───────┐                   │
       │  API Request  │                   │
       │  (< 20K rows) │                   │
       └───────┬───────┘                   │
               │                           │
       ┌───────▼───────┐                   │
       │    MERGE      │                   │
       │  (Upsert)     │                   │
       └───────┬───────┘                   │
               │                           │
               └───────────────────────────┘
               │
       ┌───────▼───────┐
       │   Complete    │
       │  1M+ records  │
       └───────────────┘
```

**DAG ID**: `earthquake_ingestion_optimized`  
**Schedule**: Manual trigger  
**Parameters**: start_date, end_date, min_magnitude, chunk_days

### Pipeline 2: Daily Incremental

Keeps the data current with scheduled updates.

**DAG ID**: `earthquake_daily_incremental`  
**Schedule**: Daily at 6 AM UTC  
**Scope**: Last 7 days of data

### Pipeline 3: dbt Transformation

Transforms raw data into analytics-ready tables.

**DAG ID**: `earthquake_dbt_transform`  
**Schedule**: Every 15 minutes  
**Models**: staging → intermediate → marts

### Pipeline 4: ML Pipeline

Trains and deploys machine learning models.

**DAG ID**: `earthquake_ml_pipeline`  
**Schedule**: Daily at 2 AM UTC  
**Models**: 4 ML models run in parallel

---

## 🤖 Machine Learning Models

| Model | Algorithm | Purpose | Output Table |
|-------|-----------|---------|--------------|
| **Magnitude Prediction** | Random Forest Regressor | Predict magnitude from location & depth | `ML_MAGNITUDE_PREDICTIONS` |
| **Anomaly Detection** | Isolation Forest | Detect unusual seismic patterns | `ML_EARTHQUAKE_ANOMALIES` |
| **Regional Risk Clustering** | K-Means (4 clusters) | Classify regions by risk level | `ML_REGIONAL_RISK_CLUSTERS` |
| **Seismic Forecasting** | Time Series Analysis | Predict future activity by region | `ML_SEISMIC_FORECAST` |

### Model Performance Metrics

Stored in `ML_MODEL_METRICS` table:
- MAE, RMSE, R² for regression
- Silhouette score for clustering
- Anomaly detection rate

---

## 📊 Dashboards

### Streamlit Dashboard

6 interactive tabs for comprehensive earthquake analysis:

| Tab | Features |
|-----|----------|
| **Overview** | KPIs, global map, recent earthquakes |
| **Historical** | Yearly/monthly trends, magnitude distribution |
| **Explorer** | Filter & search with data export |
| **Regions** | Regional deep dive, risk comparison |
| **ML Insights** | Model results, predictions, anomalies |
| **Pipeline** | Trigger DAGs, monitor status |

**Access**: `http://localhost:8501`

### Preset (Apache Superset)

Enterprise BI dashboard with:
- Big Number KPIs
- Time series charts
- Geographic maps (deck.gl)
- Cross-filtering capabilities
- SQL Lab for ad-hoc queries

**Access**: `https://[workspace].preset.io`

---

## 🚀 Setup & Installation

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Snowflake account
- (Optional) Preset.io account

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/earthquake-analytics.git
cd earthquake-analytics
```

### 2. Configure Environment

Create `.env` file:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=USER_DB_PLATYPUS
SNOWFLAKE_WAREHOUSE=PLATYPUS_QUERY_WH
SNOWFLAKE_ROLE=PLATYPUS_ROLE
```

### 3. Start Services

```bash
docker-compose up -d
```

### 4. Access Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | airflow / airflow |
| Streamlit | http://localhost:8501 | - |

### 5. Configure Airflow Connection

In Airflow UI → Admin → Connections → Add:

```
Connection Id: snowflake_conn
Connection Type: Snowflake
Host: your_account.snowflakecomputing.com
Login: your_user
Password: your_password
Schema: RAW
Extra: {"warehouse": "PLATYPUS_QUERY_WH", "database": "USER_DB_PLATYPUS", "role": "PLATYPUS_ROLE"}
```

---

## 📖 Usage

### Run Historical Data Load

```bash
# Trigger via Airflow UI or CLI
airflow dags trigger earthquake_ingestion_optimized \
  --conf '{"start_date": "2020-01-01", "end_date": "2025-12-31", "min_magnitude": 2.5}'
```

### Run dbt Transformations

```bash
cd dbt
dbt run
dbt test
```

### Launch Dashboard

```bash
cd dashboard
streamlit run earthquake_dashboard.py
```

---

## 🗄 Schema Design

### Star Schema

```
                    ┌─────────────────┐
                    │  DIM_REGIONS    │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│ DIM_MAGNITUDE   │──────────┼──────────│   DIM_DATE      │
│   CATEGORIES    │          │          │                 │
└─────────────────┘          │          └─────────────────┘
                             │
                    ┌────────▼────────┐
                    │                 │
                    │ FCT_EARTHQUAKES │
                    │                 │
                    │  • event_id     │
                    │  • magnitude    │
                    │  • depth_km     │
                    │  • latitude     │
                    │  • longitude    │
                    │  • region       │
                    │  • event_date   │
                    │  • risk_score   │
                    │                 │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼─────┐ ┌──────▼──────┐ ┌─────▼─────────┐
    │ AGG_DAILY     │ │ AGG_REGIONAL│ │    ML_*       │
    │ SUMMARY       │ │ RISK        │ │   TABLES      │
    └───────────────┘ └─────────────┘ └───────────────┘
```

### Table Overview

| Schema | Table | Type | Description |
|--------|-------|------|-------------|
| RAW | raw_earthquakes | Source | Landing table from API |
| ANALYTICS | fct_earthquakes | Fact | Main transformed fact table |
| ANALYTICS | agg_daily_summary | Aggregate | Daily statistics |
| ANALYTICS | agg_regional_risk | Aggregate | Regional risk metrics |
| ANALYTICS | ML_MAGNITUDE_PREDICTIONS | ML Output | Magnitude predictions |
| ANALYTICS | ML_EARTHQUAKE_ANOMALIES | ML Output | Detected anomalies |
| ANALYTICS | ML_REGIONAL_RISK_CLUSTERS | ML Output | Risk clustering results |
| ANALYTICS | ML_SEISMIC_FORECAST | ML Output | Activity forecasts |

---

## 👨‍💻 Author

**Akash Kumar**  
DATA 226 - Data Engineering  
San Jose State University  
Fall 2025

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/) for providing the data API
- San Jose State University DATA 226 course
