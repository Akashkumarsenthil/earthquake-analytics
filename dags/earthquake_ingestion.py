"""
Earthquake Data Ingestion DAG
Fetches real-time earthquake data from USGS and loads to Snowflake
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import logging

# Configuration
SNOWFLAKE_CONN_ID = 'snowflake_conn'  # Set this up in Airflow connections
DATABASE = 'USER_DB_PLATYPUS'
SCHEMA_RAW = 'RAW'
SCHEMA_ANALYTICS = 'ANALYTICS'

# USGS API endpoints
USGS_REALTIME_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
USGS_HISTORICAL_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

default_args = {
    'owner': 'akash',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def fetch_realtime_earthquakes(**context):
    """Fetch real-time earthquake data from USGS GeoJSON feed"""
    logging.info(f"Fetching real-time data from: {USGS_REALTIME_URL}")
    
    response = requests.get(USGS_REALTIME_URL, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    features = data.get('features', [])
    
    logging.info(f"Fetched {len(features)} earthquake events")
    
    # Transform GeoJSON to flat records
    records = []
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None, None])
        
        record = {
            'event_id': feature.get('id'),
            'magnitude': props.get('mag'),
            'mag_type': props.get('magType'),
            'place': props.get('place'),
            'event_time': props.get('time'),  # Unix timestamp in milliseconds
            'updated_time': props.get('updated'),
            'timezone_offset': props.get('tz'),
            'felt_reports': props.get('felt'),
            'cdi': props.get('cdi'),  # Community intensity
            'mmi': props.get('mmi'),  # Modified Mercalli Intensity
            'alert_level': props.get('alert'),
            'status': props.get('status'),
            'tsunami_flag': props.get('tsunami'),
            'significance': props.get('sig'),
            'network': props.get('net'),
            'event_code': props.get('code'),
            'num_stations': props.get('nst'),
            'distance': props.get('dmin'),  # Horizontal distance to nearest station
            'rms': props.get('rms'),  # Root mean square travel time residual
            'gap': props.get('gap'),  # Azimuthal gap
            'event_type': props.get('type'),
            'title': props.get('title'),
            'longitude': coords[0] if len(coords) > 0 else None,
            'latitude': coords[1] if len(coords) > 1 else None,
            'depth_km': coords[2] if len(coords) > 2 else None,
            'detail_url': props.get('detail'),
            'ingested_at': datetime.utcnow().isoformat()
        }
        records.append(record)
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='earthquake_records', value=records)
    context['ti'].xcom_push(key='record_count', value=len(records))
    
    return len(records)


def load_to_snowflake(**context):
    """Load earthquake records to Snowflake RAW schema using MERGE"""
    records = context['ti'].xcom_pull(key='earthquake_records', task_ids='fetch_earthquakes')
    
    if not records:
        logging.warning("No records to load")
        return 0
    
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Create staging table and load data
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        
        # Use the database and schema
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
        
        # Create raw table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_earthquakes (
            event_id VARCHAR(50) PRIMARY KEY,
            magnitude FLOAT,
            mag_type VARCHAR(10),
            place VARCHAR(500),
            event_time BIGINT,
            updated_time BIGINT,
            timezone_offset INT,
            felt_reports INT,
            cdi FLOAT,
            mmi FLOAT,
            alert_level VARCHAR(20),
            status VARCHAR(20),
            tsunami_flag INT,
            significance INT,
            network VARCHAR(10),
            event_code VARCHAR(20),
            num_stations INT,
            distance FLOAT,
            rms FLOAT,
            gap FLOAT,
            event_type VARCHAR(50),
            title VARCHAR(500),
            longitude FLOAT,
            latitude FLOAT,
            depth_km FLOAT,
            detail_url VARCHAR(1000),
            ingested_at TIMESTAMP_NTZ,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_sql)
        
        # Create temp staging table
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE raw_earthquakes_staging 
            LIKE raw_earthquakes
        """)
        
        # Insert into staging
        insert_sql = """
        INSERT INTO raw_earthquakes_staging (
            event_id, magnitude, mag_type, place, event_time, updated_time,
            timezone_offset, felt_reports, cdi, mmi, alert_level, status,
            tsunami_flag, significance, network, event_code, num_stations,
            distance, rms, gap, event_type, title, longitude, latitude,
            depth_km, detail_url, ingested_at
        ) VALUES (
            %(event_id)s, %(magnitude)s, %(mag_type)s, %(place)s, %(event_time)s,
            %(updated_time)s, %(timezone_offset)s, %(felt_reports)s, %(cdi)s,
            %(mmi)s, %(alert_level)s, %(status)s, %(tsunami_flag)s, %(significance)s,
            %(network)s, %(event_code)s, %(num_stations)s, %(distance)s, %(rms)s,
            %(gap)s, %(event_type)s, %(title)s, %(longitude)s, %(latitude)s,
            %(depth_km)s, %(detail_url)s, %(ingested_at)s
        )
        """
        
        for record in records:
            cursor.execute(insert_sql, record)
        
        # MERGE to handle duplicates (upsert)
        merge_sql = """
        MERGE INTO raw_earthquakes AS target
        USING raw_earthquakes_staging AS source
        ON target.event_id = source.event_id
        WHEN MATCHED AND source.updated_time > target.updated_time THEN UPDATE SET
            magnitude = source.magnitude,
            mag_type = source.mag_type,
            place = source.place,
            event_time = source.event_time,
            updated_time = source.updated_time,
            timezone_offset = source.timezone_offset,
            felt_reports = source.felt_reports,
            cdi = source.cdi,
            mmi = source.mmi,
            alert_level = source.alert_level,
            status = source.status,
            tsunami_flag = source.tsunami_flag,
            significance = source.significance,
            network = source.network,
            event_code = source.event_code,
            num_stations = source.num_stations,
            distance = source.distance,
            rms = source.rms,
            gap = source.gap,
            event_type = source.event_type,
            title = source.title,
            longitude = source.longitude,
            latitude = source.latitude,
            depth_km = source.depth_km,
            detail_url = source.detail_url,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            event_id, magnitude, mag_type, place, event_time, updated_time,
            timezone_offset, felt_reports, cdi, mmi, alert_level, status,
            tsunami_flag, significance, network, event_code, num_stations,
            distance, rms, gap, event_type, title, longitude, latitude,
            depth_km, detail_url, ingested_at
        ) VALUES (
            source.event_id, source.magnitude, source.mag_type, source.place,
            source.event_time, source.updated_time, source.timezone_offset,
            source.felt_reports, source.cdi, source.mmi, source.alert_level,
            source.status, source.tsunami_flag, source.significance, source.network,
            source.event_code, source.num_stations, source.distance, source.rms,
            source.gap, source.event_type, source.title, source.longitude,
            source.latitude, source.depth_km, source.detail_url, source.ingested_at
        )
        """
        
        result = cursor.execute(merge_sql)
        logging.info(f"MERGE completed. Records processed: {len(records)}")
        
        conn.commit()
    
    return len(records)


def fetch_historical_earthquakes(**context):
    """Fetch historical earthquake data for backfill"""
    # Get date range from DAG run conf or use defaults
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    start_date = conf.get('start_date', '2024-01-01')
    end_date = conf.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    min_magnitude = conf.get('min_magnitude', 2.5)
    
    params = {
        'format': 'geojson',
        'starttime': start_date,
        'endtime': end_date,
        'minmagnitude': min_magnitude,
        'orderby': 'time-asc',
        'limit': 20000  # Max allowed by USGS
    }
    
    logging.info(f"Fetching historical data: {start_date} to {end_date}, min mag: {min_magnitude}")
    
    response = requests.get(USGS_HISTORICAL_URL, params=params, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    features = data.get('features', [])
    
    logging.info(f"Fetched {len(features)} historical earthquake events")
    
    # Same transformation as realtime
    records = []
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [None, None, None])
        
        record = {
            'event_id': feature.get('id'),
            'magnitude': props.get('mag'),
            'mag_type': props.get('magType'),
            'place': props.get('place'),
            'event_time': props.get('time'),
            'updated_time': props.get('updated'),
            'timezone_offset': props.get('tz'),
            'felt_reports': props.get('felt'),
            'cdi': props.get('cdi'),
            'mmi': props.get('mmi'),
            'alert_level': props.get('alert'),
            'status': props.get('status'),
            'tsunami_flag': props.get('tsunami'),
            'significance': props.get('sig'),
            'network': props.get('net'),
            'event_code': props.get('code'),
            'num_stations': props.get('nst'),
            'distance': props.get('dmin'),
            'rms': props.get('rms'),
            'gap': props.get('gap'),
            'event_type': props.get('type'),
            'title': props.get('title'),
            'longitude': coords[0] if len(coords) > 0 else None,
            'latitude': coords[1] if len(coords) > 1 else None,
            'depth_km': coords[2] if len(coords) > 2 else None,
            'detail_url': props.get('detail'),
            'ingested_at': datetime.utcnow().isoformat()
        }
        records.append(record)
    
    context['ti'].xcom_push(key='earthquake_records', value=records)
    return len(records)




# DAG 1: Real-time Ingestion (runs every 5 minutes)


with DAG(
    dag_id='earthquake_realtime_ingestion',
    default_args=default_args,
    description='Ingest real-time earthquake data from USGS every 5 minutes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['earthquake', 'realtime', 'usgs', 'project']
) as dag_realtime:
    
    fetch_task = PythonOperator(
        task_id='fetch_earthquakes',
        python_callable=fetch_realtime_earthquakes,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True
    )
    
    fetch_task >> load_task



# DAG 2: Historical Backfill (manual trigger)



with DAG(
    dag_id='earthquake_historical_backfill',
    default_args=default_args,
    description='Backfill historical earthquake data from USGS',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['earthquake', 'historical', 'backfill', 'project'],
    params={
        'start_date': '2024-01-01',
        'end_date': '2024-12-01',
        'min_magnitude': 2.5
    }
) as dag_historical:
    
    fetch_historical_task = PythonOperator(
        task_id='fetch_historical_earthquakes',
        python_callable=fetch_historical_earthquakes,
        provide_context=True
    )
    
    load_historical_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True
    )
    
    fetch_historical_task >> load_historical_task
