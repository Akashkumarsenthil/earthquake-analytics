"""
Earthquake Data Ingestion DAG (Auto-Chunking)
Fetches earthquake data from USGS API and loads into Snowflake

Features:
1. Auto-chunking: Splits large date ranges into 30-day chunks (handles 20K API limit)
2. MERGE/UPSERT: Safe to re-run, no duplicates
3. Configurable: Date range and chunk size via Airflow params
4. Fault tolerant: Each chunk processed independently

Table Schema (RAW.RAW_EARTHQUAKES):
- event_id, magnitude, mag_type, place, event_time, updated_time, timezone_offset
- felt_reports, cdi, mmi, alert_level, status, tsunami_flag, significance
- network, event_code, num_stations, distance, rms, gap, event_type
- title, longitude, latitude, depth_km, detail_url, ingested_at, created_at, updated_at
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import logging
import time

# Configuration
SNOWFLAKE_CONN_ID = 'snowflake_conn'
DATABASE = 'USER_DB_PLATYPUS'
SCHEMA_RAW = 'RAW'
TABLE_NAME = 'RAW_EARTHQUAKES'

# USGS API Configuration
USGS_API_BASE = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
API_LIMIT = 20000
DEFAULT_CHUNK_DAYS = 30

default_args = {
    'owner': 'akash',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def get_snowflake_connection():
    """Get Snowflake connection using Airflow hook"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()


def create_raw_table(**context):
    """Verify raw earthquakes table exists"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
    
    # Check if table exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{SCHEMA_RAW}' AND TABLE_NAME = '{TABLE_NAME}'
    """)
    table_exists = cursor.fetchone()[0] > 0
    
    if table_exists:
        logging.info(f"Table {TABLE_NAME} exists - using existing schema")
    else:
        logging.error(f"Table {TABLE_NAME} does not exist!")
        raise Exception(f"Table {DATABASE}.{SCHEMA_RAW}.{TABLE_NAME} not found")
    
    conn.commit()
    cursor.close()
    conn.close()


def generate_date_chunks(start_date, end_date, chunk_days=DEFAULT_CHUNK_DAYS):
    """Split date range into smaller chunks to stay under API limit."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    chunks = []
    current_start = start
    
    while current_start < end:
        current_end = min(current_start + timedelta(days=chunk_days), end)
        chunks.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        current_start = current_end + timedelta(days=1)
    
    logging.info(f"Generated {len(chunks)} chunks for date range {start_date} to {end_date}")
    return chunks


def fetch_earthquakes_from_api(start_date, end_date, min_magnitude=2.5):
    """Fetch earthquake data from USGS API for a specific date range."""
    params = {
        'format': 'geojson',
        'starttime': start_date,
        'endtime': end_date,
        'minmagnitude': min_magnitude,
        'limit': API_LIMIT,
        'orderby': 'time'
    }
    
    try:
        response = requests.get(USGS_API_BASE, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        features = data.get('features', [])
        logging.info(f"Fetched {len(features)} earthquakes for {start_date} to {end_date}")
        
        return features
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {start_date} to {end_date}: {e}")
        raise


def parse_earthquake_record(feature):
    """
    Parse a single earthquake feature into a database record.
    Maps USGS API fields to your existing table columns.
    """
    props = feature.get('properties', {})
    geom = feature.get('geometry', {})
    coords = geom.get('coordinates', [None, None, None])
    
    # Map to your exact column order:
    # event_id, magnitude, mag_type, place, event_time, updated_time, timezone_offset,
    # felt_reports, cdi, mmi, alert_level, status, tsunami_flag, significance,
    # network, event_code, num_stations, distance, rms, gap, event_type,
    # title, longitude, latitude, depth_km, detail_url
    
    return (
        feature.get('id'),                              # event_id
        props.get('mag'),                               # magnitude
        props.get('magType'),                           # mag_type
        props.get('place'),                             # place
        props.get('time'),                              # event_time (unix ms)
        props.get('updated'),                           # updated_time (unix ms)
        props.get('tz'),                                # timezone_offset
        props.get('felt'),                              # felt_reports
        props.get('cdi'),                               # cdi
        props.get('mmi'),                               # mmi
        props.get('alert'),                             # alert_level
        props.get('status'),                            # status
        props.get('tsunami'),                           # tsunami_flag
        props.get('sig'),                               # significance
        props.get('net'),                               # network
        props.get('code'),                              # event_code
        props.get('nst'),                               # num_stations
        props.get('dmin'),                              # distance
        props.get('rms'),                               # rms
        props.get('gap'),                               # gap
        props.get('type'),                              # event_type
        props.get('title'),                             # title
        coords[0] if len(coords) > 0 else None,        # longitude
        coords[1] if len(coords) > 1 else None,        # latitude
        coords[2] if len(coords) > 2 else None,        # depth_km
        props.get('detail')                             # detail_url
    )


def load_chunk_to_snowflake(records, cursor):
    """
    Load a chunk of records into Snowflake using MERGE (upsert).
    Matches your exact table schema.
    """
    if not records:
        return 0
    
    # Create temp staging table matching your exact schema
    cursor.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_earthquakes (
            event_id VARCHAR(50),
            magnitude FLOAT,
            mag_type VARCHAR(10),
            place VARCHAR(500),
            event_time NUMBER,
            updated_time NUMBER,
            timezone_offset NUMBER,
            felt_reports NUMBER,
            cdi FLOAT,
            mmi FLOAT,
            alert_level VARCHAR(20),
            status VARCHAR(20),
            tsunami_flag NUMBER,
            significance NUMBER,
            network VARCHAR(10),
            event_code VARCHAR(20),
            num_stations NUMBER,
            distance FLOAT,
            rms FLOAT,
            gap FLOAT,
            event_type VARCHAR(50),
            title VARCHAR(500),
            longitude FLOAT,
            latitude FLOAT,
            depth_km FLOAT,
            detail_url VARCHAR(1000)
        )
    """)
    
    # Clear temp table
    cursor.execute("DELETE FROM temp_earthquakes")
    
    # Insert into temp table - 26 columns
    insert_sql = """
        INSERT INTO temp_earthquakes 
        (event_id, magnitude, mag_type, place, event_time, updated_time, timezone_offset,
         felt_reports, cdi, mmi, alert_level, status, tsunami_flag, significance,
         network, event_code, num_stations, distance, rms, gap, event_type,
         title, longitude, latitude, depth_km, detail_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Batch insert
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
    
    # MERGE into main table (upsert) - matches your exact schema
    merge_sql = f"""
        MERGE INTO {TABLE_NAME} AS target
        USING temp_earthquakes AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET
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
        WHEN NOT MATCHED THEN INSERT 
            (event_id, magnitude, mag_type, place, event_time, updated_time, timezone_offset,
             felt_reports, cdi, mmi, alert_level, status, tsunami_flag, significance,
             network, event_code, num_stations, distance, rms, gap, event_type,
             title, longitude, latitude, depth_km, detail_url)
        VALUES 
            (source.event_id, source.magnitude, source.mag_type, source.place,
             source.event_time, source.updated_time, source.timezone_offset,
             source.felt_reports, source.cdi, source.mmi, source.alert_level,
             source.status, source.tsunami_flag, source.significance,
             source.network, source.event_code, source.num_stations, source.distance,
             source.rms, source.gap, source.event_type, source.title,
             source.longitude, source.latitude, source.depth_km, source.detail_url)
    """
    
    cursor.execute(merge_sql)
    
    return len(records)


def ingest_earthquake_data(**context):
    """
    Main ingestion function with auto-chunking.
    Fetches data in chunks to handle USGS API 20K limit.
    """
    params = context.get('params', {})
    start_date = params.get('start_date', '2020-01-01')
    end_date = params.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    min_magnitude = params.get('min_magnitude', 2.5)
    chunk_days = params.get('chunk_days', DEFAULT_CHUNK_DAYS)
    
    logging.info(f"=== Starting Earthquake Ingestion ===")
    logging.info(f"Date Range: {start_date} to {end_date}")
    logging.info(f"Min Magnitude: {min_magnitude}")
    logging.info(f"Chunk Size: {chunk_days} days")
    
    chunks = generate_date_chunks(start_date, end_date, chunk_days)
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
    
    total_records = 0
    successful_chunks = 0
    failed_chunks = 0
    
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        try:
            logging.info(f"Processing chunk {i+1}/{len(chunks)}: {chunk_start} to {chunk_end}")
            
            features = fetch_earthquakes_from_api(chunk_start, chunk_end, min_magnitude)
            
            if features:
                records = [parse_earthquake_record(f) for f in features]
                records = [r for r in records if r[0] is not None]  # Filter null event_id
                
                rows_affected = load_chunk_to_snowflake(records, cursor)
                conn.commit()
                
                total_records += len(records)
                successful_chunks += 1
                
                logging.info(f"Chunk {i+1} complete: {len(records)} records processed")
            else:
                logging.info(f"Chunk {i+1}: No records found")
                successful_chunks += 1
            
            if i < len(chunks) - 1:
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            logging.error(f"Error processing chunk {i+1} ({chunk_start} to {chunk_end}): {e}")
            failed_chunks += 1
            continue
    
    cursor.close()
    conn.close()
    
    logging.info(f"=== Ingestion Complete ===")
    logging.info(f"Total Records: {total_records}")
    logging.info(f"Successful Chunks: {successful_chunks}/{len(chunks)}")
    logging.info(f"Failed Chunks: {failed_chunks}")
    
    if failed_chunks > 0:
        logging.warning(f"{failed_chunks} chunks failed - consider re-running")
    
    return {
        'total_records': total_records,
        'successful_chunks': successful_chunks,
        'failed_chunks': failed_chunks
    }


def get_ingestion_stats(**context):
    """Get current table statistics"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
    
    cursor.execute(f"""
        SELECT 
            COUNT(*) as total_records,
            MIN(TO_TIMESTAMP(event_time/1000)) as earliest_event,
            MAX(TO_TIMESTAMP(event_time/1000)) as latest_event,
            MIN(magnitude) as min_magnitude,
            MAX(magnitude) as max_magnitude,
            COUNT(DISTINCT DATE(TO_TIMESTAMP(event_time/1000))) as unique_days
        FROM {TABLE_NAME}
        WHERE event_time IS NOT NULL
    """)
    
    result = cursor.fetchone()
    
    stats = {
        'total_records': result[0],
        'earliest_event': str(result[1]),
        'latest_event': str(result[2]),
        'min_magnitude': result[3],
        'max_magnitude': result[4],
        'unique_days': result[5]
    }
    
    cursor.close()
    conn.close()
    
    logging.info(f"=== Table Statistics ===")
    for key, value in stats.items():
        logging.info(f"{key}: {value}")
    
    return stats


# =============================================================================
# DAG Definition - Historical Load (Manual Trigger)
# =============================================================================
with DAG(
    dag_id='earthquake_ingestion_optimized',
    default_args=default_args,
    description='Earthquake data ingestion with auto-chunking to handle USGS 20K API limit',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'start_date': '2020-01-01',
        'end_date': datetime.now().strftime('%Y-%m-%d'),
        'min_magnitude': 2.5,
        'chunk_days': 30
    },
    tags=['earthquake', 'ingestion', 'api', 'snowflake', 'project']
) as dag:
    
    create_table = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_raw_table,
    )
    
    ingest_data = PythonOperator(
        task_id='ingest_earthquake_data',
        python_callable=ingest_earthquake_data,
    )
    
    get_stats = PythonOperator(
        task_id='get_ingestion_stats',
        python_callable=get_ingestion_stats,
    )
    
    create_table >> ingest_data >> get_stats


# =============================================================================
# DAG Definition - Daily Incremental (Scheduled)
# =============================================================================
with DAG(
    dag_id='earthquake_daily_incremental',
    default_args=default_args,
    description='Daily incremental earthquake data ingestion (last 7 days)',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['earthquake', 'ingestion', 'daily', 'incremental', 'project']
) as dag_daily:
    
    def ingest_incremental(**context):
        """Ingest last 7 days of data"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        context['params'] = {
            'start_date': start_date,
            'end_date': end_date,
            'min_magnitude': 2.5,
            'chunk_days': 7
        }
        
        return ingest_earthquake_data(**context)
    
    create_table_daily = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_raw_table,
    )
    
    ingest_incremental_task = PythonOperator(
        task_id='ingest_incremental_data',
        python_callable=ingest_incremental,
    )
    
    get_stats_daily = PythonOperator(
        task_id='get_ingestion_stats',
        python_callable=get_ingestion_stats,
    )
    
    create_table_daily >> ingest_incremental_task >> get_stats_daily