from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue into mart schema',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['etl', 'mart', 'artists'],
)

# Task 1: Create mart table
create_mart_table = SQLExecuteQueryOperator(
    task_id='create_mart_table',
    conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS mart.top_artists (
        artist_id INTEGER PRIMARY KEY,
        artist_name VARCHAR(120),
        album_count INTEGER,
        track_count INTEGER,
        total_revenue NUMERIC(10, 2),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Task 2: Truncate mart table
truncate_mart_table = SQLExecuteQueryOperator(
    task_id='truncate_mart_table',
    conn_id='postgres_default',
    sql="TRUNCATE TABLE mart.top_artists;",
    dag=dag,
)

# Task 3: Load top 10 artists data
load_top_artists = SQLExecuteQueryOperator(
    task_id='load_top_artists',
    conn_id='postgres_default',
    sql="""
    INSERT INTO mart.top_artists (artist_id, artist_name, album_count, track_count, total_revenue)
    SELECT 
        ar.artist_id,
        ar.name as artist_name,
        COUNT(DISTINCT al.album_id) as album_count,
        COUNT(DISTINCT t.track_id) as track_count,
        SUM(il.quantity * il.unit_price) as total_revenue
    FROM warehouse.invoice_line il
    JOIN warehouse.track t ON il.track_id = t.track_id
    JOIN warehouse.album al ON t.album_id = al.album_id
    JOIN warehouse.artist ar ON al.artist_id = ar.artist_id
    GROUP BY ar.artist_id, ar.name
    ORDER BY total_revenue DESC
    LIMIT 10;
    """,
    dag=dag,
)

# Task 4: Verify data load
verify_data = SQLExecuteQueryOperator(
    task_id='verify_data',
    conn_id='postgres_default',
    sql="""
    SELECT 
        COUNT(*) as record_count,
        SUM(total_revenue) as total_revenue_sum
    FROM mart.top_artists;
    """,
    dag=dag,
)

# Set task dependencies
create_mart_table >> truncate_mart_table >> load_top_artists >> verify_data
