"""
DAG to ETL top 10 artists by revenue from warehouse to mart schema.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'top_10_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue into mart',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mart', 'artists'],
) as dag:

    # Task 1: Create mart table if not exists
    create_mart_table = SQLExecuteQueryOperator(
        task_id='create_mart_table',
        conn_id='postgres-local',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_10_artists (
            artist_id INTEGER,
            artist_name VARCHAR(120),
            album_count INTEGER,
            track_count INTEGER,
            total_tracks_sold INTEGER,
            total_revenue NUMERIC(10, 2),
            rank INTEGER,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (artist_id)
        );
        """,
    )

    # Task 2: Truncate mart table for fresh load
    truncate_mart_table = SQLExecuteQueryOperator(
        task_id='truncate_mart_table',
        conn_id='postgres-local',
        sql="TRUNCATE TABLE mart.top_10_artists;",
    )

    # Task 3: Load top 10 artists data
    load_top_10_artists = SQLExecuteQueryOperator(
        task_id='load_top_10_artists',
        conn_id='postgres-local',
        sql="""
        INSERT INTO mart.top_10_artists (
            artist_id,
            artist_name,
            album_count,
            track_count,
            total_tracks_sold,
            total_revenue,
            rank
        )
        SELECT 
            a.artist_id,
            a.name as artist_name,
            COUNT(DISTINCT al.album_id) as album_count,
            COUNT(DISTINCT t.track_id) as track_count,
            SUM(il.quantity) as total_tracks_sold,
            SUM(il.quantity * il.unit_price) as total_revenue,
            ROW_NUMBER() OVER (ORDER BY SUM(il.quantity * il.unit_price) DESC) as rank
        FROM warehouse.invoice_line il
        JOIN warehouse.track t ON il.track_id = t.track_id
        JOIN warehouse.album al ON t.album_id = al.album_id
        JOIN warehouse.artist a ON al.artist_id = a.artist_id
        GROUP BY a.artist_id, a.name
        ORDER BY total_revenue DESC
        LIMIT 10;
        """,
    )

    # Task 4: Verify data load
    verify_data_load = SQLExecuteQueryOperator(
        task_id='verify_data_load',
        conn_id='postgres-local',
        sql="""
        SELECT 
            COUNT(*) as record_count,
            SUM(total_revenue) as total_revenue,
            MAX(load_timestamp) as last_load_time
        FROM mart.top_10_artists;
        """,
    )

    # Define task dependencies
    create_mart_table >> truncate_mart_table >> load_top_10_artists >> verify_data_load
