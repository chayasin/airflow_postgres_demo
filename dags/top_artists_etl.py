from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Don't retry, let it error
}

# DAG definition
with DAG(
    'top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue from warehouse to mart',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mart', 'artists'],
) as dag:

    # Task 1: Create mart table
    create_mart_table = SQLExecuteQueryOperator(
        task_id='create_mart_table',
        conn_id='postgres-local',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name VARCHAR(120),
            total_albums INTEGER,
            total_tracks INTEGER,
            total_quantity_sold INTEGER,
            total_revenue NUMERIC(10, 2),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    # Task 2: Truncate table for idempotency
    truncate_table = SQLExecuteQueryOperator(
        task_id='truncate_table',
        conn_id='postgres-local',
        sql="""
        TRUNCATE TABLE mart.top_artists
        """,
    )

    # Task 3: Load top 10 artists
    load_top_artists = SQLExecuteQueryOperator(
        task_id='load_top_artists',
        conn_id='postgres-local',
        sql="""
        INSERT INTO mart.top_artists (
            artist_id,
            artist_name,
            total_albums,
            total_tracks,
            total_quantity_sold,
            total_revenue
        )
        SELECT 
            a.artist_id,
            a.name as artist_name,
            COUNT(DISTINCT al.album_id) as total_albums,
            COUNT(DISTINCT t.track_id) as total_tracks,
            SUM(il.quantity) as total_quantity_sold,
            SUM(il.quantity * il.unit_price) as total_revenue
        FROM warehouse.invoice_line il
        JOIN warehouse.track t ON il.track_id = t.track_id
        JOIN warehouse.album al ON t.album_id = al.album_id
        JOIN warehouse.artist a ON al.artist_id = a.artist_id
        GROUP BY a.artist_id, a.name
        ORDER BY total_revenue DESC
        LIMIT 10
        """,
    )

    # Task 4: Verify data loaded
    verify_data = SQLExecuteQueryOperator(
        task_id='verify_data',
        conn_id='postgres-local',
        sql="""
        SELECT 
            COUNT(*) as record_count,
            SUM(total_revenue) as total_revenue
        FROM mart.top_artists
        """,
    )

    # Define task dependencies
    create_mart_table >> truncate_table >> load_top_artists >> verify_data
