from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue from warehouse to mart',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mart', 'artists'],
) as dag:

    # Task 1: Create the mart table if it doesn't exist
    create_mart_table = SQLExecuteQueryOperator(
        task_id='create_mart_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_artists (
            artist_id INTEGER,
            artist_name VARCHAR(120),
            total_albums INTEGER,
            total_tracks INTEGER,
            total_quantity_sold INTEGER,
            total_revenue NUMERIC(10, 2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (artist_id)
        );
        """,
    )

    # Task 2: Truncate the mart table to ensure clean data load
    truncate_mart_table = SQLExecuteQueryOperator(
        task_id='truncate_mart_table',
        conn_id='postgres_default',
        sql="TRUNCATE TABLE mart.top_artists;",
    )

    # Task 3: Load top 10 artists data into mart
    load_top_artists = SQLExecuteQueryOperator(
        task_id='load_top_artists',
        conn_id='postgres_default',
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
            ROUND(SUM(il.quantity * il.unit_price)::numeric, 2) as total_revenue
        FROM warehouse.invoice_line il
        JOIN warehouse.track t ON il.track_id = t.track_id
        JOIN warehouse.album al ON t.album_id = al.album_id
        JOIN warehouse.artist a ON al.artist_id = a.artist_id
        GROUP BY a.artist_id, a.name
        ORDER BY total_revenue DESC
        LIMIT 10;
        """,
    )

    # Task 4: Verify data was loaded
    verify_data = SQLExecuteQueryOperator(
        task_id='verify_data',
        conn_id='postgres_default',
        sql="""
        SELECT 
            COUNT(*) as record_count,
            SUM(total_revenue) as total_revenue_sum
        FROM mart.top_artists;
        """,
    )

    # Set task dependencies
    create_mart_table >> truncate_mart_table >> load_top_artists >> verify_data
