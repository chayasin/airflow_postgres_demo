"""
Top Artists ETL DAG
This DAG extracts data from the warehouse schema and loads it into the mart schema.
It finds the top 10 artists by revenue.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Default arguments for the DAG
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
    dag_id='top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue into mart schema',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mart', 'artists'],
) as dag:

    # Task 1: Create the mart.top_artists table
    create_table = SQLExecuteQueryOperator(
        task_id='create_top_artists_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name VARCHAR(120),
            total_albums INTEGER,
            total_tracks INTEGER,
            total_quantity_sold INTEGER,
            total_revenue NUMERIC(10, 2),
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Truncate the table to refresh data
    truncate_table = SQLExecuteQueryOperator(
        task_id='truncate_top_artists_table',
        conn_id='postgres_default',
        sql="TRUNCATE TABLE mart.top_artists;",
    )

    # Task 3: Load top 10 artists data
    load_data = SQLExecuteQueryOperator(
        task_id='load_top_artists_data',
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
            SUM(il.quantity * il.unit_price) as total_revenue
        FROM warehouse.artist a
        JOIN warehouse.album al ON a.artist_id = al.artist_id
        JOIN warehouse.track t ON al.album_id = t.album_id
        JOIN warehouse.invoice_line il ON t.track_id = il.track_id
        GROUP BY a.artist_id, a.name
        ORDER BY total_revenue DESC
        LIMIT 10;
        """,
    )

    # Task 4: Verify the data load
    verify_data = SQLExecuteQueryOperator(
        task_id='verify_data_load',
        conn_id='postgres_default',
        sql="""
        SELECT 
            COUNT(*) as record_count,
            SUM(total_revenue) as total_revenue_sum
        FROM mart.top_artists;
        """,
    )

    # Define task dependencies
    create_table >> truncate_table >> load_data >> verify_data
