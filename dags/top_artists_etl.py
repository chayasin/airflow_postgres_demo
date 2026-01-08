"""
DAG to extract top 10 artists by revenue from warehouse to mart schema.
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
    'top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to load top 10 artists by revenue into mart',
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mart', 'artists'],
) as dag:

    # Task 1: Create mart table if not exists
    create_mart_table = SQLExecuteQueryOperator(
        task_id='create_mart_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name VARCHAR(120),
            total_revenue NUMERIC(10, 2),
            album_count INTEGER,
            track_count INTEGER,
            sales_count INTEGER,
            rank INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Truncate mart table
    truncate_mart_table = SQLExecuteQueryOperator(
        task_id='truncate_mart_table',
        conn_id='postgres_default',
        sql="TRUNCATE TABLE mart.top_artists;",
    )

    # Task 3: Load top 10 artists data
    load_top_artists = SQLExecuteQueryOperator(
        task_id='load_top_artists',
        conn_id='postgres_default',
        sql="""
        INSERT INTO mart.top_artists 
            (artist_id, artist_name, total_revenue, album_count, track_count, sales_count, rank)
        SELECT 
            artist_id,
            artist_name,
            total_revenue,
            album_count,
            track_count,
            sales_count,
            ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as rank
        FROM (
            SELECT 
                a.artist_id,
                a.name as artist_name,
                SUM(il.quantity * il.unit_price) as total_revenue,
                COUNT(DISTINCT al.album_id) as album_count,
                COUNT(DISTINCT t.track_id) as track_count,
                COUNT(il.invoice_line_id) as sales_count
            FROM warehouse.invoice_line il
            JOIN warehouse.track t ON il.track_id = t.track_id
            JOIN warehouse.album al ON t.album_id = al.album_id
            JOIN warehouse.artist a ON al.artist_id = a.artist_id
            GROUP BY a.artist_id, a.name
            ORDER BY total_revenue DESC
            LIMIT 10
        ) top_artists_data;
        """,
    )

    # Task 4: Data quality check
    validate_data = SQLExecuteQueryOperator(
        task_id='validate_data',
        conn_id='postgres_default',
        sql="""
        DO $$
        DECLARE
            record_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO record_count FROM mart.top_artists;
            
            IF record_count != 10 THEN
                RAISE EXCEPTION 'Data quality check failed: Expected 10 records, found %', record_count;
            END IF;
            
            RAISE NOTICE 'Data quality check passed: % records loaded', record_count;
        END $$;
        """,
    )

    # Define task dependencies
    create_mart_table >> truncate_mart_table >> load_top_artists >> validate_data
