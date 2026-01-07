"""
Top 10 Artists ETL DAG
This DAG extracts artist data from warehouse, transforms it, and loads it into mart schema
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='top_artists_etl',
    default_args=default_args,
    description='ETL pipeline to find and load top 10 artists by revenue',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['etl', 'artists', 'mart'],
)
def top_artists_etl():
    """
    DAG to extract top 10 artists data from warehouse and load to mart
    """
    
    # Task 1: Create mart table if not exists
    create_mart_table = PostgresOperator(
        task_id='create_mart_table',
        postgres_conn_id='postgres-local',
        sql="""
        CREATE TABLE IF NOT EXISTS mart.top_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name VARCHAR(120),
            album_count INTEGER,
            track_count INTEGER,
            total_revenue NUMERIC(10, 2),
            rank INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    
    # Task 2: Truncate mart table
    truncate_mart_table = PostgresOperator(
        task_id='truncate_mart_table',
        postgres_conn_id='postgres-local',
        sql="TRUNCATE TABLE mart.top_artists;"
    )
    
    # Task 3: Load top 10 artists into mart
    load_top_artists = PostgresOperator(
        task_id='load_top_artists',
        postgres_conn_id='postgres-local',
        sql="""
        INSERT INTO mart.top_artists (artist_id, artist_name, album_count, track_count, total_revenue, rank)
        SELECT 
            artist_id,
            artist_name,
            album_count,
            track_count,
            total_revenue,
            ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as rank
        FROM (
            SELECT 
                a.artist_id,
                a.name as artist_name,
                COUNT(DISTINCT al.album_id) as album_count,
                COUNT(DISTINCT t.track_id) as track_count,
                COALESCE(SUM(il.unit_price * il.quantity), 0) as total_revenue
            FROM warehouse.artist a
            LEFT JOIN warehouse.album al ON a.artist_id = al.artist_id
            LEFT JOIN warehouse.track t ON al.album_id = t.album_id
            LEFT JOIN warehouse.invoice_line il ON t.track_id = il.track_id
            GROUP BY a.artist_id, a.name
            ORDER BY total_revenue DESC
            LIMIT 10
        ) top_artists_data;
        """
    )
    
    @task
    def validate_results():
        """Validate that data was loaded correctly"""
        hook = PostgresHook(postgres_conn_id='postgres-local')
        result = hook.get_first("SELECT COUNT(*) FROM mart.top_artists;")
        count = result[0]
        print(f"✅ Loaded {count} artists into mart.top_artists")
        
        if count != 10:
            raise ValueError(f"Expected 10 artists but got {count}")
        
        # Print the top artists
        records = hook.get_records("""
            SELECT rank, artist_name, album_count, track_count, total_revenue 
            FROM mart.top_artists 
            ORDER BY rank
        """)
        
        print("\n🎵 Top 10 Artists by Revenue:")
        print("-" * 80)
        for record in records:
            rank, name, albums, tracks, revenue = record
            print(f"{rank:2d}. {name:30s} | Albums: {albums:3d} | Tracks: {tracks:3d} | Revenue: ${revenue:,.2f}")
        print("-" * 80)
        
        return count
    
    # Set task dependencies
    create_mart_table >> truncate_mart_table >> load_top_artists >> validate_results()


# Instantiate the DAG
dag_instance = top_artists_etl()
