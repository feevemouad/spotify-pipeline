from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

"""
DAG to extract Spotify data, load into MinIO, and copy to PostgresDB
"""

# Output name of extracted file
output_name = datetime.now().strftime("%Y%m%d")

# DAG configuration
schedule_interval = "@daily"
start_date = days_ago(1)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_pipeline_ETL_minio_postgres",
    description="Spotify ETL using MinIO and PostgresDB",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["SpotifyETL", "MinIO", "PostgresDB"],
) as dag:
    
    extract_spotify_data = BashOperator(
        task_id="extract_spotify_data",
        bash_command=f"python /opt/airflow/extraction/spotify_data_extraction.py {output_name}",
        dag=dag,
    )
    extract_spotify_data.doc_md = "Extract Spotify data and store as CSV"

    upload_to_minio = BashOperator(
        task_id="upload_to_minio",
        bash_command=f"python /opt/airflow/extraction/minio_connect_create_load.py {output_name}",
        dag=dag,
    )
    upload_to_minio.doc_md = "Upload Spotify CSV data to MinIO bucket"
    
    minio_to_postgresdb = BashOperator(
        task_id="minio_to_duckdb",
        bash_command=f"python /opt/airflow/extraction/minio_to_postgres.py {output_name}",
        dag=dag,
    )
    minio_to_postgresdb.doc_md = "Copy MinIO CSV file to Postgres table"

    extract_spotify_data >> upload_to_minio >> minio_to_postgresdb