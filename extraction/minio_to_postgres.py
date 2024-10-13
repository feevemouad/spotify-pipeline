import configparser
import pathlib
import sys
from datetime import datetime
import pandas as pd
from minio import Minio
import io
import psycopg2
from sqlalchemy import create_engine
import validation as va

# Read Configuration File
config = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "config.conf"
config_file_path = script_path / config_file
if config_file_path.exists():
    config.read(f"{script_path}/{config_file}")
else:
    print(f"Config file '{config_file_path}' does not exist.")

# Get configurations
minio_endpoint = config.get('minio config', 'endpoint_url')
minio_access_key = config.get('minio config', 'access_key')
minio_secret_key = config.get('minio config', 'secret_key')
minio_bucket = config.get('minio config', 'bucket_name')
minio_secure = config.getboolean('minio config', 'secure')

pg_host = config.get('postgres_dw config', 'host')
pg_port = config.get('postgres_dw config', 'port')
pg_database = config.get('postgres_dw config', 'database')
pg_user = config.get('postgres_dw config', 'user')
pg_password = config.get('postgres_dw config', 'password')

def main():
    output_name = sys.argv[1]
    try:
        va.validate_input(output_name)
        copy_to_postgres(output_name)
    except Exception as e:
        print(f"Error {e}")
        sys.exit(1)

def copy_to_postgres(file_name):
    try:
        # Connect to MinIO
        minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure
        )

        # Get object from MinIO
        object_name = f"{file_name}.csv"
        data = minio_client.get_object(minio_bucket, object_name)
        
        # Read CSV data into pandas DataFrame
        df = pd.read_csv(io.BytesIO(data.read()))
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
        
        # Create table if it doesn't exist
        with engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS spotify_tracks (
                    track VARCHAR,
                    artist VARCHAR,
                    danceability DOUBLE PRECISION,
                    energy DOUBLE PRECISION,
                    key INTEGER,
                    loudness DOUBLE PRECISION,
                    mode INTEGER,
                    speechiness DOUBLE PRECISION,
                    acousticness DOUBLE PRECISION,
                    instrumentalness DOUBLE PRECISION,
                    liveness DOUBLE PRECISION,
                    valence DOUBLE PRECISION,
                    tempo DOUBLE PRECISION,
                    type VARCHAR,
                    id VARCHAR,
                    uri VARCHAR,
                    track_href VARCHAR,
                    analysis_url VARCHAR,
                    duration_ms INTEGER,
                    time_signature INTEGER,
                    genres VARCHAR
                )
            """)
        
        # Insert data into PostgreSQL
        df.to_sql('spotify_tracks', engine, if_exists='append', index=False)
        
        print("Data copied to PostgreSQL successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()