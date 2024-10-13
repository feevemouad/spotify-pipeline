import configparser
import pathlib
import sys
from datetime import datetime
from minio import Minio
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

# Get MinIO configuration
endpoint_url = config.get('minio config', 'endpoint_url')
access_key = config.get('minio config', 'access_key')
secret_key = config.get('minio config', 'secret_key')
bucket_name = config.get('minio config', 'bucket_name')
secure = config.getboolean('minio config', 'secure')

def main():
    output_name = sys.argv[1]
    try:
        va.validate_input(output_name)
        client = connect_minio()
        create_bucket(client)
        upload_file(client, output_name)
    except Exception as e:
        print(f"Error with file input. Error {e}")
        sys.exit(1)

def connect_minio():
    """Connect to MinIO"""
    try:
        client = Minio(
            endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        return client
    except Exception as e:
        print(f"Can't connect to MinIO. Error: {e}")
        sys.exit(1)

def create_bucket(client):
    """Create MinIO Bucket if it doesn't exist"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        return client
    except Exception as e:
        print(f"Can't create MinIO Bucket. Error: {e}")
        sys.exit(1)

def upload_file(client, file_name):
    """Upload file to MinIO Bucket"""
    file_path = f"/tmp/{file_name}.csv"
    object_name = f"{file_name}.csv"
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"File uploaded successfully to MinIO bucket '{bucket_name}' with key '{object_name}'")
    except Exception as e:
        print(f"Failed to upload file to MinIO: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()