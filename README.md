# Spotify Data Pipeline

## Project Overview
This project is a data pipeline designed to extract, process, and analyze data from Spotify, storing it in a data warehouse and visualizing it on a dashboard. The pipeline uses Apache Airflow for orchestration, MinIO for data storage, PostgreSQL as a data warehouse, dbt for data transformation, and Metabase for visualization. All services are deployed using Docker, making the setup simple and portable.

![Capture d'écran 2024-10-31 214636](https://github.com/user-attachments/assets/e78b38dd-e340-4ce3-bbe7-1b88d9828d5c)

### Key Components:
1. **Spotify Data Extraction**: Fetches data from Spotify using their API.
2. **Data Storage**: Stores raw data in MinIO, an S3-compatible object storage service.
3. **Data Warehousing**: Loads the data from MinIO into PostgreSQL for further analysis.
4. **Data Transformation**: Uses dbt to clean, transform, and organize the data within PostgreSQL.
5. **Data Visualization**: Displays the final results on a Metabase dashboard for insights.

## Getting Started

1. Clone the repository:

    ```bash
    git clone https://github.com/feevemouad/spotify-pipeline.git
    cd spotify-pipeline
    ```

2. Configure the `config.conf` file to add API tokens and credentials:

    ```ini
    [spotify config]
    client_id = [client_id]
    secret = [secret]
    playlist_id = [playlist_id]
    ```

3. Build and start the services with Docker Compose:

    ```bash
    docker-compose up -d --build
    ```

4. Once the containers have started, you can access the following services:

   - **Airflow UI**: [http://localhost:9090](http://localhost:9090)
     - Log in with the default credentials:
       - Username: admin
       - Password: admin
     - Start the DAG to begin data processing.

   - **Metabase UI**: [http://localhost:3000](http://localhost:3000)
     - Log in with the default credentials:
       - Username: admin@example.com
       - Password: admin
     - Explore the preconfigured dashboard to visualize Spotify data insights.

Here’s an example of a dashboard created for this project:

![Capture d'écran 2024-11-05 015904](https://github.com/user-attachments/assets/eb16229c-30ce-4000-9f63-b32f941b40a3)
![Capture d'écran 2024-11-05 015930](https://github.com/user-attachments/assets/4d2627e9-4c48-420c-99f5-2449757ece0d)

You can adapt this pipeline to track any playlist or genre, transforming the extracted data into meaningful insights for music analysis.
