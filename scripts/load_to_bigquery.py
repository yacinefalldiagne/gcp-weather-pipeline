import os
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime

# Support for .env file
try:
    from dotenv import load_dotenv
    project_root = Path(__file__).parent.parent
    load_dotenv(project_root / '.env')
except ImportError:
    print("python-dotenv not installed. Using system environment variables only.")

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "weather_data")
BIGQUERY_TABLE = "weather_raw"

def create_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        client.get_dataset(dataset_id)
        print(f" Dataset {BIGQUERY_DATASET} already exists")
    except Exception:
        # Create dataset
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        dataset.description = "Weather data pipeline dataset"
        
        dataset = client.create_dataset(dataset, timeout=30)
        print(f" Created dataset {dataset.project}.{dataset.dataset_id}")

def create_table():
    """Create BigQuery table with schema"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    
    # Check if table exists
    try:
        client.get_table(table_id)
        print(f" Table {BIGQUERY_TABLE} already exists")
        return
    except Exception:
        pass
    
    # Define schema
    schema = [
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("time", "TIME", mode="REQUIRED"),
        bigquery.SchemaField("temperature", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("feels_like", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_min", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_max", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("humidity", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("pressure", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("weather", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("weather_main", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("wind_deg", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("clouds", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sunrise", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("sunset", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("visibility", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("coord_lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("coord_lon", "FLOAT", mode="NULLABLE"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Raw weather data from OpenWeather API"
    
    # Partitioning by date for better performance
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp",
    )
    
    table = client.create_table(table)
    print(f" Created table {table.project}.{table.dataset_id}.{table.table_id}")

def load_from_gcs_to_bigquery(gcs_uri=None):
    """Load data from GCS to BigQuery"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    
    if gcs_uri is None:
        # Load all JSON files from raw folder
        gcs_uri = f"gs://{GCS_BUCKET_NAME}/raw/*/*/*/*.json"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    print(f" Loading data from {gcs_uri} to BigQuery...")
    
    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    
    load_job.result()  # Wait for the job to complete
    
    destination_table = client.get_table(table_id)
    print(f" Loaded {load_job.output_rows} rows to {table_id}")
    print(f" Total rows in table: {destination_table.num_rows}")

def query_latest_data():
    """Query the latest weather data"""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    query = f"""
        SELECT 
            city,
            timestamp,
            temperature,
            feels_like,
            humidity,
            weather,
            wind_speed
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
        ORDER BY timestamp DESC
        LIMIT 10
    """
    
    print("\n Latest 10 weather records:")
    print("=" * 80)
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"{row.timestamp} | {row.city} | {row.temperature}°C | {row.humidity}% | {row.weather}")
    
    print("=" * 80)

def main():
    """Main function to setup BigQuery and load data"""
    print("=" * 60)
    print("  BigQuery Setup - Starting...")
    print("=" * 60)
    
    if not GCP_PROJECT_ID or not GCS_BUCKET_NAME:
        print(" Error: GCP_PROJECT_ID and GCS_BUCKET_NAME must be set in .env")
        return
    
    # Step 1: Create dataset
    print("\n Creating dataset...")
    create_dataset()
    
    # Step 2: Create table
    print("\n Creating table...")
    create_table()
    
    # Step 3: Load data from GCS
    print("\n Loading data from GCS...")
    load_from_gcs_to_bigquery()
    
    # Step 4: Query sample data
    print("\n Querying sample data...")
    query_latest_data()
    
    print("\n" + "=" * 60)
    print(" BigQuery setup completed!")
    print("=" * 60)
    print(f"\n View your data at:")
    print(f"   https://console.cloud.google.com/bigquery?project={GCP_PROJECT_ID}&ws=!1m5!1m4!4m3!1s{GCP_PROJECT_ID}!2s{BIGQUERY_DATASET}!3s{BIGQUERY_TABLE}")

if __name__ == "__main__":
    main()