import os
import json
import requests
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from dotenv import load_dotenv
from airflow import DAG
import pendulum 

# Import the Google Cloud Storage Hook from Airflow
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# loading env variables from the .env file
env_path = '/opt/airflow/dags/.env' 
load_dotenv(env_path)

# Paste your free client credentials here for Opensky
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30

# Define your target free tier Google Cloud Bucket Name
GCS_BUCKET_NAME = "flight-data-raw-2026"

class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 21, tz="America/New_York"),
    catchup=False,
)
def dag_extract_flight_to_bucket():


    @task.python
    def extract_live_flights():
        url = "https://opensky-network.org/api/states/all"
        
        print("reached the extract_live_flights")
        try:
            tokens = TokenManager()
        except Exception as e:
            # Catching token initialization errors (like missing JSON keyfiles)
            raise AirflowException(f"TokenManager initialization failed: {e}")
        if not tokens:
            # print("Cannot proceed without a valid token.")
            # return
            raise AirflowException("Cannot proceed without a valid token.")
        
        # Use Pendulum for reliable timezone-aware date extraction
        now = pendulum.now("UTC") 
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        
        # CONSTRUCT HIVE PARTITION PATH
        # Format: opensky_raw/year=2026/month=05/day=30/hour=12/flights_...ndjson
        gcs_prefix = f"opensky_raw/year={year}/month={month}/day={day}/hour={hour}"
        filename = f"flights_{timestamp}.ndjson"
        object_name = f"{gcs_prefix}/{filename}"
        
        print("Fetching authenticated live flight data for partition: {gcs_prefix}...")
        
        try:
            response = requests.get(url, headers=tokens.headers())
            if response.status_code == 200:
                data = response.json()
                
                # 1. Initialize the Airflow Google Cloud Storage Hook
                gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
                
                # 2. Convert your JSON object to a string format
                #json_string = json.dumps(data, indent=4)
                
                #-------------

                # 1. Grab the top-level API time (Unix epoch integer)
                api_time = data.get("time")

                json_lines = []

                flight_vectors = data.get("states", []) or []

                #with open(filepath, "w", encoding="utf-8") as f:
                for vector in flight_vectors:
                    # OpenSky returns state vectors as lists. 
                    # We map the explicit indexes to match your BigQuery table schema exactly.
                    record = {
                        "icao24": vector[0],
                        "callsign": vector[1].strip() if vector[1] else None,
                        "origin_country": vector[2],
                        "time_position": vector[3],
                        "last_contact": vector[4],
                        "longitude": vector[5],
                        "latitude": vector[6],
                        "baro_altitude": vector[7],
                        "on_ground": vector[8],
                        "velocity": vector[9],
                        "true_track": vector[10],
                        "vertical_rate": vector[11],
                        "sensors": vector[12] if vector[12] else [],
                        "geo_altitude": vector[13],
                        "squawk": vector[14],
                        "spi": vector[15],
                        "position_source": vector[16],
                        "_gcs_loaded_at": api_time  # Injecting and renaming here
                    }
                    json_lines.append(json.dumps(record))

                full_json_string = "\n".join(json_lines)
                # ------------


                # 3. Stream the string data straight to the bucket without creating a local file
                gcs_hook.upload(
                    bucket_name=GCS_BUCKET_NAME,
                    object_name=object_name, # HIVE partition name in GCS
                    data=full_json_string,
                    mime_type="application/x-ndjson"
                )
                    
                print(f"Success! Data uploaded to: gs://{GCS_BUCKET_NAME}/{object_name}")
                print(f"Total aircraft tracked: {len(flight_vectors)}")
            else:
                raise AirflowException(f"Failed to fetch data. Status Code: {response.status_code}")
                
        except Exception as e:
            print(f"An error occurred: {e}")
            raise
 
    extract_fligts_to_bucket = extract_live_flights()


    # TASK 2: Define the Bronze External Table verification task using Hive Partitioning 
    verify_bronze_external_table = BigQueryCreateTableOperator(
        task_id="verify_or_create_bronze_external_table",
        gcp_conn_id="google_bigquery_dbt_key",
        dataset_id="flight_bronze",
        table_id="gcs_raw_data",
        table_resource={
            "tableReference": {
                "datasetId": "flight_bronze",
                "tableId": "gcs_raw_data",
            },
            "externalDataConfiguration": {
                "sourceUris": [f"gs://{GCS_BUCKET_NAME}/opensky_raw/*.ndjson"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True,  # Replaces exists_ok=True logic for metadata/schema resolution
                "hivePartitioningOptions": {
                    "mode": "AUTO",
                    "sourceUriPrefix": f"gs://{GCS_BUCKET_NAME}/opensky_raw/",
                },
            },
        },
        if_exists="ignore",
    )

    # TASK 3: Define the BashOperator to execute the complete dbt Medallion layer build
    run_dbt_medallion_build = BashOperator(
        task_id="execute_dbt_medallion_build",
        bash_command="cd /opt/flight_data_dbt && dbt build --profiles-dir /opt/airflow/secrets/",
    )

    # Establish the explicit top-to-bottom pipeline dependencies
    extract_fligts_to_bucket >> verify_bronze_external_table >> run_dbt_medallion_build
    # verify_bronze_external_table >> run_dbt_medallion_build
    
    
dag_extract_flight_to_bucket()
