# End-to-End Real-Time Flight Telemetry Data Pipeline

An enterprise-grade data engineering repository showcasing automated streaming ingestion, dynamic Hive partitioning, and a robust Medallion Architecture data warehouse. This system handles real-time aircraft state vectors from the OpenSky Network API, pushes the high-frequency events to Google Cloud Storage (GCS), map-links them as an external BigQuery schema, and applies dbt Core transformations to serve production-ready geospatial analytics.

---

## 🏗️ System Architecture & Data Flow Diagram
```
[OpenSky Network API] 
         │
         ▼  (Scheduled Pull every 10 mins via Apache Airflow)
┌────────────────────────────────────────────────────────────────────────┐
│ APACHE AIRFLOW ORCHESTRATION ENGINE                                    │
│                                                                        │
│  ┌───────────────────────┐     ┌───────────────────────────────────┐   │
│  │ extract_live_flights  │ ──> │ verify_or_create_bronze_ext_table │   │
│  └───────────────────────┘     └─────────────────┬─────────────────┘   │
└──────────────┬───────────────────────────────────┼─────────────────────┘
               │                                   │ (Schema Auto-Detect)
               ▼ (Stream as NDJSON)                ▼
┌─────────────────────────────────┐   ┌──────────────────────────────────┐
│ GOOGLE CLOUD STORAGE (GCS)      │   │ GOOGLE BIGQUERY DATA WAREHOUSE   │
│                                 │   │                                  │
│  Hive Partition Directory Tree  │   │  BRONZE LAYER (`flight_bronze`)  │
│  └── year=2026/                 │   │  └── `gcs_raw_data`              │
│      └── month=05/              │◀─┼────── (External Virtual Table)   │
│          └── day=30/            │   │                                  │
│              └── hour=12/       │   │  SILVER LAYER (`flight_silver`)  │
│                  └── *.ndjson   │   │  ├── `stg_flights`               │
│                                 │   │  └── `stg_data_quality_logs`     │
│                                 │   │                                  │
│                                 │   │  GOLD LAYER (`flight_gold`)      │
│                                 │   │  ├── `dim_spatial_airspaces`     │
│                                 │   │  └── `fct_flight_snapshots`      │
│                                 │   │                                  │
│                                 │   │  BI / DATA SCIENCE MARTS         │
│                                 │   │  ├── `agg_hourly_traffic_heatmap`│
│                                 │   │  └── `agg_daily_fleet_utiliz.`   │
└─────────────────────────────────┘   └──────────────────────────────────┘
                                                   ▲
                                                   │ (dbt build Execution)
                                       ┌───────────┴───────────┐
                                       │ BashOperator Task     │
                                       └───────────────────────┘
```
---

## 🛠️ Technology Stack & Operational Profiles

*   **Data Source:** OpenSky Network Live REST API (Authenticated OAuth2 Application Token Client)
*   **Orchestrator:** Apache Airflow 2.x (TaskFlow API + Specialized GCP Operators)
*   **Data Lake:** Google Cloud Storage (GCS Target Bucket: `flight-data-raw-2026`)
*   **Data Warehouse:** Google BigQuery (Serverless Analytical Platform Engine)
*   **Transformation Core:** dbt Core (v1.x) with the `dbt-bigquery` plugin framework

---

## 🔄 Ingestion & Orchestration Layer (Apache Airflow)

The data pipeline configuration is organized under a centralized processing DAG (`dag_extract_flight_to_bucket`). Although currently set to a manual fallback (`schedule=None`) for pipeline development validation, it is optimized to be triggered **every 10 minutes** to isolate continuous snapshots of active worldwide aircraft telemetry.

### 🔑 1. Dynamic Token Lifecycle Manager
To circumvent API connection throttling, a native Python class (`TokenManager`) manages active session tokens. It checks timestamps and auto-refreshes OAuth2 credentials 30 seconds before expiration via the OpenSky Identity Gateway (`auth.opensky-network.org`).

### 📂 2. Storage Partitioning Engine
The ingestion service extracts raw JSON payloads, flattens flight status arrays into discrete structural rows, and injects runtime ingestion timestamps (`_gcs_loaded_at`). Data streams directly into GCS via the `GCSHook` without writing local disk files, using strict Hive formatting paths:
`gs://flight-data-raw-2026/opensky_raw/year=YYYY/month=MM/day=DD/hour=HH/flights_timestamp.ndjson`

### 🔗 3. Airflow DAG Logic Sequence

The pipeline dependencies enforce strict sequentially executed operations:

1.  **`extract_live_flights`** (*PythonOperator via TaskFlow*): Fetches, transforms arrays into Newline-Delimited JSON (NDJSON), and streams objects directly to GCS storage paths.
2.  **`verify_or_create_bronze_external_table`** (*BigQueryCreateTableOperator*): Automatically registers the source directory path to BigQuery, dynamically computing column types and mounting virtual columns based on folder paths via `hivePartitioningOptions` set to `AUTO`.
3.  **`execute_dbt_medallion_build`** (*BashOperator*): Triggers the multi-layered dbt transformation sequence (`dbt build`), enforcing schema mappings, running performance optimizations, and evaluating testing matrices.

---

## 🏗️ Data Warehouse Transformation Layer (dbt Core)

Once the external tables are mounted inside BigQuery, the pipeline processes the data through a modular Medallion architecture using different target configurations:

```yaml
models:
  flight_data_dbt:
    +materialized: view # Global fallback safety default
    
    silver:
      +schema: silver
      +materialized: table # Explicit materialization isolation
    gold:
      +schema: gold
      +materialized: table # Explicit performance optimization
```

### 🟤 Bronze Layer (`flight_bronze`)
*   **`gcs_raw_data`**: External virtual schema mapping objects stored in GCS. Reads row files instantly without ingestion cost, exposing structural parameters alongside virtual partition metadata.

### ⚪ Silver Layer (`flight_silver`)
*   **`stg_flights`**: Deduplicates high-frequency incoming coordinates. Filters duplicate telemetry rows based on a compound key (`icao24`, `loaded_at_timestamp`), normalizes UNIX tracking epochs into BigQuery timestamps, and uses an incremental merge strategy.
*   **`stg_data_quality_logs`**: An isolated quarantine log that flags records failing validation rules (e.g., missing coordinates or missing unique transponder keys) to keep downstream reporting models clean.

### 🟡 Gold Layer (`flight_gold`)
*   **`dim_spatial_airspaces`**: Ingests static reference map files using a dbt seed file (`seed_airspace_polygons.csv`) and compiles string coordinate tracks into native BigQuery geospatial boundaries (`GEOGRAPHY`).
*   **`fct_flight_snapshots`**: The primary facts model. Intersects aircraft point vectors with airspace bounding zones in real time using spatial joins (`SAFE.ST_WITHIN`). The model is partitioned by day (`flight_date`) and clustered by `airspace_name`.
*   **`fct_active_flight_inventory`**: A real-time lookup table that filters active aircraft from the latest telemetry snapshot, reducing query cost for live tracking dashboards.
*   **`agg_hourly_traffic_heatmap`**: Pre-aggregates flight density maps into 11km coordinate grid squares to optimize web dashboard map rendering.
*   **`agg_daily_fleet_utilization`**: Tracks daily operational trends and aircraft activity rates, using string functions to extract corporate airline identifiers from transponder callsigns.

---

## 🛡️ Data Quality Governance & Testing Matrix

To maintain accurate data models, structural tests are applied at every stage of the pipeline via `dbt test`:

*   **Primary Constraint Testing**: Unique and non-null tests protect critical entity keys (`snapshot_id`, `log_id`) from duplication and corruption during ingestion.
*   **Business Rule Constraints**: The data quality staging system uses strict value parameters to standardize error logging flags:
    ```yaml
    - name: primary_error_reason
      data_tests:
        - accepted_values:
            values: ['MISSING_AIRCRAFT_ID', 'MISSING_GEOSPATIAL_COORDINATES', 'INVALID_METADATA_TIMESTAMP']
    ```

---

## 🚀 Repository Installation & Deployment Guide

### System Prerequisites
*   Python 3.10+ and a configured virtual environment.
*   An active Google Cloud Project with BigQuery and Cloud Storage enabled.
*   An Airflow deployment containing a service account key file with `Storage Object Admin` and `BigQuery Admin` roles.

### 1. Initialize local files & environment
```bash
git clone https://github.com<your-username>/flight_data_pipeline.git
cd flight_data_pipeline

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Airflow Environments & Connections
Create an environment file (`/opt/airflow/dags/.env`) containing your authenticated OpenSky developer application accounts parameters:
```env
CLIENT_ID="your_opensky_client_id_here"
CLIENT_SECRET="your_opensky_client_secret_here"
```
Configure your Airflow metadata connection targets through the UI admin console:
*   `google_cloud_default`: Create a Google Cloud Connection linked to your project storage bucket.
*   `google_bigquery_dbt_key`: Create a BigQuery connection targeting database schema modifications.

### 3. Setup Your Local dbt Profile Connection
Configure your local connections configuration schema (`~/.dbt/profiles.yml`) to route warehouse models to your BigQuery project:
```yaml
bigquery_flight:
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /opt/airflow/secrets/gcp-service-account-key.json
      project: your-gcp-project-id
      dataset: flight_dev
      threads: 4
      timeout_seconds: 300
      priority: interactive
  target: dev
```

### 4. Direct Manual Pipeline Execution Tasks
```bash
# Move into the dbt repository context path
cd dbt_transformation_code/

# Verify profile connectivity states to the BigQuery target instance
dbt debug

# Setup spatial reference maps
dbt seed

# Manually trigger the complete pipeline transformations and tests
dbt build
```

---

## 📊 Interactive Data Lineage & Visualizations
The structural configurations, dependencies, and column constraints are documented using dbt's native catalog engine.

To view the live, interactive data lineage graph and documentation site locally, run:
```bash
dbt docs generate
dbt docs serve
```
*Portfolio Note: The `index.html`, `manifest.json`, and `catalog.json` artifacts generated by dbt can be hosted on GitHub Pages or embedded directly into a personal portfolio website to show reviewers a live, interactive map of your data pipeline.*