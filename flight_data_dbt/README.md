# Real-Time Flight Telemetry Data Pipeline (dbt Core + BigQuery)

An enterprise-grade dbt Core transformation pipeline executing a Medallion Architecture on Google BigQuery. The project processes global flight telemetry ingested from the OpenSky Network API, transforming raw semi-structured JSON snapshots into highly optimized analytical layers for spatial analysis, traffic heatmaps, and fleet utilization reporting.

## 🚀 System Architecture Overview

This project operates as the transformation core of a larger architectural ecosystem designed to handle high-frequency, scheduled API payloads.

1. **Ingestion & Orchestration:** Apache Airflow triggers a Python service at scheduled intervals to extract real-time aircraft state vectors from the OpenSky Network API.
2. **Data Lake Storage:** Raw payloads are streamed as line-delimited NDJSON files directly into a Cloud Storage (GCS) bucket bucket, organized by bucket partition paths.
3. **Data Warehouse (BigQuery):** Google Cloud Storage data is mapped natively into an external Hive-partitioned Bronze table schema.
4. **Data Transformation (dbt Core):** Executes the modular SQL pipelines detailed below to enforce data cleanliness, structural integrity, and high-performance reporting models.

---

## 🏗️ Medallion Architecture Blueprint

The project groups logic into specialized schemas to support robust data staging, advanced engineering quality metrics, and performance-tuned business logic.

```

                     ┌──────────────────────────────┐
                     │     OpenSky API Ingestion    │
                     └──────────────┬───────────────┘
                                    ▼
                     ┌──────────────────────────────┐
                     │  GCS Data Lake (NDJSON)      │
                     └──────────────┬───────────────┘
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BIGQUERY DATA WAREHOUSE                                                │
│                                                                        │
│   BRONZE LAYER (flight_bronze)                                         │
│   ┌──────────────────────────────────────────────────────────────┐     │
│   │ `gcs_raw_data` (External Hive-Partitioned Table)             │     │
│   └───────────────┬──────────────────────────────┬───────────────┘     │
│                   ▼                              ▼                     │
│   SILVER LAYER (flight_silver)                                         │
│   ┌──────────────────────────────┐┌──────────────────────────────┐     │
│   │ `stg_flights` (Incremental)  ││`stg_data_quality_logs`(Incr.)│     │
│   └───────────────┬──────────────┘└──────────────────────────────┘     │
│                   ▼                                                    │
│   GOLD LAYER (flight_gold)                                             │
│   ┌──────────────────────────────┐┌──────────────────────────────┐     │
│   │ `dim_spatial_airspaces`      ││ `fct_flight_snapshots`(Incr.)│     │
│   │ (Reference Shapefile Seed)   ││ (Telemetry Core Fact Table)  │     │
│   └──────────────────────────────┘└──────────────┬───────────────┘     │
│                                                  ▼                     │
│   ANALYTICS & BI MARTS                                                 │
│   ┌──────────────────────────────┐┌──────────────────────────────┐     │
│   │ `agg_hourly_traffic_heatmap` ││`agg_daily_fleet_utilization` │     │
│   │ (Partitioned Heatmap Block)  ││(Incremental Rolling Metrics) │     │
│   └──────────────────────────────┘└──────────────────────────────┘     │
│   ┌──────────────────────────────┐                                     │
│   │ `fct_active_flight_inventory`│                                     │
│   │ (Latest Sky Active State)    │                                     │
│   └──────────────────────────────┘                                     │
└────────────────────────────────────────────────────────────────────────┘
```

### 🟤 1. Bronze Layer (`flight_bronze`)
*   **`gcs_raw_data`**: External BigQuery table referencing raw JSON objects from Cloud Storage. It exposes native API properties alongside infrastructure load metadata (`_gcs_loaded_at`).

### ⚪ 2. Silver Layer (`flight_silver`)
Transforms raw records into strongly typed relational formats using strict casting filters to protect downstream consumers.
*   **`stg_flights`**: Evaluates new partitions incrementally using a compound merge-key strategy (`icao24`, `loaded_at_timestamp`). Implements explicit Unix-to-Timestamp parsing and windowed deduplication protocols.
*   **`stg_data_quality_logs`**: Acting as an automated quarantine repository, this model evaluates incoming records against compliance criteria. Records exhibiting structural violations (`MISSING_AIRCRAFT_ID`, `MISSING_GEOSPATIAL_COORDINATES`) are diverted to this log for system audit tracking.

### 🟡 3. Gold Layer (`flight_gold`)
Models production business marts, aggregating high-volume time series data while isolating specialized real-time tables.
*   **`dim_spatial_airspaces`**: Ingests reference geometries via a dbt static seed (`seed_airspace_polygons.csv`) and compiles Well-Known Text (WKT) boundary rows directly into native BigQuery geospatial geometry objects (`GEOGRAPHY`).
*   **`fct_flight_snapshots`**: The primary data table. Employs BigQuery's spatial engine (`SAFE.ST_WITHIN`) to map flight latitude/longitude coordinates to defined political airspace polygons. Configured with strict partition boundaries on `flight_date` and clustered on `airspace_name`.
*   **`fct_active_flight_inventory`**: Real-time caching target filtering active aircraft from the latest available execution window, minimizing BI tool query scanning overhead.
*   **`agg_hourly_traffic_heatmap`**: Pre-computed spatial aggregation grid matching down to 11km cells via localized spatial coordinates rounding, engineered specifically to power web rendering map visuals.
*   **`agg_daily_fleet_utilization`**: Incremental roll-up table tracking carrier flight densities and tracking distinct airframes safely extracted via standard callsign string prefixes.

---

## 🛠️ Performance Tuning & Data Optimization Techniques

Handling millions of continuous telemetry pings requires deliberate cloud-cost mitigation strategies. This project implements data modeling configurations directly inside dbt to guarantee performance.

### 📑 Advanced Incremental Materialization
Downstream tables scale efficiently by avoiding redundant computing scans. Models utilize a `merge` strategy to combine historical state arrays with recent GCS file segments.
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='snapshot_id',
        ...
    )
}}
```

### 🗂️ Optimized Partitioning & Clustering
To prune data scans during analytical queries, tables are explicitly partitioned by business date variables and clustered on fields commonly used for filtering.
*   **`fct_flight_snapshots`**: Partitioned by day (`flight_date`), clustered by `airspace_name, icao24`.
*   **`agg_hourly_traffic_heatmap`**: Partitioned by day (`snapshot_date`), clustered by `airspace_name`.
*   **`stg_flights`**: Partitioned by timestamp day (`loaded_at_timestamp`), clustered by `icao24, origin_country`.

---

## 🧪 Data Governance & Data Quality Testing

Robust data pipelines require structural enforcement. Data quality boundaries are verified at each runtime deployment stage.

### Generic Data Auditing Tests
Standard schemas are evaluated against fundamental constraints directly in the configuration layers:
*   `not_null`: Applied to core primary identification keys across fact snapshots and data logs.
*   `unique`: Validates uniqueness on surrogate hashes (`snapshot_id`, `log_id`) to ensure no duplicate records slip past the deduplication layer.

### Specialized Value Assertion Testing
Custom tests constrain structural column contents to known boundaries. The data quality staging schema limits log generation arrays to exact validation flags:

```yaml
- name: primary_error_reason
  data_tests:
    - accepted_values:
        values: ['MISSING_AIRCRAFT_ID', 'MISSING_GEOSPATIAL_COORDINATES', 'INVALID_METADATA_TIMESTAMP']
```

---

## 💻 Local Project Execution Guide

### Prerequisites
*   Python 3.10+ installed
*   An active Google Cloud Platform account with BigQuery enabled
*   GCP Service Account credentials containing `BigQuery Data Editor` and `BigQuery User` permissions

### 1. Setup Development Environment
Clone the project repository and initialize an isolated virtual environment shell:
```bash
git clone https://github.com<your-username>/flight_data_dbt.git
cd flight_data_dbt

python -p venv venv
source venv/bin/activate
pip install dbt-bigquery
```

### 2. Connect Your BigQuery Environment
Configure your local connection profiles file (typically situated under `~/.dbt/profiles.yml`) to align with the target schema configurations:
```yaml
bigquery_flight:
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/your/gcp-service-account-key.json
      project: flight-data-pipeline-497203 # Your GCP Project ID
      dataset: flight_dev
      threads: 4
      timeout_seconds: 300
      priority: interactive
  target: dev
```

### 3. Run and Validate the dbt Pipeline
Execute seed setups, run data transformations, and validate table states via the command line interface:
```bash
# Verify connection profile paths match the BigQuery schema destination
dbt debug

# Load the static regional airspace polygons seed
dbt seed

# Run the complete Medallion Architecture transformations
dbt run

# Execute all configured schema validation tests
dbt test
```

### 4. Interactive Lineage Graph & Project Documentation
Generate the project catalog to view model configurations, data types, and live relationship graphs interactively:
```bash
# Compute JSON manifests, metadata logs, and structural maps
dbt docs generate

# Serve the static documentation interface locally
dbt docs serve
```
*Tip: The generated `target/index.html`, `manifest.json`, and `catalog.json` can be hosted directly on GitHub Pages to share a live, interactive data lineage map with portfolio reviewers!*