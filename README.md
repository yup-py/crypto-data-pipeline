
# Cryptocurrency Data Analytics Pipeline (Medallion Architecture)

This repository contains an end-to-end **ELT data pipeline** designed to ingest, process, store, and visualize real-time cryptocurrency metrics from the  **CoinGecko API** . Built using a  **Medallion Architecture** , the pipeline leverages a local data lake for object storage, an automated workflow orchestrator, a cloud data warehouse, and a reporting layer to deliver actionable financial insights.

## 📚 Project Documentation

For a deep dive into the underlying systems engineering, technical specifications, and relational data structures, refer to the individual modules below:

* 🗺️ **[End-to-End Pipeline Architecture Documentation](./docs/PIPELINE_ARCHITECTURE.md)** — Explains the system data flows across MinIO, Snowflake, and Tableau.
* 📊 **[Dimensional Star Schema Documentation](./docs/DIMENSIONAL_MODEL.md)** — Outlines table granularities, primary/foreign keys, and data-dictionary boundaries.

## 🏗️ Architecture Overview

The pipeline implements an **ELT (Extract, Load, Transform)** workflow structured around three logical data layers to ensure clean, reliable, and decoupled data management:

**Plaintext**

```
[ CoinGecko API ] ──> (Airflow Orchestrator)
                             │
                             ▼
     ┌──────────────────────────────────────────────┐
     │            MinIO S3 Data Lake                │
     │  ┌──────────────┐ ┌──────────────┐ ┌──────┐  │
     │  │ Bronze (Raw) │ │Silver(Clean) │ │ Gold │  │
     │  └──────────────┘ └──────────────┘ └──────┘  │
     └───────────────────────┬──────────────────────┘
                             │
                             ▼
                    [ Snowflake WH ]
                             │
                             ▼
                    [ Tableau/BI Layer ]
```

### Data Layers

1. **Bronze Layer (Raw Ingestion)**
   * Extracts raw market tracking payloads directly from the CoinGecko API.
   * Saves data incrementally as native JSON records partitioned by execution date (`YYYY/MM/DD/raw.json`) inside **MinIO** storage.
2. **Silver Layer (Cleaned & Validated)**
   * Reads from the Bronze storage bucket, applies structural schemas, flattens heavily nested fields, and standardizes datatypes.
   * Handles deduplication, handles missing values, and writes processed internal data back to MinIO.
3. **Gold Layer (Analytical Modeling)**
   * Implements a business-level dimensional star schema layout.
   * Performs critical aggregations (market caps, trading volumes, price percentage changes) optimized for analytical queries.
4. **Data Warehouse Loading**
   * Automatically copies processed datasets from the Gold/Silver storage layer directly into optimized structural tables in **Snowflake** for consumption.

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow
* **Data Lake Storage:** MinIO (S3-Compatible Object Storage)
* **Data Warehousing:** Snowflake
* **Languages & Environments:** Python, SQL, Docker, Docker Compose
* **Visualization Layer:** Tableau Workbooks (`.twb`)

## 📂 Project Structure

**Plaintext**

```
.
├── dags/
│   └── crypto_pipeline_dag.py         # Airflow DAG definition and task scheduling
├── dashboard/
│   ├── crypto.twb                     # Tableau Dashboard file
│   └── screenshots/                   # Performance and UI reference captures
├── data/
│   └── landing/                       # Local volume landings (CSV/JSON formats)
├── docs/
│   ├── assets/                        # ERD and pipeline diagram illustrations
│   ├── DIMENSIONAL_MODEL.md           # Analytical data architecture schemas
│   └── PIPELINE_ARCHITECTURE.md       # Deep-dive pipeline processing flow documentation
├── logs/                              # Step-by-step pipeline execution logs
├── minio_data/                        # Persistent localized volume directory for MinIO storage
├── src/
│   ├── config/
│   │   └── minio_config.py            # MinIO credentials and connectivity setup
│   ├── pipelines/
│   │   ├── bronze_layer.py            # Raw data ingestion pipeline tasks
│   │   ├── silver_layer.py            # Data cleaning and transformation modules
│   │   ├── gold_layer.py              # Dimensional model aggregation layer
│   │   └── load_snowflake.py          # Data ingestion and synchronizations for Snowflake
│   ├── scripts/
│   │   └── setup_minio.py             # Provisioning script for automated bucket setup
│   └── utils/
│       ├── minio_client.py            # MinIO API wrapper client abstraction
│       └── pipeline_utils.py          # Universal helper scripts and logging utilities
├── .gitignore                         # Tracking ignore parameters
├── docker-compose.yml                 # Local Docker infrastructure multi-container composition
├── Dockerfile                         # Airflow-extended execution image build file
├── requirements.txt                   # External dependencies specification list
└── view_data.py                       # Local sanity verification script for developer debugging
```

## 🚀 Getting Started

### Prerequisites

* Ensure **Docker** and **Docker Compose** are installed and running locally.
* A **Snowflake** account with a configured database, warehouse, and stage integration permissions.

### Step 1: Environment Variables Setup

Configure your local connections by establishing access variables. Create a `.env` file in the root directory:

**Code snippet**

```
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=supersecretpassword
MINIO_ENDPOINT=minio:9000

SNOWFLAKE_ACCOUNT=<your_account_identifier>
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

### Step 2: Launch the Infrastructure

Spin up the coordinated environment services (Airflow, MinIO, and associated persistent databases):

**Bash**

```
docker-compose up -d --build
```

### Step 3: Initialize Object Storage Buckets

Run the custom internal initialization script to systematically pre-configure the necessary infrastructure storage namespaces inside MinIO:

**Bash**

```
docker-compose exec airflow-trigger python src/scripts/setup_minio.py
```

This ensures the baseline structures are created correctly:

* `crypto-bronze`
* `crypto-silver`
* `crypto-gold`

## 📊 Pipeline Orchestration

The workflow runs via  **Apache Airflow** , tracking data lineage across multiple distinct processing tasks sequentially:

| **Task ID**      | **Component Module** | **Objective**                  | **Input Source** | **Target Output**   |
| ---------------------- | -------------------------- | ------------------------------------ | ---------------------- | ------------------------- |
| `ingest_raw_data`    | `bronze_layer.py`        | Pull raw ticker items                | CoinGecko API          | MinIO (`crypto-bronze`) |
| `clean_silver_data`  | `silver_layer.py`        | Schema typing and flattening         | MinIO Bronze           | MinIO (`crypto-silver`) |
| `build_gold_metrics` | `gold_layer.py`          | Core analytic matrix generation      | MinIO Silver           | MinIO (`crypto-gold`)   |
| `sync_to_snowflake`  | `load_snowflake.py`      | Sync production-ready dimension sets | MinIO Gold/Silver      | Snowflake Data Warehouse  |

## 📈 Insights and Data Storytelling

Analytical metrics loaded into Snowflake are plugged directly into downstream analytics layers (`dashboard/crypto.twb`) to report on:

* **Market Capitalization Tranches:** Real-time volume shifting classifications across tokens.
* **Price Volatility Tracking:** High-granularity trends parsing top gainers and losers.
* **Volume/Liquidity Matrix:** Statistical breakdowns cross-referencing total circulation liquidity values against active tracking velocity.

> 📝 **Note:** Captured application states and visual analytics layouts are archived for review within the `dashboard/screenshots/` directory.
>
