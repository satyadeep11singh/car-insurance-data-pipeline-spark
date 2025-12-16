# Car Insurance Data Pipeline

A production-grade ETL pipeline for insurance data processing using Apache Spark, PostgreSQL, and Python. This project demonstrates best practices for data ingestion, cleaning, dimensional modeling, and analytics.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Pipeline Execution](#pipeline-execution)
- [Project Structure](#project-structure)
- [Scripts Overview](#scripts-overview)
- [Database Schema](#database-schema)
- [Troubleshooting](#troubleshooting)
- [Security](#security)

---

## 🎯 Project Overview

This pipeline processes insurance contract, vehicle, claims, and telematics data through multiple stages:

1. **Ingestion**: Load raw CSV files and stage as Parquet
2. **Cleaning**: Validate data quality, transform schemas, handle inconsistencies
3. **Dimensional Loading**: Extract and load dimension tables (Customer, Policy, Date)
4. **Fact Loading**: Load fact table with measures (Premium, Policy Count)
5. **Analysis**: Generate analytics by time period and customer segment

**Data Volume**: ~15,000 policy records with supporting claims and telematics data

---

## 🏗️ Architecture

```
Raw Data (CSV)
     ↓
[01_ingest_csv_to_parquet.py] → Staged Data (Parquet)
     ↓
[02_clean_contracts_data.py] + [03_clean_multisource_data.py] → Cleaned Data
     ↓
[04_load_dimension_tables.py]
[05_load_fact_tables.py]       ├→ PostgreSQL Data Warehouse
[07_load_fact_claims.py]       
[08_load_driver_risk_scores.py]
     ↓
[06_validate_data_quality.py] → QA Report
[09_analyze_fact_metrics.py] → Time-Series Analytics
[10_analyze_customer_segments.py] → Segment Analytics
```

### Data Layers

| Layer | Format | Location | Purpose |
|-------|--------|----------|---------|
| **Raw** | CSV | `data/raw/` | Original source data |
| **Staged** | Parquet | `data/staged/` | Normalized, partitioned format |
| **Cleaned** | Parquet | `data/cleaned/` | Schema enforced, validated data |
| **Warehouse** | PostgreSQL | `insurance_dw` schema | Dimensional model (dimensions + facts) |

---

## 📋 Prerequisites

### Software Requirements

- **Python**: 3.12 or 3.13 (Python 3.14 has compatibility issues with PySpark 4.0.1)
- **Apache Spark**: 3.5.7+ (with Hadoop 3)
- **PostgreSQL**: 12+
- **Java**: JDK 17 or later

### Environment Variables

Ensure these are set on your system:

```
JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot
SPARK_HOME=C:\Program Files\spark-3.5.7-bin-hadoop3
```

### Python Packages

All dependencies are listed in `requirements.txt`:

```
pandas>=1.3.0
pyarrow>=6.0.0
pyspark>=3.0.0
python-dotenv>=0.19.0
```

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/satyadeep11singh/car-insurance-data-pipeline-spark.git
cd car-insurance-data-pipeline-spark
```

### 2. Set Up Python Environment

```bash
# Create virtual environment (optional but recommended)
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### 3. Create PostgreSQL Database

```bash
# Connect to PostgreSQL and create database
createdb insurance_dw_db -U postgres

# Or using psql:
psql -U postgres
CREATE DATABASE insurance_dw_db;
\c insurance_dw_db
\i sql/create_dw_schema.sql
```

### 4. Configure Environment Variables

```bash
# Copy the example file
copy .env.example .env  # Windows
cp .env.example .env    # Linux/Mac

# Edit .env with your actual credentials
nano .env  # or use your preferred editor
```

**Required .env variables:**

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=insurance_dw_db
DB_USER=your_postgres_user
DB_PASSWORD=your_password
JDBC_JAR_PATH=C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar
SPARK_LOG_LEVEL=ERROR
```

### 5. Prepare Input Data

Place your raw CSV files in the `data/raw/` directory:

```
data/raw/
├── contracts.csv         # Policy contract records
├── vehicles.csv          # Vehicle information
├── claims.csv            # Claims data
└── Telematicsdata.csv    # Telematics records
```

---

## 📊 Pipeline Execution

### Sequential Execution Order

```bash
cd scripts

# Stage 1: Ingestion & Cleaning
python 01_ingest_csv_to_parquet.py
python 02_clean_contracts_data.py
python 03_clean_multisource_data.py

# Stage 2: Dimensional Loading (requires JDBC JAR and PostgreSQL)
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 04_load_dimension_tables.py
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 05_load_fact_tables.py
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 06_validate_data_quality.py
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 07_load_fact_claims.py
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 08_load_driver_risk_scores.py

# Stage 3: Analysis
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 09_analyze_fact_metrics.py
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 10_analyze_customer_segments.py

# Stage 4: Cleanup
spark-submit --jars "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar" 11_cleanup_spark_session.py
```

**Note**: Environment variables (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD) must be set in `.env` file.

---

## 📁 Project Structure

```
car-insurance-data-pipeline/
├── data/
│   ├── raw/                              # Original CSV files
│   ├── staged/                           # Parquet-formatted, ready for processing
│   └── cleaned/                          # Cleaned, validated Parquet files
├── scripts/
│   ├── 01_ingest_csv_to_parquet.py      # Read CSVs, stage as Parquet
│   ├── 02_clean_contracts_data.py       # Clean and validate contracts
│   ├── 03_clean_multisource_data.py     # Clean vehicles, claims, telematics
│   ├── 04_load_dimension_tables.py      # Load dimensions (customer, policy, date)
│   ├── 05_load_fact_tables.py           # Load fact_policy_snapshot
│   ├── 06_validate_data_quality.py      # Data quality validation
│   ├── 07_load_fact_claims.py           # Load fact_claims
│   ├── 08_load_driver_risk_scores.py    # Calculate and load driver risk scores
│   ├── 09_analyze_fact_metrics.py       # Analyze by time period
│   ├── 10_analyze_customer_segments.py  # Analyze by customer segment
│   └── 11_cleanup_spark_session.py      # Cleanup Spark resources
├── config.py                             # Centralized configuration (database, paths, JDBC)
├── requirements.txt                      # Python dependencies
├── .env                                  # Environment variables (DO NOT commit)
├── .env.example                          # Template for .env
├── .gitignore                            # Git ignore rules
├── COMMENTS_ANALYSIS.md                  # Code quality documentation
└── README.md                             # This file
```

---

## 📜 Scripts Overview

### 01_ingest_csv_to_parquet.py
**Reads** raw CSV files → **Outputs** Parquet files in `data/staged/`  
Removes completely empty rows and normalizes format.

### 02_clean_contracts_data.py
**Reads** `contracts.parquet` → **Outputs** `contracts_clean.parquet`  
Splits names, parses dates (MM/dd/yyyy & yyyy-MM-dd), removes currency, enforces schema.

### 03_clean_multisource_data.py
**Reads** vehicles, claims, telematics Parquet → **Outputs** cleaned Parquet files  
Vehicles: Parse HP, remove €. Claims: Handle date formats. Telematics: Convert Unix timestamps, parse GPS.

### 04_load_dimension_tables.py
**Loads** dim_customer, dim_policy, dim_date to PostgreSQL  
Deduplicates by natural key, generates surrogate keys.

### 05_load_fact_tables.py
**Loads** fact_policy_snapshot to PostgreSQL  
Joins cleaned contracts with dimensions, creates measures (policy_count, total_premium).

### 06_validate_data_quality.py
**Validates** fact_policy_snapshot: record counts, NULLs in keys, premium statistics.

### 07_load_fact_claims.py
**Loads** fact_claims to PostgreSQL  
Joins claims with fact_policy_snapshot and dim_date.

### 08_load_driver_risk_scores.py
**Loads** fact_driver_risk to PostgreSQL  
Calculates Haversine distance, speeds, and risk scores from telematics GPS data.

### 09_analyze_fact_metrics.py
**Aggregates** premium and policies by year/month.

### 10_analyze_customer_segments.py
**Aggregates** premium and policies by customer segment.

### 11_cleanup_spark_session.py
**Gracefully stops** Spark session and releases resources.

---

## 🗄️ Database Schema

### Dimension Tables

#### dim_customer
- **customer_key** (PK): Customer identifier
- **first_name, last_name**: Customer names
- **age, gender_code**: Demographics
- **city_postal_code**: Location
- **customer_segment**: Premium, Standard, Budget
- **load_date**: Load timestamp

#### dim_policy
- **policy_key** (PK): Surrogate key
- **contract_id** (NK): Natural key (from source)
- **product_type**: Coverage type
- **risk_zone, sales_channel**: Policy attributes
- **contract_status**: Active/Expired/Pending
- **load_date**: Load timestamp

#### dim_date
- **date_key** (PK): YYYYMMDD format
- **year, month, day_of_week**: Temporal attributes
- **month_name, quarter**: Additional attributes

### Fact Tables

#### fact_policy_snapshot
- **customer_key, policy_key, load_date_key**: Surrogate keys
- **policy_count**: Always 1
- **total_premium**: Annual premium

#### fact_claims
- **customer_key, policy_key, claim_date_key**: Surrogate keys
- **claim_id**: Degenerate dimension
- **claim_amount, claim_status, claim_type, liability**: Measures and attributes

#### fact_driver_risk
- **customer_key, device_id**: Driver/device identifiers
- **risk_score**: 0-100 scale (SAFE≥80, MODERATE≥60, RISKY≥40, VERY_RISKY<40)
- **avg_speed, max_speed, speeding_count**: Telematics metrics

---

## 🔧 Troubleshooting

### PostgreSQL Authentication Error
**Error**: `org.postgresql.util.PSQLException: password is an empty string`

**Solution**: Ensure `.env` has valid DB_PASSWORD:
```
DB_PASSWORD=your_actual_password
```
Do not leave it empty.

### JDBC Driver Not Found
**Error**: `java.lang.ClassNotFoundException: org.postgresql.Driver`

**Solution**: Update JDBC_JAR_PATH in `.env`:
```
JDBC_JAR_PATH=C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar
```

### Database Connection Refused
**Error**: `org.postgresql.util.PSQLException: Connection to localhost:5432 refused`

**Solution**: 
1. Verify PostgreSQL is running
2. Check credentials in `.env` are correct
3. Verify database exists: `psql -l`

### Missing Data Files
**Error**: `FileNotFoundError` or parquet file not found

**Solution**: Place CSV files in `data/raw/`:
- contracts.csv
- vehicles.csv
- claims.csv
- Telematicsdata.csv

### Python Version Incompatibility
**Error**: `TypeError: 'JavaPackage' object is not callable`

**Solution**: Use Python 3.12 or 3.13 (avoid 3.14 with PySpark 4.0.1)

---

## 🔒 Security

### Credential Management

✅ **DO**:
- Store sensitive credentials in `.env` file
- Use environment variables via `config.py`
- Add `.env` to `.gitignore`
- Share `.env.example` as a template

❌ **DON'T**:
- Hardcode passwords in scripts
- Commit `.env` to version control
- Share `.env` file with credentials

### Database Privileges

Create a dedicated PostgreSQL user for the pipeline:

```sql
CREATE USER pipeline_user WITH PASSWORD 'strong_password';
ALTER ROLE pipeline_user SET search_path = insurance_dw;
GRANT CONNECT ON DATABASE insurance_dw_db TO pipeline_user;
GRANT USAGE ON SCHEMA insurance_dw TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA insurance_dw TO pipeline_user;
```

---

## 📈 Performance Considerations

- **Staging Layer**: Parquet format enables efficient columnar reads
- **Partitioning**: Fact table partitioned by `contract_status` for query optimization
- **JDBC Batch Size**: Default 1000 rows; adjust for larger datasets
- **Spark Executor Memory**: Configure in `spark-submit` for large-scale processing

Example:
```bash
spark-submit --driver-memory 4g --executor-memory 4g --jars "path" script.py
```

---

**Last Updated**: December 16, 2025  
**Pipeline Version**: 1.0  
**Status**: Production-Ready
