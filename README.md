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
[01_ingestion.py] → Staged Data (Parquet)
     ↓
[02_cleaning_validation.py] → Cleaned Data (Parquet)
     ↓
[03_load_dimensions.py] ┐
                        ├→ PostgreSQL Data Warehouse
[04_load_facts.py]      ┘
     ↓
[05_analyze_fact.py] → Analytics by Year/Month
[06_analyze_customer.py] → Analytics by Customer Segment
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

### Run the Entire Pipeline

```bash
cd scripts
python 01_ingestion.py
python 02_cleaning_validation.py
spark-submit --jars "path/to/postgresql-42.7.8.jar" 03_load_dimensions.py
spark-submit --jars "path/to/postgresql-42.7.8.jar" 04_load_facts.py
spark-submit --jars "path/to/postgresql-42.7.8.jar" 05_analyze_fact.py
spark-submit --jars "path/to/postgresql-42.7.8.jar" 06_analyze_customer.py
```

### Run Individual Steps

Each script can be executed independently:

```bash
# Stage raw data
python 01_ingestion.py

# Clean and validate
python 02_cleaning_validation.py

# Load dimensions only
spark-submit --jars "path/to/postgresql-42.7.8.jar" 03_load_dimensions.py

# Load facts only
spark-submit --jars "path/to/postgresql-42.7.8.jar" 04_load_facts.py

# Generate analytics
spark-submit --jars "path/to/postgresql-42.7.8.jar" 05_analyze_fact.py
spark-submit --jars "path/to/postgresql-42.7.8.jar" 06_analyze_customer.py
```

### Stop Spark Session (if needed)

```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 07_stop_spark.py
```

---

## 📁 Project Structure

```
car-insurance-data-pipeline/
├── data/
│   ├── raw/                          # Original CSV files
│   ├── staged/                       # Parquet-formatted, ready for processing
│   └── cleaned/                      # Cleaned, validated Parquet files
├── scripts/
│   ├── 01_ingestion.py              # Read CSVs, stage as Parquet
│   ├── 02_cleaning_validation.py    # Clean, validate, enforce schema
│   ├── 03_load_dimensions.py        # Load dimension tables
│   ├── 04_load_facts.py             # Load fact table
│   ├── 05_analyze_fact.py           # Analyze by time period
│   ├── 06_analyze_customer.py       # Analyze by customer segment
│   └── 07_stop_spark.py             # Cleanup Spark resources
├── sql/
│   └── create_dw_schema.sql         # Data warehouse schema definition
├── config.py                         # Centralized configuration
├── requirements.txt                  # Python dependencies
├── .env                              # Environment variables (DO NOT commit)
├── .env.example                      # Template for .env
├── .gitignore                        # Git ignore rules
└── README.md                         # This file
```

---

## 📜 Scripts Overview

### 01_ingestion.py
**Purpose**: Extract raw CSV data and stage as Parquet

**Inputs**: `data/raw/*.csv`  
**Outputs**: `data/staged/*.parquet`  
**Processing**:
- Read CSV files with Pandas
- Remove completely empty rows
- Convert to Parquet format for efficient processing

**Key Features**:
- Handles large files with `low_memory=False`
- Validates file existence
- Reports row counts before/after cleaning

---

### 02_cleaning_validation.py
**Purpose**: Clean, validate, and enforce schema on staged data using PySpark

**Inputs**: `data/staged/contracts.parquet`  
**Outputs**: `data/cleaned/contracts_clean.parquet`  
**Processing**:
- Remove currency symbols (€) from premiums
- Handle multiple date formats (MM/dd/yyyy and yyyy-MM-dd)
- Split client names into first/last names
- Cast to proper data types (Integer, Double, Date)
- Handle invalid data (nulls, negatives) with business logic
- Partition output by contract status

**Key Features**:
- PySpark distributed processing
- Schema inference and explicit casting
- Data quality checks for premiums
- Comprehensive error handling

---

### 03_load_dimensions.py
**Purpose**: Extract and load dimension tables to PostgreSQL

**Dimension Tables**:
1. **dim_customer**: Customer attributes (age, gender, segment, location)
2. **dim_policy**: Policy attributes (product, channel, status, risk zone)
3. **dim_date**: Calendar dimension (year, month, day_of_week, is_weekend)

**Processing**:
- Deduplicates records by natural key
- Generates surrogate keys for efficiency
- Uses JDBC to write to PostgreSQL

**Run Command**:
```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 03_load_dimensions.py
```

---

### 04_load_facts.py
**Purpose**: Build fact table by joining dimensions and measures

**Fact Table**: `fact_policy_snapshot`

**Measures**:
- `policy_count`: Always 1 per row (count measure)
- `total_premium`: Annual premium amount

**Processing**:
- Reads dimension tables from PostgreSQL
- Performs surrogate key lookups
- Joins cleaned data with dimensions
- Selects final fact columns

**Run Command**:
```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 04_load_facts.py
```

---

### 05_analyze_fact.py
**Purpose**: Aggregate premium and policy counts by year and month

**Output**: Year-Month analysis with rolling sums

**Processing**:
- Joins fact table with date dimension
- Groups by year, month
- Aggregates premium and policy counts
- Orders by year and month

**Run Command**:
```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 05_analyze_fact.py
```

---

### 06_analyze_customer.py
**Purpose**: Aggregate premium and policies by customer segment

**Output**: Customer segment analysis with premium distribution

**Processing**:
- Joins fact table with customer dimension
- Groups by customer segment
- Aggregates premium and policy counts
- Orders by total premium (descending)

**Run Command**:
```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 06_analyze_customer.py
```

---

### 07_stop_spark.py
**Purpose**: Gracefully shut down Spark sessions

**Usage**: Run after analysis to clean up resources

```bash
spark-submit --jars "path/to/postgresql-42.7.8.jar" 07_stop_spark.py
```

---

## 🗄️ Database Schema

### Dimension Tables

#### dim_customer
```sql
customer_key VARCHAR(50) PRIMARY KEY
first_name VARCHAR(100)
last_name VARCHAR(100)
age INTEGER
gender_code VARCHAR(10)
city_postal_code VARCHAR(15)
customer_segment VARCHAR(50)
load_date DATE
```

#### dim_policy
```sql
contract_key VARCHAR(50) PRIMARY KEY
product_type VARCHAR(50)
risk_zone VARCHAR(50)
sales_channel VARCHAR(50)
contract_status VARCHAR(20)
load_date DATE
```

#### dim_date
```sql
date_key INTEGER PRIMARY KEY (YYYYMMDD format)
full_date DATE
calendar_year INTEGER
calendar_month INTEGER
day_of_week INTEGER
is_weekend BOOLEAN
```

### Fact Table

#### fact_policy_snapshot
```sql
policy_fact_id BIGSERIAL PRIMARY KEY
customer_key VARCHAR(50) -- FK to dim_customer
contract_key VARCHAR(50) -- FK to dim_policy
load_date_key INTEGER -- FK to dim_date
annual_premium DOUBLE PRECISION
policy_count INTEGER
load_date DATE
```

---

## 🔧 Troubleshooting

### Python 3.14 Compatibility Issue

**Error**: `TypeError: 'JavaPackage' object is not callable`

**Solution**: Use Python 3.12 or 3.13. PySpark 4.0.1 has compatibility issues with Python 3.14.

```bash
# Check Python version
python --version

# Switch to Python 3.12 if available
python3.12 -m venv venv
```

### JDBC Driver Not Found

**Error**: `java.lang.ClassNotFoundException: org.postgresql.Driver`

**Solution**: Ensure `JDBC_JAR_PATH` in `.env` points to the correct PostgreSQL JDBC driver:

```
JDBC_JAR_PATH=C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar
```

### Database Connection Refused

**Error**: `org.postgresql.util.PSQLException: Connection to localhost:5432 refused`

**Solution**: 
1. Ensure PostgreSQL is running
2. Check `.env` credentials are correct
3. Verify database exists: `psql -l`

### File Not Found

**Error**: `FileNotFoundError: Raw file not found`

**Solution**: Place CSV files in `data/raw/` directory:
- `contracts.csv`
- `vehicles.csv`
- `claims.csv`
- `Telematicsdata.csv`

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

## 🤝 Contributing

To improve this pipeline:

1. Create a new branch for your feature
2. Follow Python PEP 8 style guide
3. Add comments for complex transformations
4. Test with sample data before committing
5. Update README if adding new features

---

## 📝 License

This project is provided as-is for educational and operational purposes.

---

## 👤 Contact

For questions or issues: Open an issue on the GitHub repository

---

**Last Updated**: December 15, 2025  
**Pipeline Version**: 1.0
