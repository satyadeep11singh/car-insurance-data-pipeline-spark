import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, date_format, to_date, sequence, explode
from datetime import date, timedelta # Keep for constants, but they won't be used for list generation
from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
from pyspark.sql.window import Window

# Import configuration
from config import DatabaseConfig, PathConfig, SparkConfig, DatabaseTables

# --- Configuration ---
CLEANED_CONTRACTS_PATH = PathConfig.CLEANED_CONTRACTS_PATH

# --- PostgreSQL Connection Details (from environment variables) ---
DB_URL = DatabaseConfig.get_jdbc_url()
DB_PROPERTIES = DatabaseConfig.get_jdbc_properties()

# --- Dimension Table Destinations ---
CUSTOMER_TABLE = DatabaseTables.DIM_CUSTOMER
POLICY_TABLE = DatabaseTables.DIM_POLICY


def initialize_spark_session():
    """Initializes Spark Session with JDBC driver configuration."""
    spark = SparkSession.builder \
        .appName("DimensionalLoading") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .config("spark.driver.host", "127.0.0.1")  \
        .config("spark.executor.host", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel(SparkConfig.LOG_LEVEL)
    print("--- PySpark Session Initialized for JDBC Loading ---")
    return spark


def load_dimension_table(df_source, table_name, unique_key_cols):
    """
    Performs deduplication and writes the resulting DataFrame to PostgreSQL.
    """
    print(f"\n--- Loading {table_name} ---")

    # 1. Deduplication: Select the most recent record for each unique key
    # We assume 'load_date' in the source is the latest pipeline run date.
    # In a real DW, you would use window functions for more complex SCD type 2 logic.
    df_deduped = df_source.dropDuplicates(unique_key_cols)

    # 2. Write to PostgreSQL using JDBC
    # Using 'overwrite' is simple for initial loading, but 'append' or 'upsert'
    # logic (via pandas or an external library) is typical for production DW.
    df_deduped.write \
        .jdbc(url=DB_URL,
              table=table_name,
              mode="overwrite", # Choose overwrite for simplicity in this phase
              properties=DB_PROPERTIES)

    print(f"Data successfully loaded into PostgreSQL table: {table_name}")
    print(f"Total records loaded: {df_deduped.count()}")


def load_dimensions(spark):
    """Orchestrates the loading of all dimension tables."""

    # 1. Read Cleaned Contracts Data
    print(f"Reading cleaned data from: {CLEANED_CONTRACTS_PATH}")
    df_contracts = spark.read.parquet(CLEANED_CONTRACTS_PATH)

    # 2. Extract and Load DIM_CUSTOMER
    # Selecting columns and dropping duplicates based on the primary key (customer_key)
    df_customer = df_contracts.select(
        col("customer_key"),
        col("first_name"),
        col("last_name"),
        col("age"),
        col("gender_code"),
        col("city_postal_code"),
        col("customer_segment"),
        col("load_date")
    )
    load_dimension_table(df_customer, CUSTOMER_TABLE, ["customer_key"])

    # 3. Extract and Load DIM_POLICY
    # Selecting columns and dropping duplicates based on the primary key (contract_key)
    df_policy = df_contracts.select(
        col("contract_id"),
        col("product_type"),
        col("risk_zone"),
        col("sales_channel"),
        col("contract_status"),
        col("load_date")
    )

    # 3b. Generate the Surrogate Key (SK) - policy_key
    # We use row_number() over a window to create a sequential, unique integer ID.
    window_spec = Window.orderBy("contract_id") # Ordering ensures consistent key generation
    
    df_dim_policy = df_policy.withColumn(
        "policy_key",
        row_number().over(window_spec)
    )

    # 3c. Select the final columns, ensuring 'policy_key' is first (Best Practice)
    df_dim_policy = df_dim_policy.select(
        col("policy_key"),
        col("contract_id"),
        col("product_type"),
        col("risk_zone"),
        col("sales_channel"),
        col("contract_status"),
        col("load_date")
    )
    load_dimension_table(df_policy, POLICY_TABLE, ["contract_id"])

    # NOTE: DIM_DATE population is a separate process, often done by generating
    # a calendar table and then loading it once. We will skip the generation script for now.
    
    print("\n==============================================")
    print("DIMENSIONAL LOAD COMPLETE: Customer and Policy tables are populated.")
    print("==============================================")

    # Define Date Table Constants
    DATE_TABLE = "insurance_dw.dim_date"
    START_DATE = "2020-01-01"
    END_DATE = "2030-12-31"

    print("\n--- Loading insurance_dw.dim_date ---")

    # 4. Generate and Load DIM_DATE
    df_dim_date = generate_date_dimension(spark, START_DATE, END_DATE)

    # Note: We use 'date_key' as the unique key, which is the date itself.
    load_dimension_table(df_dim_date, DATE_TABLE, ["date_key"])

    print("==============================================")
    print("DIMENSIONAL LOAD COMPLETE: All tables populated.")
    print("==============================================")


def generate_date_dimension(spark, start_date, end_date):
    """Generates a date dimension DataFrame using Spark's sequence function."""
    
    # 1. Create a single-row DataFrame with a sequence of dates
    df_temp = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as dates")
    
    # 2. Explode the list of dates into individual rows
    df_dates = df_temp.withColumn("date_key", explode(col("dates"))).drop("dates")
    
    # 3. Calculate time attributes
    df_dim_date = df_dates.select(
        col("date_key"),
        year(col("date_key")).alias("year"),
        month(col("date_key")).alias("month"),
        dayofmonth(col("date_key")).alias("day"),
        date_format(col("date_key"), "EEEE").alias("day_of_week"),
        date_format(col("date_key"), "MMMM").alias("month_name"),
        ((month(col("date_key")) - 1) / 3 + 1).cast("integer").alias("quarter")
    )
    
    return df_dim_date

if __name__ == "__main__":
    # Initialize Spark Session with configuration from config.py
    spark = initialize_spark_session()
    
    # Load dimensions
    load_dimensions(spark)
    
    # Stop Spark Session
    spark.stop()
    print("--- Spark Session Stopped ---")
    print("ERROR: Please update POSTGRES_JDBC_JAR with the full path to your postgresql-*.jar file.")
else:
    spark = initialize_spark_session(POSTGRES_JDBC_JAR)
    load_dimensions(spark)
    spark.stop()