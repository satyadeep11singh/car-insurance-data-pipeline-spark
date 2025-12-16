# 04_load_facts.py

import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F

# Import configuration
from config import DatabaseConfig, PathConfig, SparkConfig, DatabaseTables

# --- Configuration ---
JDBC_URL = DatabaseConfig.get_jdbc_url()
JDBC_PROPERTIES = DatabaseConfig.get_jdbc_properties()

FACT_TABLE = DatabaseTables.FACT_POLICY_SNAPSHOT
CLEANED_DATA_PATH = PathConfig.CLEANED_CONTRACTS_PATH

# Dimension table names (as loaded in 03_load_dimensions.py)
DIM_CUSTOMER = DatabaseTables.DIM_CUSTOMER
DIM_POLICY = DatabaseTables.DIM_POLICY
DIM_DATE = DatabaseTables.DIM_DATE


# --- Utility Function to Read Dimension Tables ---
def read_dimension_table(spark: SparkSession, table_name: str):
    """Reads a dimension table from PostgreSQL into a Spark DataFrame."""
    print(f"Reading dimension table: {table_name}")

    # Select only the key columns for efficient lookups
    if table_name == DIM_CUSTOMER:
        cols = ["customer_key"]
    elif table_name == DIM_POLICY:
        # Note: 'policy_key' is the SK, 'contract_id' is the NK used for joining
        cols = ["policy_key", "contract_id"]
    elif table_name == DIM_DATE:
        # We need the 'date_key' (natural key) from the dimension to join on load_date
        cols = ["date_key"]

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=table_name,
        properties=JDBC_PROPERTIES
    ).select(*cols)

    return df


# --- Main Fact Loading Logic ---
def load_fact_table(spark: SparkSession):
    # 1. Read the Source Data
    print("Reading cleaned contracts data...")
    df_source = spark.read.parquet(CLEANED_DATA_PATH)

    # The source data has 15000 rows
    print(f"Source records count: {df_source.count()}")

    # 2. Read Dimension Tables (for Surrogate Keys)
    df_dim_customer = read_dimension_table(spark, DIM_CUSTOMER)
    df_dim_policy = read_dimension_table(spark, DIM_POLICY)
    df_dim_date = read_dimension_table(spark, DIM_DATE) # 'date_key' is read (as DATE type, typically)

    # 3. Join/Lookup to get Surrogate Keys
    print("\nStarting lookup joins to generate the Fact table...")

    # A. Join to Customer Dimension (Customer Key is already present in source)
    # FIX: Use the column name directly in the join list. Spark will automatically handle the ambiguity
    # and drop the duplicate column, leaving only one 'customer_key'.
    df_fact = df_source.join(
        df_dim_customer,
        ["customer_key"], # <<< FIX IS HERE
        "inner"
    ) 

    # B. Join to Policy Dimension (using contract_id as Natural Key)
    # This is fine as it joins on 'contract_id' and we drop the duplicate 'contract_id' later.
    df_fact = df_fact.join(
        df_dim_policy,
        df_fact["contract_id"] == df_dim_policy["contract_id"],
        "inner"
    ).drop(df_dim_policy["contract_id"]) # Drop the Natural Key 'contract_id' after join

    # C. Join to Date Dimension (to get Load Date Surrogate Key)

    # 1. Prepare the Date Natural Key for the Fact Table
    df_fact = df_fact.withColumn(
        "date_key_nk",
        F.date_format(F.col("load_date"), "yyyyMMdd").cast("int")
    )

    # 2. Prepare the Date Dimension Key to match the Fact Natural Key format (YYYYMMDD integer)
    df_dim_date_prepared = df_dim_date.withColumn(
        "date_key_sk",
        F.date_format(F.col("date_key"), "yyyyMMdd").cast("int")
    ).drop("date_key") # Drop the original 'date_key' column

    # 3. Perform the Date Join
    df_fact = df_fact.join(
        df_dim_date_prepared,
        df_fact["date_key_nk"] == df_dim_date_prepared["date_key_sk"],
        "inner"
    ).drop("date_key_nk") # Drop the Natural Key after join

    # 4. Add Measures and Final Select (The Fact Table Structure)
    print("Adding Measures and Selecting final Fact table columns...")

    # Now 'customer_key' is unambiguous, and 'policy_key' and 'date_key_sk' are present.
    df_fact_final = df_fact.select(
        col("customer_key"),                       # SK from dim_customer
        col("policy_key"),                        # SK from dim_policy
        col("date_key_sk").alias("load_date_key"), # SK from dim_date, renamed for fact table

        # --- Measures ---
        F.lit(1).alias("policy_count"),           # Measure: Count of policies (always 1 per row)
        col("annual_premium").alias("total_premium") # Measure: The Annual Premium amount
    )

    # 5. Write to PostgreSQL Fact Table
    print(f"\nWriting {df_fact_final.count()} records to {FACT_TABLE}...")

    # NOTE: Since this is an initial load, we use 'overwrite'.
    df_fact_final.write.jdbc(
        url=JDBC_URL,
        table=FACT_TABLE,
        mode="overwrite",
        properties=JDBC_PROPERTIES
    )

    print("==============================================")
    print("FACT TABLE LOAD COMPLETE.")
    print("==============================================")


# --- Entry Point ---
if __name__ == "__main__":
    print("--- PySpark Session Initialized for Fact Loading ---")

    spark = SparkSession.builder \
        .appName("FactLoading") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    load_fact_table(spark)

    spark.stop()