"""
02_clean_contracts_data.py - Contract Data Cleaning and Validation

Transform and validate contracts data with schema enforcement:
- Split full names into first/last names
- Parse and standardize dates (handles both MM/dd/yyyy and yyyy-MM-dd)
- Remove currency symbols and cast to numeric types
- Rename columns for clarity in data modeling
- Add load_date metadata

Input: ../data/staged/contracts.parquet
Output: ../data/cleaned/contracts_clean.parquet (partitioned by contract_status)

Dependencies: PySpark, config
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when, regexp_replace, split, substring_index
from pyspark.sql.types import IntegerType, DateType, DoubleType, StringType


# --- Configuration ---
STAGED_CONTRACTS_PATH = '../data/staged/contracts.parquet'
CLEANED_CONTRACTS_PATH = '../data/cleaned/contracts_clean.parquet'

def initialize_spark_session():
    """Initializes and returns the Spark Session."""
    spark = SparkSession.builder \
        .appName("InsuranceDataCleaning") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Reduce noisy logging
    print("--- PySpark Session Initialized ---")
    return spark

def clean_and_validate_contracts(spark):
    """Performs cleaning, validation, and schema enforcement on contracts data."""
    
    # 1. Read Staged Data (Parquet)
    print(f"Reading staged data from: {STAGED_CONTRACTS_PATH}")
    # We let Spark infer the schema first, then explicitly cast/clean below.
    df_contracts = spark.read.parquet(STAGED_CONTRACTS_PATH)
    
    # 2. Schema Enforcement and Data Quality (DQ) Checks
    df_clean = df_contracts.select(
        # Identity Fields (Keep as String for now)
        col("contract_id").alias("contract_id"),
        col("client_id").alias("customer_key"), # Renamed for clarity in modeling
        
        # Composite Field Splitting (New Step)
        # Split "Pascal Dubois" into "Pascal" and "Dubois"
        substring_index(col("client_name"), " ", 1).alias("first_name"),
        substring_index(col("client_name"), " ", -1).alias("last_name"),

        # Numeric Cleaning (Premium) (CRITICAL STEP)
        regexp_replace(col("annual_premium"), "€", "").alias("premium_str_clean"),
        # The result of the above is a string that can be cast to Double
        
        # Inconsistent Date Handling (CRITICAL STEP)
        # Handle two known formats: MM/dd/yyyy AND yyyy-MM-dd
        # We use a CASE statement to check for common yyyy-MM-dd pattern first
        when(col("start_date").like("____-__-__"), to_date(col("start_date"), "yyyy-MM-dd"))
        .otherwise(to_date(col("start_date"), "MM/dd/yyyy"))
        .alias("contract_start_date"),
        
        # Assuming end_date only uses the yyyy-MM-dd format for simplification
        to_date(col("end_date"), "yyyy-MM-dd").alias("contract_end_date"),
        
        # Other Fields: Casting and Simple Cleansing
        col("product").alias("product_type"),
        col("status").alias("contract_status"),
        col("city_postal").alias("city_postal_code"),
        col("risk_zone").alias("risk_zone"),
        col("client_age").cast(IntegerType()).alias("age"), # Enforce Integer Type
        col("channel").alias("sales_channel"),
        col("csp").alias("customer_segment"),
        col("gender").alias("gender_code")
    )
    
    # 3. Final Casting and DQ Checks
    df_clean = df_clean.withColumn("annual_premium", 
        # Cast the cleaned string to Double. If the cast fails (e.g., non-numeric data remains), it becomes NULL.
        col("premium_str_clean").cast(DoubleType())
    ).drop("premium_str_clean")

    # DQ Check: Handle invalid or null premium
    # If the premium is null (due to bad data or failed cast) or negative, set to 0.0
    df_clean = df_clean.withColumn("annual_premium", 
        when(col("annual_premium").isNull() | (col("annual_premium") < 0), lit(0.0))
        .otherwise(col("annual_premium"))
    )

    # Add a pipeline metadata column
    df_clean = df_clean.withColumn("load_date", lit("2025-12-15").cast(DateType()))
    
    print(f"Schema after cleaning:")
    df_clean.printSchema()

    # 4. Save to Cleaned Layer
    # Use partitioning by contract status for efficient queries
    print(f"\nWriting cleaned data to: {CLEANED_CONTRACTS_PATH}")
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("contract_status") \
        .parquet(CLEANED_CONTRACTS_PATH)
        
    print(f"--- Contracts data cleaned and saved successfully. ---")
    
    return df_clean

# --- Main Execution ---
if __name__ == "__main__":
    spark = initialize_spark_session()
    
    df_clean_contracts = clean_and_validate_contracts(spark)
    
    # Optional: Show a few clean records and check types
    df_clean_contracts.select("contract_id", "customer_key", "first_name", "annual_premium", "contract_start_date").show(5, truncate=False)
    print(f"Final record count: {df_clean_contracts.count()}")
    
    spark.stop()