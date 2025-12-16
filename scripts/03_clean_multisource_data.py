"""
03_clean_multisource_data.py - Multi-Source Data Cleaning and Validation

Unified cleaning and validation for multiple data sources (Vehicles, Claims, Telematics).
Transforms staged Parquet data into cleaned, validated Parquet files with proper schema enforcement.

Datasets processed:
- Vehicles: Parse HP from "128 HP", remove € from market value
- Claims: Handle two date formats (dd-MM-yyyy and yyyy-MM-dd), remove currency
- Telematics: Parse Unix milliseconds to timestamps, extract GPS coordinates (lat/lon/alt)

Inputs: Parquet files in ../data/staged/
Outputs: Cleaned Parquet files in ../data/cleaned/

Dependencies: PySpark, config, typing, os
"""

import sys
import os
from typing import Optional

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, lit, when, regexp_replace, split, from_unixtime
from pyspark.sql.types import IntegerType, DateType, DoubleType, TimestampType

# Import configuration
from config import PathConfig, SparkConfig

# --- Configuration ---
RAW_BASE = PathConfig.STAGED_DIR
CLEAN_BASE = PathConfig.CLEANED_DIR


def initialize_spark() -> SparkSession:
    """
    Initialize and return a Spark Session with error handling.
    
    Returns:
        SparkSession: Configured Spark session for data processing
    """
    try:
        spark = SparkSession.builder.appName("InsuranceUnifiedCleaning").getOrCreate()
        spark.sparkContext.setLogLevel(SparkConfig.LOG_LEVEL)
        print(" PySpark Session Initialized")
        return spark
    except Exception as e:
        print(f" ERROR: Failed to initialize Spark Session: {e}")
        raise


def clean_vehicles(spark: SparkSession) -> Optional[DataFrame]:
    """
    Clean and validate vehicle data.
    
    Transformations:
    - Extract horsepower from "128 HP" format
    - Remove € currency symbol from market value
    - Cast year and previous claims to integer
    
    Args:
        spark: SparkSession
        
    Returns:
        DataFrame: Cleaned vehicle data or None if error
    """
    try:
        input_path = os.path.join(RAW_BASE, "vehicles.parquet")
        output_path = os.path.join(CLEAN_BASE, "vehicles_clean.parquet")
        
        print("--- Processing Vehicles ---")
        
        # Check if input file exists
        try:
            df = spark.read.parquet(input_path)
            rows_in = df.count()
            print(f"Input records: {rows_in}")
        except Exception as e:
            print(f" Warning: Could not read vehicles.parquet: {e}")
            return None
        
        df_clean = df.select(
            col("contract_id"),
            col("brand"),
            col("model"),
            col("year").cast(IntegerType()).alias("manufacture_year"),
            # Sample: "128 HP" -> 128
            regexp_replace(col("power"), " HP", "").cast(IntegerType()).alias("horsepower"),
            col("fuel_type"),
            # Sample: "29567.77€" -> 29567.77
            regexp_replace(col("current_value"), "€", "").cast(DoubleType()).alias("market_value"),
            col("color"),
            col("usage").alias("usage_type"),
            col("previous_claims").cast(IntegerType())
        )
        
        # Null Handling Strategy for Vehicles:
        # - manufacture_year: Default to 0 (unknown year)
        # - horsepower: Default to 0 (unknown power)
        # - market_value: Default to 0.0 (no value)
        # - previous_claims: Default to 0 (no claims)
        # - Other string fields: Keep as null for quality control
        df_clean = df_clean.fillna({
            "manufacture_year": 0,
            "horsepower": 0,
            "market_value": 0.0,
            "previous_claims": 0
        })
        
        # Log null counts before write
        null_counts = {col_name: df_clean.filter(col(col_name).isNull()).count() 
                      for col_name in ["manufacture_year", "horsepower", "market_value", "previous_claims"]}
        if any(null_counts.values()):
            print(f"Filled nulls - manufacture_year: {null_counts['manufacture_year']}, "
                  f"horsepower: {null_counts['horsepower']}, "
                  f"market_value: {null_counts['market_value']}, "
                  f"previous_claims: {null_counts['previous_claims']}")
        
        # Ensure output directory exists
        os.makedirs(CLEAN_BASE, exist_ok=True)
        
        df_clean.write.mode("overwrite").parquet(output_path)
        rows_out = df_clean.count()
        print(f"Output records: {rows_out}")
        print(f" Vehicles cleaned successfully")
        
        return df_clean
        
    except Exception as e:
        print(f" ERROR in clean_vehicles: {e}")
        return None


def clean_claims(spark: SparkSession) -> Optional[DataFrame]:
    """
    Clean and validate claims data.
    
    Transformations:
    - Handle two date formats: dd-MM-yyyy and yyyy-MM-dd
    - Remove € currency symbol from amounts
    - Fill null values with 0.0
    - Cast claims count to integer
    
    Args:
        spark: SparkSession
        
    Returns:
        DataFrame: Cleaned claims data or None if error
    """
    try:
        input_path = os.path.join(RAW_BASE, "claims.parquet")
        output_path = os.path.join(CLEAN_BASE, "claims_clean.parquet")
        
        print("--- Processing Claims ---")
        
        # Check if input file exists
        try:
            df = spark.read.parquet(input_path)
            rows_in = df.count()
            print(f"Input records: {rows_in}")
        except Exception as e:
            print(f" Warning: Could not read claims.parquet: {e}")
            return None
        
        df_clean = df.select(
            col("claim_id"),
            col("contract_id"),
            # Logic: Handles '26-11-2023' (dd-MM-yyyy) OR '2023-10-02' (yyyy-MM-dd)
            when(col("occurrence_date").like("__-__-____"), to_date(col("occurrence_date"), "dd-MM-yyyy"))
            .otherwise(to_date(col("occurrence_date"), "yyyy-MM-dd")).alias("occurrence_date"),
            to_date(col("declaration_date"), "yyyy-MM-dd").alias("declaration_date"),
            col("claim_type"),
            regexp_replace(col("damage_amount"), "€", "").cast(DoubleType()).alias("damage_amount"),
            regexp_replace(col("indemnified_amount"), "€", "").cast(DoubleType()).alias("indemnified_amount"),
            col("status").alias("claim_status"),
            col("expert_id"),
            col("liability")
        ).fillna(0.0, subset=["damage_amount", "indemnified_amount"])
        
        # Ensure output directory exists
        os.makedirs(CLEAN_BASE, exist_ok=True)
        
        df_clean.write.mode("overwrite").parquet(output_path)
        rows_out = df_clean.count()
        print(f"Output records: {rows_out}")
        print(f" Claims cleaned successfully")
        
        return df_clean
        
    except Exception as e:
        print(f" ERROR in clean_claims: {e}")
        return None


def clean_telematics(spark: SparkSession) -> Optional[DataFrame]:
    """
    Clean and validate telematics data.
    
    Transformations:
    - Convert Unix milliseconds to timestamp
    - Parse GPS coordinates from comma-separated values
    - Extract latitude, longitude, altitude for POSITION records
    - Handle different variable types (POSITION, BATTERY, etc.)
    
    Args:
        spark: SparkSession
        
    Returns:
        DataFrame: Cleaned telematics data or None if error
    """
    try:
        input_path = os.path.join(RAW_BASE, "telematics_raw.parquet")
        output_path = os.path.join(CLEAN_BASE, "telematics_clean.parquet")
        
        print("--- Processing Telematics ---")
        
        # Check if input file exists
        try:
            df = spark.read.parquet(input_path)
            rows_in = df.count()
            print(f"Input records: {rows_in}")
        except Exception as e:
            print(f" Warning: Could not read telematics_raw.parquet: {e}")
            return None
        
        # Process Unix Milli and split the comma-separated GPS value
        df_clean = df.withColumn("event_time", from_unixtime(col("timeMili") / 1000).cast(TimestampType())) \
                     .withColumn("pos_split", split(col("value"), ","))

        df_final = df_clean.select(
            col("deviceId"),
            col("event_time"),
            col("timestamp").alias("original_timestamp"),
            col("variable"),
            col("alarmClass").cast(IntegerType()),
            # Sample 1 (Battery): "86.0" -> numeric
            # Sample 2 (Position): "13.330059,74.74467,-12.0" -> [13.33, 74.74, -12.0]
            when(col("variable") == "POSITION", col("pos_split").getItem(0).cast(DoubleType()))
            .otherwise(col("value").cast(DoubleType())).alias("latitude_or_value"),
            
            when(col("variable") == "POSITION", col("pos_split").getItem(1).cast(DoubleType())).alias("longitude"),
            when(col("variable") == "POSITION", col("pos_split").getItem(2).cast(DoubleType())).alias("altitude")
        )
        
        # Null Handling Strategy for Telematics:
        # - alarmClass: Default to -1 (unknown/no alarm)
        # - latitude_or_value: Default to 0.0 (unknown value or position)
        # - longitude: Default to 0.0 (unknown position)
        # - altitude: Default to 0.0 (ground level)
        # - deviceId, event_time, variable: Keep as null for quality control
        df_final = df_final.fillna({
            "alarmClass": -1,
            "latitude_or_value": 0.0,
            "longitude": 0.0,
            "altitude": 0.0
        })
        
        # Log null counts before write
        null_counts = {col_name: df_final.filter(col(col_name).isNull()).count() 
                      for col_name in ["alarmClass", "latitude_or_value", "longitude", "altitude"]}
        if any(null_counts.values()):
            print(f"Filled nulls - alarmClass: {null_counts['alarmClass']}, "
                  f"latitude_or_value: {null_counts['latitude_or_value']}, "
                  f"longitude: {null_counts['longitude']}, "
                  f"altitude: {null_counts['altitude']}")
        
        # Ensure output directory exists
        os.makedirs(CLEAN_BASE, exist_ok=True)
        
        df_final.write.mode("overwrite").parquet(output_path)
        rows_out = df_final.count()
        print(f"Output records: {rows_out}")
        print(f" Telematics cleaned successfully")
        
        return df_final
        
    except Exception as e:
        print(f" ERROR in clean_telematics: {e}")
        return None


def main() -> None:
    """
    Main orchestration function for multi-table cleaning.
    Processes all three data sources and validates completion.
    """
    print("=" * 50)
    print("UNIFIED CLEANING AND VALIDATION")
    print("=" * 50)
    
    spark = initialize_spark()
    
    try:
        # Clean all data sources
        vehicles_cleaned = clean_vehicles(spark)
        claims_cleaned = clean_claims(spark)
        telematics_cleaned = clean_telematics(spark)
        
        # Summary
        print("\n" + "=" * 50)
        print("CLEANING SUMMARY")
        print("=" * 50)
        
        success_count = sum([
            1 for df in [vehicles_cleaned, claims_cleaned, telematics_cleaned]
            if df is not None
        ])
        
        print(f" Successfully cleaned: {success_count} of 3 datasets")
        
        if success_count == 3:
            print(" All cleaning operations completed successfully!")
        else:
            print(" Some datasets were not processed. Check warnings above.")
        
        print("=" * 50)
        
    except Exception as e:
        print(f" FATAL ERROR during cleaning: {e}")
        raise
    finally:
        spark.stop()
        print("PySpark Session stopped")


# --- Main Execution ---
if __name__ == "__main__":
    main()