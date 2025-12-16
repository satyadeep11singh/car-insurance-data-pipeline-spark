"""
06_validate_data_quality.py - Data Quality Validation and Reporting

Perform comprehensive QA checks on the fact_policy_snapshot table:
- Record count
- Schema validation (check for expected columns)
- NULL counts for surrogate keys (customer_key, policy_key, load_date_key, vehicle_key)
- Vehicle coverage analysis (policies with/without vehicle_key)
- Premium statistics (sum, avg, min, max)

Validation approach: Flexible column checking (only validates columns that exist)
Output: Console reporting with [OK]/[WARN] status indicators

Dependencies: PySpark, config
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os
import traceback

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, DatabaseTables, SparkConfig

def run_validation():
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    # Get URL and basic auth properties
    jdbc_url = DatabaseConfig.get_jdbc_url()
    jdbc_props = DatabaseConfig.get_jdbc_properties()

    table_name = DatabaseTables.FACT_POLICY_SNAPSHOT

    print(f"--- Validating Table: {table_name} ---")

    try:
        # Read fact table
        df_fact = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_props)
        
        total_count = df_fact.count()
        print(f"Total Fact Records: {total_count}")
        
        if total_count == 0:
            print("WARNING: Fact table is empty!")
            spark.stop()
            return

        # Get schema to check for expected columns
        schema_cols = df_fact.columns
        print(f"\nFact Table Columns: {schema_cols}")

        # Check for NULL Surrogate Keys - Only check columns that exist
        required_keys = ["customer_key", "policy_key", "load_date_key"]
        null_results = {}
        
        for col_name in required_keys:
            if col_name in schema_cols:
                null_count = df_fact.filter(F.col(col_name).isNull()).count()
                null_results[col_name] = null_count
                status = "[OK]" if null_count == 0 else "[WARN]"
                print(f"{status} NULL {col_name}: {null_count}")

        # Check for vehicle_key if it exists
        if "vehicle_key" in schema_cols:
            veh_null_count = df_fact.filter(F.col("vehicle_key").isNull()).count()
            veh_present = total_count - veh_null_count
            status = "[OK]" if veh_null_count == 0 else "[WARN]"
            print(f"{status} Policies with Vehicles: {veh_present}/{total_count}")
            print(f"   Policies without Vehicles: {veh_null_count}")
        else:
            print("[WARN] vehicle_key column not found in fact table")

        # Premium validation
        if "total_premium" in schema_cols:
            premium_stats = df_fact.agg(
                F.sum("total_premium").alias("total_premium"),
                F.avg("total_premium").alias("avg_premium"),
                F.min("total_premium").alias("min_premium"),
                F.max("total_premium").alias("max_premium")
            ).collect()[0]
            
            print(f"\nPremium Statistics:")
            print(f"   Total Premium: {premium_stats['total_premium']}")
            print(f"   Avg Premium: {premium_stats['avg_premium']:.2f}")
            print(f"   Min Premium: {premium_stats['min_premium']}")
            print(f"   Max Premium: {premium_stats['max_premium']}")

        print("\nValidation Complete!")

    except Exception as e:
        print(f"Error during validation: {e}")
        print(f"\nFull traceback:")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    run_validation()