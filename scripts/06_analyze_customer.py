import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import configuration
from config import DatabaseConfig, SparkConfig, DatabaseTables

# --- Configuration ---
JDBC_URL = DatabaseConfig.get_jdbc_url()
JDBC_PROPERTIES = DatabaseConfig.get_jdbc_properties()

FACT_TABLE = DatabaseTables.FACT_POLICY_SNAPSHOT
DIM_CUSTOMER = DatabaseTables.DIM_CUSTOMER


def analyze_customer_data(spark: SparkSession):
    print("--- Starting PySpark Customer Dimensional Analysis ---")

    # 1. Read the Fact Table
    df_fact = spark.read.jdbc(
        url=JDBC_URL,
        table=FACT_TABLE,
        properties=JDBC_PROPERTIES
    )

    # 2. Read the Customer Dimension
    print(f"Reading Customer Dimension: {DIM_CUSTOMER}")
    df_dim_customer = spark.read.jdbc(
        url=JDBC_URL,
        table=DIM_CUSTOMER,
        properties=JDBC_PROPERTIES
    ).select(
        F.col("customer_key"),
        F.col("customer_segment") # Using the confirmed column name
    )

    # 3. Join Fact and Customer Dimension on the Surrogate Key
    print("Joining Fact table to Customer Dimension...")
    df_analysis = df_fact.join(
        df_dim_customer,
        df_fact["customer_key"] == df_dim_customer["customer_key"],
        "inner"
    )

    # 4. Perform Aggregation (Total Premium by Customer Segment)
    print("Aggregating Total Premium by Customer Segment...")
    df_results = df_analysis.groupBy(
        F.col("customer_segment")
    ).agg(
        F.sum("total_premium").alias("total_premium_by_segment"),
        F.sum("policy_count").alias("total_policies")
    ).orderBy(
        F.col("total_premium_by_segment").desc()
    )

    # 5. Display Results
    print("\n--- ANALYSIS RESULTS: Premium & Policies by Customer Segment ---")
    df_results.show()
    print("--- PySpark Customer Analysis Complete ---")


# --- Entry Point ---
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CustomerAnalysis") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    analyze_customer_data(spark)

    spark.stop()