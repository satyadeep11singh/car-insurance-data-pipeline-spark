import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import configuration
from config import DatabaseConfig, SparkConfig, DatabaseTables

# --- Configuration (Use the same connection details) ---
JDBC_URL = DatabaseConfig.get_jdbc_url()
JDBC_PROPERTIES = DatabaseConfig.get_jdbc_properties()

FACT_TABLE = DatabaseTables.FACT_POLICY_SNAPSHOT
DIM_DATE = DatabaseTables.DIM_DATE


def analyze_data(spark: SparkSession):
    print("--- Starting PySpark Dimensional Analysis ---")

    # 1. Read the Fact Table
    print(f"Reading Fact Table: {FACT_TABLE}")
    df_fact = spark.read.jdbc(
        url=JDBC_URL,
        table=FACT_TABLE,
        properties=JDBC_PROPERTIES
    )

    # 2. Read the Date Dimension with the CORRECT column names
    print(f"Reading Date Dimension: {DIM_DATE}")
    df_dim_date = spark.read.jdbc(
        url=JDBC_URL,
        table=DIM_DATE,
        properties=JDBC_PROPERTIES
    ).select(
        F.col("date_key").alias("date_key_nk"), # Renaming the DATE column for clarity in the join
        F.col("year"),                          # Actual column name
        F.col("month_name"),                    # Actual column name
        F.col("month")                          # Actual column name (for sorting)
    )

    # To join fact's INTEGER key (YYYYMMDD) with DIM's DATE key, 
    # we explicitly convert the Fact's date_key_nk to the INTEGER format
    df_dim_date_prepared = df_dim_date.withColumn(
        "load_date_key_sk", 
        F.date_format(F.col("date_key_nk"), "yyyyMMdd").cast("int")
    )

    # 3. Join Fact and Dimension on the Surrogate Key
    print("Joining Fact table to Date Dimension...")
    df_analysis = df_fact.join(
        df_dim_date_prepared,
        df_fact["load_date_key"] == df_dim_date_prepared["load_date_key_sk"],
        "inner"
    ).drop("date_key_nk", "load_date_key_sk")

    # 4. Perform Aggregation (Total Premium by Year and Month)
    print("Aggregating Total Premium and Policies by Year and Month...")
    df_results = df_analysis.groupBy(
        F.col("year"),
        F.col("month_name"),
        F.col("month") # Group by month number to enable correct sorting
    ).agg(
        F.sum("total_premium").alias("total_premium_sold"),
        F.sum("policy_count").alias("total_policies")
    ).orderBy(
        F.col("year"),
        F.col("month") # Order by month number (1, 2, 3...)
    ).drop("month") # Drop the month number column before showing

    # 5. Display Results
    print("\n--- ANALYSIS RESULTS: Premium & Policies by Load Date ---")
    df_results.show()
    print(f"Total rows in result: {df_results.count()}")
    print("--- PySpark Analysis Complete ---")


# --- Entry Point ---
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FactAnalysis") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    analyze_data(spark)

    spark.stop()