"""
07_load_fact_claims.py - Load Claims Fact Table

Create and load the claims fact table (fact_claims) by joining claims source data
with fact_policy_snapshot and date dimension.

Join path:
  claims_clean.parquet
    + fact_policy_snapshot (on contract_id → policy_key) → gets customer_key, policy_key
    + dim_date (on occurrence_date) → gets claim_date_key

Fact table dimensions and measures:
  - customer_key, policy_key (surrogate keys from dimensions)
  - claim_date_key (date surrogate key)
  - claim_id (degenerate dimension)
  - claim_amount (damage_amount cast to decimal)
  - claim_status, claim_type, liability (categorical fields)

Output: insurance_dw.fact_claims

Null handling: Fills with defaults (0.0, 'UNKNOWN', 'OTHER')
Join type: INNER (drops unmatched claims)

Dependencies: PySpark, config, PostgreSQL JDBC driver
"""
import sys
import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, PathConfig, SparkConfig, DatabaseTables


# Add missing path to config
CLEANED_CLAIMS_PATH = os.path.join(PathConfig.CLEANED_DIR, "claims_clean.parquet")
FACT_CLAIMS_TABLE = f"{DatabaseTables.SCHEMA}.fact_claims"


def load_claims_fact():
    """Load claims fact table by joining claims data with fact_policy_snapshot and date dimension.
    
    Joins path: claims (contract_id) -> fact_policy_snapshot (get customer_key, policy_key)
                + occurrence_date -> dim_date (get claim_date_key)
    """
    spark = SparkSession.builder \
        .appName("ClaimsFactLoading") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    # Configuration
    JDBC_URL = DatabaseConfig.get_jdbc_url()
    JDBC_PROPS = DatabaseConfig.get_jdbc_properties()
    DIM_PROPS = DatabaseConfig.get_jdbc_properties_for_dimension_read()
    
    print("--- Loading Claims Fact Table ---")

    try:
        # 1. Read Cleaned Claims (Source)
        print(f"Reading cleaned claims from: {CLEANED_CLAIMS_PATH}")
        df_claims_raw = spark.read.parquet(CLEANED_CLAIMS_PATH)
        source_count = df_claims_raw.count()
        print(f"[INFO] Source claims records: {source_count}")

        if source_count == 0:
            print("[WARN] No claims data found!")
            spark.stop()
            return

        # 2. Read fact_policy_snapshot to get customer_key and policy_key mappings
        print("Reading fact_policy_snapshot for customer and policy keys...")
        df_fact_policy = spark.read.jdbc(JDBC_URL, DatabaseTables.FACT_POLICY_SNAPSHOT, properties=JDBC_PROPS) \
            .select("customer_key", "policy_key")
        
        print(f"[INFO] Fact policy records: {df_fact_policy.count()}")

        # Read dim_date for claim date join
        print("Reading date dimension...")
        df_dim_date = spark.read.jdbc(JDBC_URL, DatabaseTables.DIM_DATE, properties=DIM_PROPS) \
            .select("date_key")

        # 3. Join claims with fact_policy_snapshot using contract_id == policy_key
        # Note: policy_key in fact_policy_snapshot equals contract_id (created in 04_load_facts.py)
        print("Joining claims with fact_policy_snapshot...")
        df_claims_fact = df_claims_raw.alias("c") \
            .join(df_fact_policy.alias("fp"), col("c.contract_id") == col("fp.policy_key"), "inner")

        joined_count = df_claims_fact.count()
        print(f"[INFO] Records after policy join: {joined_count}")

        # 4. Handle Dates (occurrence_date for claim date dimension)
        # Convert ISO timestamp string to date format for dimension join
        df_claims_fact = df_claims_fact.withColumn(
            "claim_date_only",
            F.to_date(to_timestamp(col("c.occurrence_date")))
        ).withColumn(
            "claim_date_key_nk",
            F.date_format(col("claim_date_only"), "yyyyMMdd").cast("int")
        )

        # Prepare date dimension for join
        df_dim_date_prepared = df_dim_date.withColumn(
            "date_key_sk",
            F.date_format(col("date_key"), "yyyyMMdd").cast("int")
        ).drop("date_key")

        # Join with date dimension
        df_claims_fact = df_claims_fact.join(
            df_dim_date_prepared,
            df_claims_fact["claim_date_key_nk"] == df_dim_date_prepared["date_key_sk"],
            "inner"
        ).drop("claim_date_key_nk", "claim_date_only")

        print(f"[INFO] Records after date join: {df_claims_fact.count()}")

        # 5. Select Final Columns & Measures
        print("Selecting final columns...")
        df_final = df_claims_fact.select(
            col("fp.customer_key"),
            col("fp.policy_key"),
            col("date_key_sk").alias("claim_date_key"),
            col("c.claim_id"),  # Degenerate Dimension
            col("c.damage_amount").cast("decimal(18,2)").alias("claim_amount"),
            col("c.claim_status"),
            col("c.claim_type"),
            col("c.liability")
        )

        # Fill nulls with defaults
        df_final = df_final.fillna({
            "claim_amount": 0.0,
            "claim_status": "UNKNOWN",
            "claim_type": "OTHER",
            "liability": "UNKNOWN"
        })

        final_count = df_final.count()
        print(f"[INFO] Final records for fact table: {final_count}")

        # 6. Write to PostgreSQL
        print(f"Writing {final_count} claims to {FACT_CLAIMS_TABLE}...")
        df_final.write.jdbc(
            url=JDBC_URL, 
            table=FACT_CLAIMS_TABLE, 
            mode="overwrite", 
            properties=JDBC_PROPS
        )

        print("[OK] Claims Fact Table Loaded Successfully!")

    except Exception as e:
        print(f"[ERROR] Error loading claims fact table: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    load_claims_fact()