"""
08_load_driver_risk_scores.py - Calculate and Load Driver Risk Scores

Process telematics GPS data to calculate driver risk profiles based on:
- Haversine distance calculation between consecutive GPS points
- Speed analysis from distance/time (with outlier filtering)
- Risk scoring algorithm (0-100 scale)
- Device-to-customer mapping via device_mapping.csv

Risk categories:
  - SAFE: score >= 80
  - MODERATE: score >= 60
  - RISKY: score >= 40
  - VERY_RISKY: score < 40

Speed thresholds (km/h):
  - SPEEDING: > 110 (incident flag)
  - IMPOSSIBLE: > 160 (GPS data quality filter)
  - CAP: 300 (realistic maximum speed)

Inputs:
  - telematics_clean.parquet (GPS event data)
  - device_mapping.csv (deviceId → customer_id mapping)
  - dim_customer (for customer_key lookup)

Output: insurance_dw.fact_driver_risk

Dependencies: PySpark, config, PostgreSQL JDBC driver
"""

import sys
import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, PathConfig, SparkConfig, DatabaseTables


# Configuration
CLEANED_TELEMATICS_PATH = os.path.join(PathConfig.CLEANED_DIR, "telematics_clean.parquet")
DEVICE_MAPPING_PATH = os.path.join(PathConfig.RAW_DIR, "device_mapping.csv")
DRIVER_RISK_TABLE = f"{DatabaseTables.SCHEMA}.fact_driver_risk"

# Speed thresholds for risk calculation (km/h)
SPEED_THRESHOLD_SPEEDING = 110  # Speeding incident threshold
SPEED_THRESHOLD_IMPOSSIBLE = 160  # Filter out unrealistic GPS readings
SPEED_CAP_REALISTIC = 300  # Cap for realistic maximum vehicle speed


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS coordinates using Haversine formula.
    
    Args:
        lat1, lon1: Previous latitude/longitude (in degrees)
        lat2, lon2: Current latitude/longitude (in degrees)
    
    Returns:
        Distance in kilometers
    """
    # Radius of Earth in KM
    R = 6371.0
    
    # Convert degrees to radians
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    
    return R * c


def final_risk_load(spark, df_scored, jdbc_url, jdbc_props, dim_props):
    """Map device-level risk scores to customer dimension and load to fact table.
    
    Joins: 
        df_scored (deviceId) 
        -> device_mapping (customer_id) 
        -> dim_customer (customer_key)
        -> fact_driver_risk table
    
    Args:
        spark: SparkSession
        df_scored: DataFrame with driver risk scores (deviceId, speeding_incidents, avg_speed, max_speed, total_events, driver_risk_score, risk_category)
        jdbc_url: JDBC connection URL
        jdbc_props: JDBC properties for fact table write
        dim_props: JDBC properties for dimension reads (no partitioning)
    """
    print("\n--- Mapping Devices to Customer Keys ---")
    
    try:
        # 1. Load the local Device-to-Customer Mapping (has deviceId and customer_id)
        print(f"[INFO] Loading device mapping from: {DEVICE_MAPPING_PATH}")
        df_mapping = spark.read.csv(DEVICE_MAPPING_PATH, header=True)
        mapping_count = df_mapping.count()
        print(f"[INFO] Device mappings found: {mapping_count}")

        # 2. Read dim_customer from PostgreSQL (has customer_key and customer_id equivalent)
        print("[INFO] Reading dim_customer from database...")
        df_dim_customer = spark.read.jdbc(
            url=jdbc_url,
            table="insurance_dw.dim_customer",
            properties=dim_props
        )
        cust_count = df_dim_customer.count()
        print(f"[INFO] Customers loaded: {cust_count}")

        # 3. Join Chain: Risk Stats -> Mapping (customer_id) -> dim_customer (customer_key)
        print("[INFO] Joining risk scores with device mapping...")
        df_with_mapping = df_scored.join(df_mapping, "deviceId", "inner")
        
        print("[INFO] Joining with customer dimension on customer_id = customer_key...")
        df_final_fact = df_with_mapping.join(
            df_dim_customer,
            F.col("customer_id") == F.col("customer_key"),
            "inner"
        ).select(
            F.col("customer_key"),
            F.col("deviceId"),
            F.col("speeding_incidents"),
            F.col("avg_speed"),
            F.col("max_speed"),
            F.col("total_events"),
            F.col("driver_risk_score"),
            F.col("risk_category")
        )

        final_count = df_final_fact.count()
        print(f"[INFO] Final risk profiles mapped: {final_count}")

        # 4. Write to Warehouse Fact Table
        print(f"[INFO] Writing {final_count} risk profiles to {DRIVER_RISK_TABLE}...")
        df_final_fact.write.jdbc(
            url=jdbc_url,
            table=DRIVER_RISK_TABLE,
            mode="overwrite",
            properties=jdbc_props
        )
        print(f"[OK] Successfully loaded {final_count} risk profiles to {DRIVER_RISK_TABLE}")
        return True

    except Exception as e:
        print(f"[ERROR] Error mapping devices to customers: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        return False


def calculate_risk_scores():
    """Calculate driver risk scores from telematics data.
    
    Risk factors:
    - Speeding incidents (speed > 110 km/h)
    - Average speed patterns
    
    Returns driver score 0-100 (higher = safer driver)
    """
    spark = SparkSession.builder \
        .appName("TelematicsRiskScoring") \
        .config("spark.driver.extraClassPath", SparkConfig.JDBC_JAR_PATH) \
        .getOrCreate()

    JDBC_URL = DatabaseConfig.get_jdbc_url()
    JDBC_PROPS = DatabaseConfig.get_jdbc_properties()
    DIM_PROPS = DatabaseConfig.get_jdbc_properties_for_dimension_read()
    
    print("--- Calculating Driver Risk Scores ---")

    try:
        # 1. Load Cleaned Telematics
        print(f"Loading telematics data from: {CLEANED_TELEMATICS_PATH}")
        df = spark.read.parquet(CLEANED_TELEMATICS_PATH)
        source_count = df.count()
        print(f"[INFO] Source telematics records: {source_count}")

        if source_count == 0:
            print("[WARN] No telematics data found!")
            spark.stop()
            return

        # 2. Filter for Position data only
        print("Filtering for POSITION events...")
        df_pos = df.filter(F.col("variable") == "POSITION") \
                   .select("deviceId", "event_time", "latitude_or_value", "longitude")
        
        pos_count = df_pos.count()
        print(f"[INFO] POSITION events found: {pos_count}")

        if pos_count == 0:
            print("[WARN] No position data found!")
            spark.stop()
            return

        # 3. Calculate Speed between consecutive pings with proper window functions
        print("Calculating speed between consecutive pings...")
        
        # Define Window - partition by device and order by time
        window_spec = Window.partitionBy("deviceId").orderBy("event_time")

        # Create LAG columns first (must be done before calculations)
        print("[INFO] Computing LAG columns for previous coordinates...")
        df_with_lags = df_pos.withColumn("prev_lat", F.lag("latitude_or_value").over(window_spec)) \
                             .withColumn("prev_long", F.lag("longitude").over(window_spec)) \
                             .withColumn("prev_time", F.lag("event_time").over(window_spec))

        # Calculate time difference in seconds using cast to long for numeric comparison
        print("[INFO] Computing time differences...")
        df_calculated = df_with_lags.withColumn(
            "time_diff_sec", 
            (F.col("event_time").cast("long") - F.col("prev_time").cast("long"))
        ).filter(F.col("time_diff_sec") > 0)  # Remove first row of each partition where lag is NULL

        # Calculate distance using Haversine formula
        print("[INFO] Computing Haversine distances...")
        df_calculated = df_calculated.withColumn(
            "dist_km",
            haversine_distance(
                F.col("prev_lat"),
                F.col("prev_long"),
                F.col("latitude_or_value"),
                F.col("longitude")
            )
        ).fillna({"dist_km": 0.0})

        # Calculate speed in km/h with safety checks
        print("[INFO] Computing speed in km/h...")
        df_final_speed = df_calculated.withColumn(
            "speed_kmh",
            # Safe division: check time_diff_sec > 0
            F.when(
                F.col("time_diff_sec") > 0,
                (F.col("dist_km") / F.col("time_diff_sec")) * 3600.0
            ).otherwise(0.0)
        ).withColumn(
            # Cap speed at realistic maximum
            "speed_kmh",
            F.when(F.col("speed_kmh") > SPEED_CAP_REALISTIC, SPEED_CAP_REALISTIC).otherwise(F.col("speed_kmh"))
        ).fillna({"speed_kmh": 0.0})

        # Filter out "Impossible" speeds (GPS Errors)
        # Speeds over threshold are likely GPS errors, not real driving
        print(f"[INFO] Filtering out impossible speeds (> {SPEED_THRESHOLD_IMPOSSIBLE} km/h due to GPS errors)...")
        df_final = df_final_speed.filter(F.col("speed_kmh") < SPEED_THRESHOLD_IMPOSSIBLE)
        
        filtered_count = df_final.count()
        print(f"[INFO] Records after filtering impossible speeds: {filtered_count}")

        # 4. Aggregate Risk Features by Driver
        print("Aggregating risk features by driver...")
        # Recalculate speeding incidents with configured threshold
        df_risk = df_final.groupBy("deviceId").agg(
            F.count(F.when(F.col("speed_kmh") > SPEED_THRESHOLD_SPEEDING, 1)).alias("speeding_incidents"),
            F.avg("speed_kmh").alias("avg_speed"),
            F.max("speed_kmh").alias("max_speed"),
            F.count("event_time").alias("total_events")
        )

        # 5. Create the Risk Score (0-100, where 100 = safest)
        print("Calculating driver risk scores...")
        df_scored = df_risk.withColumn(
            "driver_risk_score", 
            F.when(
                F.col("speeding_incidents") > 0,
                100 - (F.col("speeding_incidents") * 5) - (F.col("avg_speed") / 20)
            ).otherwise(100)
        ).withColumn(
            "driver_risk_score",
            F.when(F.col("driver_risk_score") < 0, 0).otherwise(F.col("driver_risk_score"))
        ).withColumn(
            "risk_category",
            F.when(F.col("driver_risk_score") >= 80, "SAFE")
             .when(F.col("driver_risk_score") >= 60, "MODERATE")
             .when(F.col("driver_risk_score") >= 40, "RISKY")
             .otherwise("VERY_RISKY")
        )

        final_count = df_scored.count()
        print(f"[INFO] Drivers scored: {final_count}")

        # 6. Map devices to customer keys and load to fact table
        if JDBC_URL and JDBC_PROPS and DIM_PROPS:
            success = final_risk_load(spark, df_scored, JDBC_URL, JDBC_PROPS, DIM_PROPS)
            if not success:
                print("[WARN] Device mapping failed - displaying scored data only")
                df_scored.show(10)
        else:
            print("[WARN] JDBC configuration not available - displaying scores only")
            df_scored.show(10)

    except Exception as e:
        print(f"[ERROR] Error calculating risk scores: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    calculate_risk_scores()