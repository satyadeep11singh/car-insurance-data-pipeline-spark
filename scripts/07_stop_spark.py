from pyspark.sql import SparkSession

def cleanup_spark():
    """
    Stops the currently running Spark session.
    """
    print("--- Attempting to stop Spark Session ---")
    try:
        # Get the currently active Spark Session
        spark = SparkSession.builder.getOrCreate()
        spark.stop()
        print(" Spark Session stopped successfully.")
    except Exception as e:
        print(f" Could not stop Spark Session: {e}")
        print("It may already be stopped or not running.")

# --- Entry Point ---
if __name__ == "__main__":
    # Note: We need to set up the connection details again if running via spark-submit
    # to ensure the entry point succeeds, but the stop operation is quick.
    spark = SparkSession.builder \
        .appName("Cleanup") \
        .config("spark.driver.extraClassPath", "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar") \
        .getOrCreate()

    spark.stop()
    print(" PySpark resources cleanly shut down.")