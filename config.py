"""
config.py - Centralized Configuration Management

This module handles all configuration including database credentials,
paths, and JDBC settings using environment variables.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()


class DatabaseConfig:
    """Database configuration from environment variables."""
    
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("DB_PORT", "5432")
    DATABASE = os.getenv("DB_NAME", "insurance_dw_db")
    USER = os.getenv("DB_USER", "satyadeep")
    PASSWORD = os.getenv("DB_PASSWORD", "")
    DRIVER = os.getenv("DB_DRIVER", "org.postgresql.Driver")
    
    @classmethod
    def get_jdbc_url(cls):
        """Generate JDBC URL from configuration."""
        return f"jdbc:postgresql://{cls.HOST}:{cls.PORT}/{cls.DATABASE}"
    
    @classmethod
    def get_jdbc_properties(cls):
        """Return JDBC properties dictionary."""
        return {
            "user": cls.USER,
            "password": cls.PASSWORD,
            "driver": cls.DRIVER
        }


class PathConfig:
    """Data path configuration."""
    
    # Base directory (relative to scripts folder)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Data directories
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    STAGED_DIR = os.path.join(BASE_DIR, "data", "staged")
    CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
    
    # Data files
    STAGED_CONTRACTS_PATH = os.path.join(STAGED_DIR, "contracts.parquet")
    CLEANED_CONTRACTS_PATH = os.path.join(CLEANED_DIR, "contracts_clean.parquet")


class SparkConfig:
    """Spark configuration."""
    
    # JDBC JAR path
    JDBC_JAR_PATH = os.getenv(
        "JDBC_JAR_PATH",
        "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar"
    )
    
    # Spark settings
    LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "ERROR")


class DatabaseTables:
    """Database table names (schema.table format)."""
    
    SCHEMA = "insurance_dw"
    
    # Dimension tables
    DIM_CUSTOMER = f"{SCHEMA}.dim_customer"
    DIM_POLICY = f"{SCHEMA}.dim_policy"
    DIM_DATE = f"{SCHEMA}.dim_date"
    
    # Fact tables
    FACT_POLICY_SNAPSHOT = f"{SCHEMA}.fact_policy_snapshot"


if __name__ == "__main__":
    # Quick validation
    print("Database Config:")
    print(f"  URL: {DatabaseConfig.get_jdbc_url()}")
    print(f"  User: {DatabaseConfig.USER}")
    print("\nPath Config:")
    print(f"  Base: {PathConfig.BASE_DIR}")
    print(f"  Raw: {PathConfig.RAW_DIR}")
    print(f"  Staged: {PathConfig.STAGED_DIR}")
    print(f"  Cleaned: {PathConfig.CLEANED_DIR}")
    print("\nSparkConfig:")
    print(f"  JDBC JAR: {SparkConfig.JDBC_JAR_PATH}")
    print(f"  Log Level: {SparkConfig.LOG_LEVEL}")
    print("\nDatabase Tables:")
    print(f"  Customer: {DatabaseTables.DIM_CUSTOMER}")
    print(f"  Policy: {DatabaseTables.DIM_POLICY}")
    print(f"  Fact: {DatabaseTables.FACT_POLICY_SNAPSHOT}")
