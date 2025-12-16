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
    
    @classmethod
    def get_jdbc_properties_for_dimension_read(cls):
        """Return JDBC properties for dimension reads (no partitioning).
        
        Used for reading small dimension tables without parallel partitioning.
        """
        return {
            "user": cls.USER,
            "password": cls.PASSWORD,
            "driver": cls.DRIVER
        }


class PathConfig:
    """Data path configuration."""
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Data directories
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    STAGED_DIR = os.path.join(BASE_DIR, "data", "staged")
    CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")


class SparkConfig:
    """Spark configuration."""
    
    JDBC_JAR_PATH = os.getenv(
        "JDBC_JAR_PATH",
        "C:/Program Files/PostgreSQL/18/jdbc/postgresql-42.7.8.jar"
    )
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
    FACT_CLAIMS = f"{SCHEMA}.fact_claims"
    FACT_DRIVER_RISK = f"{SCHEMA}.fact_driver_risk"


if __name__ == "__main__":
    print("Configuration loaded successfully.")
    print(f"Database: {DatabaseConfig.get_jdbc_url()}")
    print(f"User: {DatabaseConfig.USER}")
    print(f"Base Path: {PathConfig.BASE_DIR}")
    print(f"JDBC JAR: {SparkConfig.JDBC_JAR_PATH}")
