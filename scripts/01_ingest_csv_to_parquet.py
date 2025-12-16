"""
01_ingest_csv_to_parquet.py - CSV Ingestion and Parquet Staging

Read raw CSV files and convert them to Parquet format for efficient processing.
This is the first step in the ETL pipeline (Extract phase).

Datasets ingested:
- contracts.csv → contracts.parquet
- vehicles.csv → vehicles.parquet
- claims.csv → claims.parquet
- Telematicsdata.csv → telematics_raw.parquet

Basic cleaning: removes completely empty rows

Dependencies: pandas, os
Outputs: Parquet files in ../data/staged/
"""
import pandas as pd
import os

# --- Configuration: Define File Mappings ---
# Key: Name used in the script (for clarity)
# Value: (Raw CSV Filename, Staged Parquet Filename)
FILE_MAP = {
    "POLICY_CONTRACTS": ("contracts.csv", "contracts.parquet"),
    "VEHICLES": ("vehicles.csv", "vehicles.parquet"),
    "CLAIMS": ("claims.csv", "claims.parquet"),
    # Telematics data is treated as a very large batch/historical stream snapshot for staging
    "TELEMATICS": ("Telematicsdata.csv", "telematics_raw.parquet")
}

RAW_DIR = '../data/raw'
STAGED_DIR = '../data/staged'

def ingest_and_stage_file(source_name, raw_filename, staged_filename):
    """Reads raw CSV, performs basic cleanup, and stages as Parquet."""
    raw_path = os.path.join(RAW_DIR, raw_filename)
    staged_path = os.path.join(STAGED_DIR, staged_filename)

    print(f"\n--- Processing {source_name} ---")
    print(f"Reading raw data from: {raw_path}")
    
    # 1. Extract (Read Raw Data)
    try:
        # low_memory=False is a good practice for large, unknown datasets
        df = pd.read_csv(raw_path, low_memory=False)
    except FileNotFoundError:
        print(f"🛑 ERROR: Raw file not found at {raw_path}. Please place it in the {RAW_DIR} folder.")
        return False

    # 2. Basic Cleaning (Minimal Transform for Staging)
    rows_before = len(df)
    
    # Drop records where ALL columns are null/empty.
    # This is a good way to handle unexpected blank lines at the end of files.
    df_cleaned = df.dropna(how='all')
    rows_after = len(df_cleaned)

    print(f"Rows before cleaning: {rows_before}. Rows after cleaning: {rows_after}")
    
    # 3. Load (Stage Data as Parquet)
    os.makedirs(STAGED_DIR, exist_ok=True)
    
    # Use Parquet format for efficient reading in PySpark later
    df_cleaned.to_parquet(staged_path, index=False)
    
    print(f"✅ Staged successfully at: {staged_path}")
    return True

def main():
    """Main function to run staging for all files."""
    success_count = 0
    total_files = len(FILE_MAP)
    
    for source_name, (raw_file, staged_file) in FILE_MAP.items():
        if ingest_and_stage_file(source_name, raw_file, staged_file):
            success_count += 1
            
    print(f"\n======================================")
    print(f"STAGING COMPLETE: {success_count} of {total_files} files staged.")
    print(f"Your staged data is now ready in the '{STAGED_DIR}' folder.")
    print(f"======================================")

if __name__ == "__main__":
    main()

# After running this script, the data will be ready for PySpark.