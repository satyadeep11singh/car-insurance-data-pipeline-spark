-- sql/create_dw_schema.sql

-- 1. Create the Schema
CREATE SCHEMA IF NOT EXISTS insurance_dw;

-- Set the search path to our new schema
SET search_path TO insurance_dw;

--- --- --- --- --- --- --- --- --- --- ---
-- 2. DIMENSION TABLES
--- --- --- --- --- --- --- --- --- --- ---

-- DIM_CUSTOMER: Stores unique customer attributes (derived from contracts data)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key VARCHAR(50) PRIMARY KEY, -- CLI_000002 (Natural Key, used for FKs)
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    age INTEGER,
    gender_code VARCHAR(10),             -- Expected to be normalized (e.g., 'F', 'M')
    city_postal_code VARCHAR(15),
    customer_segment VARCHAR(50),        -- Worker, Employee, etc.
    load_date DATE NOT NULL
);

-- DIM_POLICY: Stores unique policy/contract attributes
CREATE TABLE IF NOT EXISTS dim_policy (
    contract_key VARCHAR(50) PRIMARY KEY, -- CTR_000002 (Natural Key, used for FKs)
    product_type VARCHAR(50) NOT NULL,    -- Auto, Home, Life
    risk_zone VARCHAR(50),
    sales_channel VARCHAR(50),            -- Phone, Web, Agency
    contract_status VARCHAR(20),          -- Active, Renewed, Lapsed
    load_date DATE NOT NULL
);


-- DIM_DATE: Standard calendar dimension (populated by PySpark in the next step)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,         -- YYYYMMDD
    full_date DATE NOT NULL,
    calendar_year INTEGER NOT NULL,
    calendar_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN
);

--- --- --- --- --- --- --- --- --- --- ---
-- 3. FACT TABLES (CORE MEASURES)
--- --- --- --- --- --- --- --- --- --- ---

-- FACT_POLICY_SNAPSHOT: Contains the measures and foreign keys related to policy metrics
CREATE TABLE IF NOT EXISTS fact_policy_snapshot (
    policy_fact_id BIGSERIAL PRIMARY KEY,
    
    -- Foreign Keys (Linking to Dimension Tables)
    contract_key VARCHAR(50) REFERENCES dim_policy(contract_key),
    customer_key VARCHAR(50) REFERENCES dim_customer(customer_key),
    
    -- Date Keys (Using both start and end date for policy period)
    start_date_key INTEGER REFERENCES dim_date(date_key), 
    end_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Measures
    annual_premium DOUBLE PRECISION NOT NULL,
    
    -- Metadata
    load_date DATE NOT NULL
);

-- Indexing for performance
CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON fact_policy_snapshot(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_policy_key ON fact_policy_snapshot(contract_key);