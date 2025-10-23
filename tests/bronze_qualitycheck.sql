/*
================================================================================
Data Quality Checks – Bronze Layer: crm_prd_info
================================================================================
Purpose:
    This script performs a series of data validation checks on the 'bronze.crm_prd_info' table
    to ensure data integrity, consistency, and readiness for promotion to the Silver layer.

Checks Included:
    - Primary key integrity (nulls, duplicates)
    - Record-level inspection for specific IDs
    - Latest record flagging using ROW_NUMBER
    - Category ID mapping validation
    - String formatting and whitespace cleanup
    - Defaulting nulls for numeric fields
    - Date sequencing validation
================================================================================
*/

-- ==============================================================================
-- Check 1: Detect NULLs or Duplicates in Primary Key (prd_id)
-- Expectation: No results returned
-- ==============================================================================
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ==============================================================================
-- Check 2: Inspect records for a specific duplicate prd_id (e.g., 29466)
-- ==============================================================================
SELECT *
FROM bronze.crm_prd_info
WHERE prd_id = 29466;

-- ==============================================================================
-- Check 3: Flag latest record per prd_id using ROW_NUMBER
-- ==============================================================================
SELECT *, ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_prd_info
WHERE prd_id = 29466;

-- ==============================================================================
-- Check 4: Identify non-latest records for duplicate prd_id values
-- ==============================================================================
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_prd_info
) t
WHERE flag_last != 1;

-- ==============================================================================
-- Check 5: Validate category ID mapping from prd_key to reference table
-- Expectation: All cat_id values should exist in bronze.erp_px_cat_g1v2
-- ==============================================================================
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
);

-- ==============================================================================
-- Check 6: Parse prd_key into category ID and product key components
-- ==============================================================================
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- ==============================================================================
-- Check 7: Detect unwanted leading/trailing spaces in product name
-- ==============================================================================
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- ==============================================================================
-- Check 8: Apply default value for null product cost and re-parse prd_key
-- ==============================================================================
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- ==============================================================================
-- Check 9: Validate date sequencing using LEAD function
-- Objective: Ensure prd_start_dt precedes the next record's start date
-- ==============================================================================
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM bronze.crm_prd_info;
