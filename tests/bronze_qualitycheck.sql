/*
================================================================================
Data Quality Checks – Bronze Layer
================================================================================
Purpose:
    This script performs integrity, formatting, and standardization checks on
    the bronze table to ensure clean and consistent customer data.

Checks Included:
    - Primary key validation (nulls, duplicates)
    - Record-level inspection for specific IDs
    - Latest record flagging using ROW_NUMBER
    - Whitespace cleanup in string fields
    - Gender and marital status standardization
    - Value profiling for categorical fields
================================================================================
*/

/*
================================================================================
Data Quality Checks – Bronze Layer: crm_cust_info
================================================================================
*/

-- ==============================================================================
-- Check 1: Detect NULLs or Duplicates in Primary Key (cst_id)
-- Expectation: No results returned
-- ==============================================================================
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- ==============================================================================
-- Check 2: Inspect records for a specific duplicate cst_id (e.g., 29466)
-- ==============================================================================
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- ==============================================================================
-- Check 3: Flag latest record per cst_id using ROW_NUMBER
-- ==============================================================================
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- ==============================================================================
-- Check 4: Identify non-latest records for duplicate cst_id values
-- ==============================================================================
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last != 1;

-- ==============================================================================
-- Check 5: Detect unwanted leading/trailing spaces in string fields
-- ==============================================================================
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- ==============================================================================
-- Check 6: Standardize gender and marital status values
-- ==============================================================================
SELECT 
    cst_id, 
    cst_key, 
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname, 
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr, 
    cst_create_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last != 1;

-- ==============================================================================
-- Check 7: Profile distinct gender values for consistency review
-- ==============================================================================
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
/*
================================================================================
Data Quality Checks – Bronze Layer: crm_prd_info
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
