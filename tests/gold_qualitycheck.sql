/*
===============================================================================
Script Name   : Gold Layer Quality Checks
===============================================================================
Purpose       :
    Validate the integrity, consistency, and accuracy of the Gold layer in the
    data warehouse. These checks ensure:
    - Surrogate key uniqueness in dimension tables
    - Referential integrity between fact and dimension tables
    - Logical consistency of relationships for analytical reliability

Usage Notes   :
    - Investigate and resolve any anomalies or mismatches identified by these checks.
===============================================================================
*/

-- ====================================================================
-- Check 1: Uniqueness of surrogate keys in 'gold.dim_customers'
-- Expectation: No duplicate customer_key values
-- ====================================================================
SELECT 
    [Customer Key],
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY [Customer Key]
HAVING COUNT(*) > 1;

-- ====================================================================
-- Check 2: Uniqueness of surrogate keys in 'gold.dim_products'
-- Expectation: No duplicate product_key values
-- ====================================================================
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Check 3: Referential integrity in 'gold.fact_sales'
-- Expectation: All foreign keys should match valid dimension keys
-- ====================================================================
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON c.[Customer Key] = f.[Customer Key]
LEFT JOIN gold.dim_products p ON p.product_key = f.[Product Key]
WHERE p.product_key IS NULL OR c.[Customer Key] IS NULL;
