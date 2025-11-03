/*
===============================================================================
Script Name   : Create Gold Layer Views
===============================================================================
Purpose       :
    Define views for the Gold layer of the data warehouse, representing
    finalized dimension and fact tables in a star schema model.

    These views apply necessary transformations to curated Silver layer data,
    producing clean, enriched, and analytics-ready datasets for business reporting.

Usage         :
    - Query these views directly for dashboards, KPIs, and analytical insights.
    - Serves as the foundation for BI tools and data consumers.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS [Customer Key], -- Surrogate key
	ci.cst_id                          AS [Customer Id],
	ci.cst_key                         AS [Customer Number],
	ci.cst_firstname                   AS [First Name],
	ci.cst_lastname                    AS [Last Name],
	la.cntry                           AS [Country],
	ci.cst_marital_status              AS [Marital Status],
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
		ELSE COALESCE(ca.gen, 'n/a')               -- Fallback to ERP data
	END                                AS [Gender],
	ca.bdate                           AS [Birthdate],
	ci.cst_create_date                 AS [Create Date]
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS [Product Id],
	pn.prd_key      AS [Product Number],
	pn.prd_nm       AS [Product Name],
	pn.cat_id       AS [Category Id],
	pc.cat          AS [Category],
	pc.subcat       AS [Subcategory],
	pc.maintenance  AS [Maintenance],
	pn.prd_cost     AS [Cost],
	pn.prd_line     AS [Product Line],
	pn.prd_start_dt AS [Start Date]
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS [Order Number],
    pr.product_key  AS [Product Key],
    cu.[Customer Key] AS [Customer Key],
    sd.sls_order_dt AS [Order Date],
    sd.sls_ship_dt  AS [Shipping Date],
    sd.sls_due_dt   AS [Due Date],
    sd.sls_sales    AS [Sales Amount],
    sd.sls_quantity AS [Quantity],
    sd.sls_price    AS [Price]
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.[Product Number]
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.[Customer Id];
GO
