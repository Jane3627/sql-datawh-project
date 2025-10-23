/*
===============================================================================
Quality Assurance Checks – Bronze Layer
===============================================================================
Purpose:
    This script validates data integrity and consistency within the 'bronze' layer,
    specifically targeting the following quality dimensions:
    - Null or duplicate values in primary keys.
    - Leading/trailing spaces in string fields.
    - Standardization of formats and values.

===============================================================================
*/

--check for null or duplicates for primary keys
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;


select *
from bronze.crm_prd_info
where prd_id = 29466;

select *, ROW_NUMBER() OVER (partition by prd_id order by cst_create_date desc) as flag_last
from bronze.crm_prd_info
where prd_id = 29466;

SELECT * FROM (
select *, ROW_NUMBER() OVER (partition by prd_id order by cst_create_date desc) as flag_last
from bronze.crm_prd_info
)t where flag_last!=1;


select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') not in 
(
	select distinct id from bronze.erp_px_cat_g1v2)

select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info

--check for unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info

select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt
lead(prd_start_dt) over (partition prd_key order by prd_start_dt) as prd_end_dt_test
from bronze.crm_prd_info
