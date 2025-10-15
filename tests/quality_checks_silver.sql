/*
==================================================
Quality Checks on Silver Tables
==================================================
Script Purpose:
    This script performs data quality checks on the silver tables after data has been loaded from the bronze layer.
    It includes checks for nulls, duplicates, unwanted spaces, data standardization, consistency, and valid date orders.
    Each section is dedicated to a specific silver table and outlines the expectations for data quality.
==================================================
Usage Notes:
    - Ensure that the silver tables have been populated by executing the silver.load_silver() procedure before running these checks.
    - Review the results of each query to identify and address any data quality issues.
*/




/*
==================================================
CRM Customer Info Silver Table Quality Checks
==================================================
*/

-- Check for Nulls OR Duplicates in Primary Key
-- Expectation is that this query returns 0 rows    
SELECT 
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation is that this query returns 0 rows
SELECT cst_first_name
FROM silver.crm_cust_info
WHERE cst_first_name != TRIM(cst_first_name);

--Data Standardization AND Consistency
SELECT DISTINCT cst_gender
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;

/*
==================================================
CRM Product Info Silver Table Quality Checks
==================================================
*/

-- Check for unwanted spaces
-- Expectation is that this query returns 0 rows
SELECT prd_name
FROM silver.crm_prd_info
WHERE prd_name != TRIM(prd_name);

--Check For Null Values or Negative Numbers
-- Expectation is that this query returns 0 rows
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


--Data Standardization AND Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


--Check for Invalid Dates Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT
    prd_id,
    prd_key,
    prd_name,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Check for unwanted duplicates
-- Expectation is that this query returns 0 rows
SELECT 
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT * FROM silver.crm_prd_info;


/*
==================================================
CRM Sales Details Silver Table Quality Checks
==================================================
*/

--Check For Invalid Dates
SELECT
    NULLIF(sls_ord_dt, 0) AS sls_ord_dt
FROM bronze.crm_sales_details
WHERE sls_ord_dt <=0
OR LENGTH(sls_ord_dt::text) != 8
OR sls_ord_dt > 20500101
OR sls_ord_dt < 19000101;

SELECT
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0
OR LENGTH(sls_ship_dt::text) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101;

SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0
OR LENGTH(sls_due_dt::text) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

-- Check for Invalid Dates Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_ship_dt < sls_ord_dt OR sls_ord_dt > sls_due_dt;

--Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
Select
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS ols_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;


/*
==================================================
ERP Price Caustomers az12 Silver Table Quality Checks
==================================================
*/

-- Identify Out of Range Birth Dates
Select 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1900-01-01' OR bdate > CURRENT_DATE;

-- Data Standardization AND Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;


/*
==================================================
ERP Location a101 Silver Table Quality Checks
==================================================
*/

-- Data Standardization AND Consistency
SELECT DISTINCT 
    cntry AS old_cntry,    
    CASE WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         ELSE TRIM(cntry)
    END AS cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT * FROM silver.erp_loc_a101;


/*
==================================================
ERP Price Category g1v2 Silver Table Quality Checks
==================================================
*/

--Check for unwanted spaces
-- Expectation is that this query returns 0 rows
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance);

--Data Standardization AND Consistency
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;
