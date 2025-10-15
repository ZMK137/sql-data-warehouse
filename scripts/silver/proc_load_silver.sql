/*
==============================================================
Stored Procedure: silver.load_silver (Bronze -> Silver)
==============================================================
Script Purpose:
    This script defines a stored procedure to load data from the 'bronze' schema to the 'silver' schema.
    It includes data transformations, standardizations, and validations to ensure data quality.
    The procedure truncates existing data in silver tables before loading new data.
    After defining the procedure, it is executed to perform the data load.
==============================================================
*/


-- Execute the procedure to load data into silver tables
CALL silver.load_silver();

-- Procedure to load data from bronze to silver layer with transformations and validations
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql AS $$
BEGIN-- Load data into silver tables from bronze with necessary transformations and validations
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_first_name,
        cst_last_name,
        cst_material_status,
        cst_gender,
        cst_created_date)
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_first_name) AS cst_first_name,
        TRIM(cst_last_name)AS cst_last_name,
        CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a' END AS cst_material_status_standardized, -- Normalize marital status values to readable format
        CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
            ELSE 'n/a' END AS cst_gender_standardized, -- Normalize gender values to readable format
        cst_created_date
    FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_created_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL) t
    WHERE flag_last = 1; -- Select the most recent record per customer

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_name,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt) 
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,--- Normalize category IDs by replacing hyphens with underscores
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,--- Extract product key by removing category prefix
        prd_name,
        COALESCE(prd_cost, 0) AS prd_cost,--- Default missing costs to zero

        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                ELSE 'n/a' END AS prd_line,--- Standardize product line codes to full descriptive names
        CAST(prd_start_dt AS DATE) AS prd_start_dt,--- Convert start date to proper date format
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt--- Derive end date as the day before the next start date for the same product
    FROM bronze.crm_prd_info;

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_ord_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_ord_dt = 0 OR LENGTH(sls_ord_dt::text) != 8 THEN NULL
            ELSE CAST(CAST(sls_ord_dt AS VARCHAR(8)) AS DATE)
        END AS sls_ord_dt,--- Convert order date from integer to date, setting invalid dates to NULL
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
        END AS sls_ship_dt,--- Convert ship date from integer to date, setting invalid dates to NULL
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
        END AS sls_due_dt,--- Convert due date from integer to date, setting invalid dates to NULL
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price END AS sls_price -- Derive price if original value is invalid
    FROM bronze.crm_sales_details;

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)

    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid END AS cid,--- Remove 'NAS' prefix from customer IDs for standardization
        CASE WHEN bdate < '1900-01-01' OR bdate > CURRENT_DATE THEN NULL
            ELSE bdate END AS bdate,--- Set out-of-range birth dates to NULL
        CASE WHEN UPPER(TRIM(gen)) = 'M'  THEN 'Male'
            WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
            WHEN gen IS NULL THEN 'n/a'
            WHEN UPPER(TRIM(gen)) = '' THEN 'n/a'
            ELSE gen END AS gen
    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (cid, cntry)

    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            ELSE TRIM(cntry) -- Normalize country names
        END AS cntry
    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
END;
$$;


--Check Counts
SELECT COUNT(*) FROM silver.crm_cust_info;
SELECT COUNT(*) FROM silver.crm_prd_info;
SELECT COUNT(*) FROM silver.crm_sales_details;
SELECT COUNT(*) FROM silver.erp_cust_az12;
SELECT COUNT(*) FROM silver.erp_loc_a101;
SELECT COUNT(*) FROM silver.erp_px_cat_g1v2;

---Sample Data Checks
SELECT * FROM silver.erp_cust_az12 LIMIT 10;
SELECT * FROM silver.erp_loc_a101 LIMIT 10;
SELECT * FROM silver.erp_px_cat_g1v2 LIMIT 10;
SELECT * FROM silver.crm_sales_details LIMIT 10;
SELECT * FROM silver.crm_cust_info LIMIT 10;
SELECT * FROM silver.crm_prd_info LIMIT 10;
