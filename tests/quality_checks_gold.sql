/*
==============================================================================
Data Warehouse Gold Layer
==============================================================================
Quality Checks
==============================================================================       
*/


-- Check for duplicates after joins for customer dimension
SELECT cst_id, COUNT(*) FROM
(
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_first_name,
        ci.cst_last_name,
        ci.cst_material_status,
        ci.cst_gender,
        ci.cst_created_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca 
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
) t GROUP BY cst_id 
HAVING COUNT(*) > 1;

--Data integration
SELECT DISTINCT
    ci.cst_gender,
    ca.gen,
    CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
         ELSE COALESCE(ca.gen, 'n/a') 
    END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2;
