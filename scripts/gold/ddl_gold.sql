/*
==============================
 Data Warehouse - Gold Layer
==============================
 This script creates the final dimension and fact tables in the gold layer
 by integrating and transforming data from the silver layer.
 It includes:
 1. Customer Dimension Table (dim_customers)
 2. Product Dimension Table (dim_products)
 3. Sales Fact Table (fact_sales)
 ==============================
 Usage:
 Run this script after the silver layer tables are populated and validated. 
*/




-- Create the final dimension table in gold layer gold.dim_customers
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS--- Final customer dimension view integrating CRM and ERP data 
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,--- Surrogate key for the customer dimension
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_first_name AS first_name,
    ci.cst_last_name AS last_name,
    la.cntry AS country,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master for gender info
         ELSE COALESCE(ca.gen, 'n/a') 
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_created_date AS created_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid




-- Create the final product dimension table in gold layer
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS--- Final product dimension view integrating CRM product info with ERP category details
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt ,pn.prd_key) AS product_key,--- Surrogate key for the product dimension
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_name AS product_name,   
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS product_cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data to get current active products



-- Create the final sales fact table in gold layer
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS--- Final sales fact table linking to customer and product dimensions
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_ord_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quanity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;
