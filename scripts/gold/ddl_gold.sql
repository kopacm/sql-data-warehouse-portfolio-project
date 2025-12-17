/*
===============================================================================
-- Script:       GOLD LAYER - DIMENSION & FACT VIEWS (SQL Server)
-- Author:       Miroslav Kopac
-- Date:         2025-12-17
--
-- Purpose:
--   The scripts creates views for the Gold layer on top of the
--   cleansed Silver layer for reporting and analytics.
--
-- Objects Created:
--   - gold.dim_customers : Customer dimension
--   - gold.dim_products  : Product dimension (active products only)
--   - gold.fact_sales    : Sales fact view joined to customer & product dims
--
-- Usage Examples:
--   SELECT * FROM gold.dim_customers;
--   SELECT * FROM gold.dim_products;
--   SELECT * FROM gold.fact_sales WHERE order_date >= '2025-01-01';
--
-- Notes:
--   - All objects are created as views to keep the Gold layer virtual.
--   - Keys (customer_key, product_key) are surrogate keys generated via
--     ROW_NUMBER for analytical convenience.
===============================================================================
*/

-- =====================================================
-- CUSTOMER DIMENSION VIEW (gold.dim_customers)
-- =====================================================
-- Description:
--   - Provides a conformed customer dimension.
--   - Enriches CRM customer data with ERP attributes (birthday, country, gender).
--   - Gender falls back from CRM to ERP when CRM has 'n/a' or NULL.
-- =====================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC) AS customer_key,  -- Surrogate key for analytics
    ci.cst_id        AS customer_id,                             
    ci.cst_key       AS customer_number,                         
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    -- Gender fallback: prioritize ci.cst_gndr, fallback to ca.gen if 'n/a' or NULL
    CASE 
        WHEN ci.cst_gndr IN ('n/a', NULL) THEN ISNULL(ca.gen, 'n/a')
        ELSE ci.cst_gndr
    END AS gender,
    ca.bdate              AS birthday,
    ci.cst_marital_status AS marital_status,
    la.cntry              AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;
GO

-- =====================================================
-- PRODUCT DIMENSION VIEW (gold.dim_products)
-- =====================================================
-- Description:
--   - Provides product dimensions.
--   - Filters to active products only (prd_end_dt IS NULL).
--   - Surrogate key is generated from start date and product id.
-- =====================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_id) AS product_key, -- Surrogate key
    pr.prd_id      AS product_id,       
    pr.prd_key     AS product_number,   
    pr.cat_id      AS category_id,
    pc.cat         AS category,
    pc.subcat      AS subcategory,
    pc.maintenance AS maintenance_required,
    pr.prd_nm      AS product_name,
    pr.prd_line    AS product_line,
    pr.prd_cost    AS cost,
    pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2  pc ON pr.cat_id = pc.id
WHERE pr.prd_end_dt IS NULL;  -- Active products only
GO

-- =====================================================
-- SALES FACT VIEW (gold.fact_sales)
-- =====================================================
-- Description:
--   - Central fact view for sales analysis.
--   - Links sales transactions from Silver layer to Gold dimensions:
--       * gold.dim_customers (by customer_id)
--       * gold.dim_products  (by product_number)
--   - Provides key business measures: sales_amount, quantity, price.
-- =====================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num  AS order_number,    
    pr.product_key,                     -- Surrogate product key (FK to dim_products)
    cu.customer_key,                    -- Surrogate customer key (FK to dim_customers)
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date, 
    sd.sls_due_dt   AS due_date,  
    sd.sls_sales    AS sales_amount,    
    sd.sls_quantity AS quantity, 
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers  cu ON cu.customer_id   = sd.sls_cust_id
LEFT JOIN gold.dim_products   pr ON pr.product_number = sd.sls_prd_key;
GO
