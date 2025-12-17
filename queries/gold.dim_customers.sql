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
