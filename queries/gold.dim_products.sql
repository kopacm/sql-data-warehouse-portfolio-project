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
