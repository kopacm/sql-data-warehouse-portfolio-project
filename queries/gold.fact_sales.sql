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
