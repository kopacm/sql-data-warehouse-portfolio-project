/*
===============================================================================
-- Script:         Load and Cleanse Sales Details (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads sales data from the raw
--                 'bronze.crm_sales_details' table into the cleansed 
--                 'silver.crm_sales_details' table.
===============================================================================
*/

INSERT INTO silver.crm_sales_details (
    -- Target columns in the silver table
    sls_ord_num,  
    sls_prd_key,  
    sls_cust_id,  
    sls_order_dt,
    sls_ship_dt, 
    sls_due_dt,  
    sls_sales,    
    sls_quantity, 
    sls_price
)
SELECT 
    -- Source columns from the bronze table with transformations
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    /* 
    -- Date Cleansing (Order Date)
    -- This logic handles dates stored improperly (e.g., as integers like 20231225).
    -- 1. It checks if the length is not 8 characters or if the value is negative.
    -- 2. If the data is invalid, it sets the date to NULL.
    -- 3. Otherwise, it safely casts the integer/string to a valid DATE format.
    */
    CASE 
        WHEN LEN(sls_order_dt) != 8 OR sls_order_dt < 0
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS sls_order_dt,

    /* 
    -- Date Cleansing (Ship Date)
    -- Same logic as sls_order_dt, applied to the shipping date.
    */
    CASE 
        WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt < 0
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS sls_ship_dt,

    /* 
    -- Date Cleansing (Due Date)
    -- Same logic as sls_order_dt, applied to the payment due date.
    */
    CASE 
        WHEN LEN(sls_due_dt) != 8 OR sls_due_dt < 0
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS sls_due_dt,

    /*
    -- Sales Amount Validation and Correction
    -- This enforces data integrity for the sales amount.
    -- It recalculates the sales amount IF:
    --   - It's NULL, zero, or negative.
    --   - It doesn't equal the expected calculation (quantity * price).
    -- This ensures the final sales value is always positive and consistent.
    -- ABS(sls_price) is used to handle potential negative price entries in the source data.
    */
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != (sls_quantity * ABS(sls_price)) 
        THEN ABS(sls_price) * sls_quantity
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    /*
    -- Price Calculation and Correction
    -- This logic derives the unit price if it's missing or invalid.
    -- It calculates the price from `sales / quantity` IF:
    --   - The price is negative, NULL, or zero.
    -- `NULLIF(sls_quantity, 0)` is a crucial safety check to prevent "division by zero" errors
    -- if a record has a quantity of 0.
    */
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM 
    bronze.crm_sales_details;
