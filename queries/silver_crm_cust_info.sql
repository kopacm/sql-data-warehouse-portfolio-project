/*
===============================================================================
-- Script:         Load and Cleanse Customer Data (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads customer data from the raw
--                 'bronze.crm_cust_info' table into the cleansed 
--                 'silver.crm_cust_info' table.
--                 It also handles deduplication to ensure unique customer records.
===============================================================================
*/

INSERT INTO silver.crm_cust_info (
    -- Target columns in the silver table
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,

    /*
    -- Name Cleaning
    -- Removes unnecessary leading and trailing whitespace from names.
    */
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,

    /*
    -- Marital Status Standardization
    -- Maps single-character codes to descriptive text.
    */
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cst_marital_status,

    /*
    -- Gender Standardization
    -- Maps single-character codes to descriptive text.
    */
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,

    cst_create_date

FROM 
    (
        /*
        -- Deduplication Logic
        -- The source data may contain duplicate records for the same customer (cst_id).
        -- We use the Window Function RANK() to identify the most recent record.
        -- PARTITION BY cst_id: Groups data by customer.
        -- ORDER BY cst_create_date DESC: Orders them by date (newest first).
        -- Result: The row with 'last_date = 1' is the most recent entry.
        */
        SELECT 
            *,
            RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_date
        FROM 
            bronze.crm_cust_info
    ) t
WHERE 
    -- Filter to keep only the most recent record per customer
    last_date = 1 
    -- Ensure the creation date is valid (not NULL)
    AND cst_create_date IS NOT NULL;
