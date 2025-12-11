/*
===============================================================================
-- Script:         Load and Cleanse Product Data (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads product data from the raw
--                 'bronze.crm_prd_info' table into the cleansed 
--                 'silver.crm_prd_info' table.
===============================================================================
*/

INSERT INTO silver.crm_prd_info (
    -- Target columns in the silver table
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,

    /*
    -- Key Extraction
    -- The source 'prd_key' is a composite field (e.g., 'CAT-01-PROD123').
    -- This logic parses the string to extract the actual Product Key.
    -- SUBSTRING starts from the 7th character to skip the category prefix.
    */
    SUBSTRING(TRIM(prd_key), 7, LEN(TRIM(prd_key))) AS prd_key,

    /*
    -- Category ID Derivation
    -- This extracts the first 5 characters of the source key to form the Category ID.
    -- It also replaces hyphens with underscores (e.g., 'CAT-01' -> 'CAT_01') 
    -- to maintain consistency with internal naming conventions.
    */
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,

    prd_nm,

    /*
    -- Cost Standardization
    -- Handles missing cost data by replacing NULLs with 0.
    -- This ensures calculations won't fail due to null propagation.
    */
    ISNULL(prd_cost, 0) AS prd_cost,

    /*
    -- Product Line Normalization
    -- Maps short abbreviation codes to full, descriptive business names.
    -- UPPER(TRIM(...)) ensures the input is clean and case-insensitive before matching.
    */
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    -- Cast Start Date to standard DATE format
    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    /*
    -- End Date Calculation (Data Fix)
    -- This logic handles invalid historical data where the End Date is earlier than the Start Date.
    -- If such an error is found:
    --   1. It looks ahead to the *next* record's Start Date for the same product (LEAD function).
    --   2. It sets the current End Date to one day before that next Start Date.
    -- This creates a continuous history without overlaps.
    */
    CASE 
        WHEN prd_end_dt < prd_start_dt 
        THEN CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        ELSE prd_end_dt -- (Implicitly returns NULL if condition is false)
    END AS prd_end_dt

FROM 
    bronze.crm_prd_info;
