/*
===============================================================================
-- Script:         Load and Standardize Location Data (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads location data from the raw
--                 'bronze.erp_loc_a101' table into the standardized 
--                 'silver.erp_loc_a101' table.
===============================================================================
*/

INSERT INTO silver.erp_loc_a101 (
    -- Target columns in the silver table
    cid,
    cntry
)
SELECT 
    /*
    -- Key Standardization (CID)
    -- The REPLACE function removes hyphens ('-') from the customer ID to 
    -- ensure a consistent, alphanumeric-only format (e.g., 'C-123' becomes 'C123').
    */
    REPLACE(cid, '-', '') AS cid,

    /*
    -- Country Name Standardization and Enrichment
    -- This CASE statement normalizes country names to a full standard format.
    -- 1. 'DE' is expanded to 'Germany'.
    -- 2. 'US' or 'USA' are unified to 'United States'.
    -- 3. Empty strings or NULLs are replaced with a default 'n/a' value for data quality.
    -- 4. TRIM removes any unwanted leading/trailing whitespace.
    */
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = 'US' THEN 'United States'
        WHEN TRIM(cntry) = 'USA' THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry

FROM 
    bronze.erp_loc_a101;
