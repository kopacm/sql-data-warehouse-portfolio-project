/*
===============================================================================
-- Script:         Load and Cleanse Customer Data (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads customer data from the raw
--                 'bronze.erp_cust_az12' table into the cleansed 
--                 'silver.erp_cust_az12' table.
===============================================================================
*/

INSERT INTO silver.erp_cust_az12 (
    -- Target columns in the silver table
    cid,
    bdate,
    gen
)
SELECT
    /*
    -- Key Standardization (CID)
    -- This logic handles IDs that begin with a specific prefix 'NAS'.
    -- If the ID starts with 'NAS', the SUBSTRING function extracts the rest of 
    -- the string starting from the 4th character (skipping the 3-character prefix).
    -- Otherwise, the original ID is kept. This aligns keys across different source systems.
    */
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
        ELSE cid
    END AS cid,

    /*
    -- Date Validation (Birth Date)
    -- This enforces a logical data quality rule: Birth dates cannot be in the future.
    -- 1. Checks if the birth date (bdate) is greater than the current system date (GETDATE).
    -- 2. If true (invalid), returns NULL.
    -- 3. Otherwise, safely casts the value to the standard DATE format.
    */
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE CAST(bdate AS DATE)
    END AS bdate,

    /*
    -- Gender Normalization
    -- This standardizes gender values into distinct categories ('Male', 'Female', 'n/a').
    -- 1. UPPER(TRIM(gen)) ensures case-insensitivity and removes extra spaces.
    -- 2. Matches abbreviations ('F', 'M') and full words to a single standard output.
    -- 3. Defaults to 'n/a' for any unrecognized or missing values.
    */
    CASE UPPER(TRIM(gen))
        WHEN 'F' THEN 'Female'
        WHEN 'FEMALE' THEN 'Female'
        WHEN 'M' THEN 'Male'
        WHEN 'MALE' THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM 
    bronze.erp_cust_az12;
