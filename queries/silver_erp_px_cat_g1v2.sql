/*
===============================================================================
-- Script:         Load and Cleanse Product Category Data (Bronze to Silver)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script transforms and loads product category data from 
--                 the raw 'bronze.erp_px_cat_g1v2' table into the cleansed 
--                 'silver.erp_px_cat_g1v2' table.
===============================================================================
*/

INSERT INTO silver.erp_px_cat_g1v2 (
    -- Target columns in the silver table
    id,          
    cat,        
    subcat,     
    maintenance
)
SELECT
    -- Source columns from the bronze table
    id,

    /*
    -- Category and Sub-Category Normalization
    -- The TRIM function removes any inadvertent leading or trailing whitespace 
    -- to ensure consistent string matching and reporting.
    */
    TRIM(cat) AS cat,
    TRIM(subcat) AS subcat,
    maintenance

FROM 
    bronze.erp_px_cat_g1v2;
