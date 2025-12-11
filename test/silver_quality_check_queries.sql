/*
===============================================================================
-- Script:         Quality Assurance & Validation Queries (Silver Layer)
-- Author:         Miroslav Kopac
-- Date:           2025-12-11
-- Description:    This script contains validation queries to check data quality
--                 integrity, normalization, and standardization in the Silver Layer.
--                 Each section targets a specific table to ensure no anomalies exist.
===============================================================================
*/

-- ====================================================================
-- 1. Table: silver.crm_cust_info (Customer Information)
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No results (names should be trimmed)
SELECT 
    cst_firstname,
    cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
   OR cst_lastname != TRIM(cst_lastname);

-- Check for Standardization of Marital Status & Gender
-- Expectation: Distinct values should only be: Married/Single/n/a AND Male/Female/n/a
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;


-- ====================================================================
-- 2. Table: silver.crm_prd_info (Product Information)
-- ====================================================================

-- Check for Unwanted Spaces in Product Names
-- Expectation: No results
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Negative or NULL Costs
-- Expectation: No results (Costs should be >= 0)
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for Standardization of Product Lines
-- Expectation: Readable values (e.g., 'Road', 'Mountain') instead of codes ('R', 'M')
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Check for Invalid Date Ranges
-- Expectation: No results (End Date must not be before Start Date)
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check for Primary Key Duplicates or NULLs
-- Expectation: No results (PK must be unique and non-null)
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- ====================================================================
-- 3. Table: silver.crm_sales_details (Sales Details)
-- ====================================================================

-- Check for Unwanted Spaces in Order Numbers
-- Expectation: No results
SELECT 
    sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check for Invalid Date Formats or Values
-- Expectation: No results (Dates should be valid standard formats)
SELECT 
    sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL 
   OR LEN(CAST(sls_order_dt AS VARCHAR)) < 10 -- Standard YYYY-MM-DD is 10 chars
   OR sls_order_dt < '1900-01-01';          -- Basic sanity check for year

-- Check for Data Integrity (Sales Amount Logic)
-- Expectation: No results. Sales = Quantity * Price. No negatives/NULLs.
SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales < 0 
   OR sls_sales IS NULL 
   OR sls_sales = 0 
   OR sls_sales != (sls_quantity * ABS(sls_price));

-- Check for Invalid Prices
-- Expectation: No results (Prices > 0)
SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_price <= 0 OR sls_price IS NULL;

-- Check for Logical Date Errors
-- Expectation: No results (Order date cannot be after shipping or due date)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;


-- ====================================================================
-- 4. Table: silver.erp_loc_a101 (Location Data)
-- ====================================================================

-- Check for Standardization of Country Names
-- Expectation: Standardized names (e.g., 'United States', 'Germany')
SELECT DISTINCT cntry FROM silver.erp_loc_a101;

-- Check for Foreign Key Integrity (CID)
-- Expectation: Identify keys in ERP not present in CRM (Data Gaps)
SELECT DISTINCT cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);


-- ====================================================================
-- 5. Table: silver.erp_cust_az12 (ERP Customer Data)
-- ====================================================================

-- Check for Unreal Birth Dates
-- Expectation: No results (Birth dates cannot be in the future)
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Check for CID Normalization Issues
-- Expectation: No results (IDs should be standardized, typically < 10 chars after cleaning)
SELECT cid
FROM silver.erp_cust_az12
WHERE LEN(cid) > 10;

-- Check for Standardization of Gender
-- Expectation: Distinct values: 'Male', 'Female', 'n/a'
SELECT DISTINCT gen FROM silver.erp_cust_az12;


-- ====================================================================
-- 6. Table: silver.erp_px_cat_g1v2 (Product Categories)
-- ====================================================================

-- Check for Unwanted Spaces 
-- Expectation: No results
SELECT cat,subcat,maintenance
FROM silver.erp_px_cat_g1v2
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance

-- Check for Standardization of Maintenance Column
-- Expectation: Verified list of maintenance statuses
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;
