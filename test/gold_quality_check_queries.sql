/*
===============================================================================
-- Script:       GOLD LAYER - QUALITY & INTEGRATION CHECKS
-- Author:       Miroslav Kopac
-- Date:         2025-12-17
--
-- Purpose:
--   Validate the correctness and integrity of Gold layer views:
--   - gold.dim_customers
--   - gold.dim_products
--   - gold.fact_sales
--
-- What this script checks:
--   1. Standardization of customer gender values in dim_customers.
--   2. Uniqueness of surrogate keys in dim_customers and dim_products.
--   3. Referential integrity between fact_sales and both dimensions:
--      - Every fact row should have a matching customer_key.
--      - Every fact row should have a matching product_key.
--
-- Usage:
--   Run after (re)building the Gold layer to perform regression checks.
===============================================================================
*/

-- Check Integration Logic
-- Expectation: No nulls, only standardized values (Male, Female, n/a)
SELECT DISTINCT gender 
FROM gold.dim_customers;

-- Check Uniqueness
-- Expectation: Empty result (Surrogate key must be unique)
SELECT 
    customer_key
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1; 

-- Validate
-- Expect no rows (surrogate key unique).
SELECT 
    product_key
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check fact and customer dimension connectivity
-- Expect no rows (every fact has a valid customer)
SELECT
    *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
    ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

-- Check fact and product dimension connectivity
-- Expect no rows (every fact has a valid product)
SELECT 
    *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;
