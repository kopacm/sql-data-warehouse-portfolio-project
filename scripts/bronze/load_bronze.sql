/*
======================================================
-- Script: Bronze Layer - Bulk Load CRM & ERP Data
-- Author: Miroslav Kopac
-- Date: 2025-12-03
-- Purpose: Load raw CSV files into bronze layer tables
======================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME;
    BEGIN TRY
        PRINT '======================================================';
        PRINT 'CRM DATA LOADING (Bronze Layer)';
        PRINT '======================================================';
        PRINT '>> ---------------------------------------------------';

        -- 1. CRM Customer Info
        SET @bronze_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded crm_cust_info';
        SET @end_time = GETDATE();
        PRINT '>> Loaded time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>> ---------------------------------------------------';

        -- 2. CRM Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded crm_prd_info';
        SET @end_time = GETDATE();
        PRINT '>> Loaded time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>> ---------------------------------------------------';

        -- 3. CRM Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded crm_sales_details';
        SET @end_time = GETDATE();
        PRINT '>> Loaded time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>> ---------------------------------------------------';

    
        PRINT '======================================================';
        PRINT 'ERP DATA LOADING (Bronze Layer)';
        PRINT '======================================================';
        -- 4. ERP Customer AZ12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded erp_cust_az12';
        SET @end_time = GETDATE();
        PRINT '>> Loaded time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>> ---------------------------------------------------';

        -- 5. ERP Location A101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded erp_loc_a101';

        -- 6. ERP Price Category G1V2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\PARA\01_Projects\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            FIELDQUOTE = '"',
            FORMAT = 'CSV'
        );
        PRINT '>> Loaded erp_px_cat_g1v2';
        SET @end_time = GETDATE();
        PRINT '>> Loaded time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
       
        

        -- ======================================================
        -- SUMMARY: Bronze layer fully loaded
        -- Tables: 6 datasets | CRM: 3 | ERP: 3
        -- ======================================================

       
        SET @bronze_end_time = GETDATE()
        PRINT '======================================================';
        PRINT 'Bronze layer fully loaded';
        PRINT '>> Total loading time: ' + CAST(DATEDIFF(second,@bronze_start_time,@bronze_end_time) AS NVARCHAR) + 'seconds';
        PRINT '======================================================';
    END TRY
    BEGIN CATCH
    PRINT '======================================================';
    PRINT 'ERROR WHILE LOADING BRONZE LAYER';
    PRINT 'Error Message' + Error_message() ;
    PRINT 'Error Message' + CAST (Error_number() AS NVARCHAR) ;
    PRINT '======================================================';
    END CATCH
END