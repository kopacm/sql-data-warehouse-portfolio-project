/*
======================================================
-- Script: Create DataWarehouse Database with Schemas
-- Author: Miroslav Kopac
-- Date: 2025-11-30
======================================================

-- Purpose:
-- 1. Drop existing DataWarehouse database if it exists for a fresh setup.
-- 2. Create a new DataWarehouse database.
-- 3. Create three schemas (bronze, silver, gold) to organize data layers.

-- Warning:
-- Dropping the database will permanently DELETE all data and objects in it.
-- Ensure no active connections to the database before running this script.

*/

-- Create the database
CREATE DATABASE DataWarehouse;
GO

-- Use the database context to create schemas
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
GO
