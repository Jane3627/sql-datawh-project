/*
=============================================================
Database Initialization Script
=============================================================

Purpose:
    This script initializes a SQL Server database named 'DataWarehouse'.
    If the database already exists, it will be dropped and recreated.
    It also defines three schemas within the database: 'bronze', 'silver', and 'gold',
    aligned with the Medallion Architecture for data warehousing.

Caution:
    Executing this script will permanently delete the existing 'DataWarehouse' database,
    including all stored data. Ensure that appropriate backups are in place before proceeding.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
