/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

use master;
GO

--Drop and recreate the 'Datewarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases where name = 'Datewarehouse')
BEGIN
	ALTER DATABASE Datewarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datewarehouse;
END;
GO

--create 'Datewarehouse' database
CREATE DATABASE Datewarehouse;
GO

USE Datewarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;

