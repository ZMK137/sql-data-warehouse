/*
============================================================
CREATE DATABASE AND SCHEMAS
============================================================
Script Purpose: 
    This script creates a new database named 'DataWarehouse' after checking it already exists.
    If the database exists, it will be dropped and recreated. Additionaly, the script sets up three schemas: 'bronze', 'silver', and 'gold'.


Warning: 
    Executing this script will result in the loss of all existing data in the 'DataWarehouse' database if it already exists.
    Please ensure that you have backup of any important data before running this script.
*/


--Drop and recreate the 'DataWarehouse' database
DROP DATABASE IF EXISTS DataWarehouse;


--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;


--Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;
