--1. Create Database 'DataWarehouse' & Schemas

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

--Drop and Recreate Database if it's already exist (careful)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create Database 'DataWarehouse'
USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

--Create SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

--Bronze Layer (ddl_proc_load)
/*
=============================================================
DDL Script: Create Bronze Tables & Load Data from CSV into Table with stored procedure
=============================================================
Script Purpose:
    This script creates a table in bronze schema (and drop table if it already exist),
    BULK INSERT all created table with data from csv (and truncate it before inserting to make table up-to-date),
    All in one on Stored Procedure with time calculation for whole process and each table upserting process
*/

--2.
--IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    --DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
);

CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

CREATE TABLE bronze.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
);

--3.BULK INSERT (Stored Procedure)
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT 'Loading Bronze Layer';
        PRINT '=====================================';

        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT 'Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info 
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE  bronze.crm_prd_info;
        PRINT 'Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Finished project\sql-datawarehouse-project\sql-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT 'Loading Bronze Layer is Completed';
        PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR(10));
    END CATCH
END

EXEC bronze.load_bronze

SELECT TOP 100
    *
FROM bronze.crm_cust_info;

--Silver Layer (ddl, data qualiy check & cleaning, proc_load)
--4.Silver Layer (ddl)
CREATE TABLE silver.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--5.Silver Layer (dq check & cleaning)
--Check for NULLs or Duplicates in Primary Key
--Expectation : No Result
SELECT 
	cst_id,
	COUNT(1)
--FROM bronze.crm_cust_info
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(1) > 1 or cst_id IS NULL

SELECT
	*
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
--FROM bronze.crm_cust_info
FROM silver.crm_cust_info
WHERE cst_id IS NOT NULL
)t
--WHERE flag_last = 1
WHERE flag_last != 1

--Check for unwanted spaces
--Expectation : No Results
SELECT 
	cst_firstname
	--cst_lastname
--FROM bronze.crm_cust_info
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
--WHERE cst_lastname != TRIM(cst_lastname)

--Data Standardization & Consistency
SELECT 
	DISTINCT cst_gndr
	--DISTINCT cst_marital_status
--FROM bronze.crm_cust_info
FROM silver.crm_cust_info

SELECT TOP 100
	*
FROM silver.crm_cust_info

--Cleaning Queries and Inserting into from bronze -> silver layer (crm_cust_info)
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT
	[cst_id]
   ,[cst_key]
   ,TRIM(cst_firstname) cst_firstname
   ,TRIM(cst_lastname) cst_lastname
   ,CASE UPPER(TRIM(cst_marital_status))
		WHEN 'S' THEN 'Single'
		WHEN 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status --Normalize marital status values to readable format
   ,CASE UPPER(TRIM(cst_gndr))
		WHEN 'F' THEN 'Female'
		WHEN 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr --Normalize gender values to readable format
   ,[cst_create_date]
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last --Select the most recent record per customer
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t 
WHERE flag_last = 1;

--======================================================================================================================================

--Check for NULLs or Duplicates in Primary Key
--Expectation : No Result
SELECT 
	prd_id,
	COUNT(1)
--FROM bronze.crm_prd_info
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(1) > 1 or prd_id IS NULL

--Check for unwanted spaces
--Expectation : No Results
SELECT 
	prd_nm
--FROM bronze.crm_prd_info
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check for Negative Numbers or NULLs
--Expectation : No Results
SELECT prd_cost
--FROM bronze.crm_prd_info
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Standardization & Consistency
SELECT 
	DISTINCT prd_line
--FROM bronze.crm_prd_info
FROM silver.crm_prd_info

--Check for Invalid Date Orders
SELECT *
--FROM bronze.crm_prd_info
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--Cleaning Queries and Inserting into from bronze -> silver layer (crm_prd_info)
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO [silver].[crm_prd_info]
           ([prd_id]
           ,[cat_id] --(derived column)
           ,[prd_key]
           ,[prd_nm]
           ,[prd_cost]
           ,[prd_line]
           ,[prd_start_dt]
           ,[prd_end_dt]
)
SELECT [prd_id]
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id --Extract category id from prd_key (derived column)
	  ,SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key --Extract product key (separate it from cat_id)
      ,[prd_nm]
      ,ISNULL(prd_cost, 0) prd_cost --If there's NULL change value to 0
	  ,CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line --Map prd_line to descriptive values
      ,CAST(prd_start_dt AS DATE) prd_start_dt --Changing DATETIME to DATE
	  ,CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) prd_end_dt --Calculate prd_end_date as one day before the next start date (data enrichment)
FROM [DataWarehouse].[bronze].[crm_prd_info];
--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
--(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
--WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN
--(SELECT sls_prd_key FROM bronze.crm_sales_details)

--================================================================================================================================================================================
--Check for Invalid Dates
SELECT sls_order_dt
--FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

--Check for Invalid Date Orders
SELECT *
--FROM bronze.crm_sales_details
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
--Sales = Quantity * Price
--Values must not be NULL, zero, or negative
SELECT
	sls_sales,
	sls_quantity,
	sls_price
--FROM bronze.crm_sales_details
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

SELECT TOP 20
*
FROM silver.crm_sales_details

--Cleaning Queries and Inserting into from bronze -> silver layer (silver.crm_sales_details)
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
	   [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
)
SELECT 
	   [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
	  ,CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL --Handling invalid data
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)		   --Cast from VARCHAR to DATE
	   END sls_order_dt
      ,CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	   END sls_ship_dt
      ,CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	   END sls_due_dt
      ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) --Handling invalid data
			THEN sls_quantity * ABS(sls_price)	--Use formula(doing calculation) to make derived value
			ELSE sls_sales
	   END sls_sales
	  ,[sls_quantity]
	  ,CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
			ELSE sls_price
	   END sls_price
FROM [bronze].[crm_sales_details];
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
--WHERE sls_ord_num != TRIM(sls_ord_num)
  
  --=============================================================================================================================
--Identify Out-of-Range Dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

--Data Standardization & Consistendy
SELECT DISTINCT
	gen
--FROM bronze.erp_cust_az12
FROM silver.erp_cust_az12

--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_cust_az12)
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove NAS prefix if present
		ELSE cid
	END cid,
	CASE
		WHEN bdate < '1926-01-01' OR bdate > GETDATE() --Set 100 year olds and future birthdate to NULL
		THEN NULL
		ELSE bdate
	END bdate,
		CASE 
		WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' --Normalize gender values and handle unknown cases
		WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		ELSE 'n/a'
	END gen
FROM bronze.erp_cust_az12;

SELECT TOP 20
*
FROM silver.erp_cust_az12

--=====================================================================================================================
--Data Standardization & Consistency
SELECT DISTINCT
cntry
--FROM bronze.erp_loc_a101
FROM silver.erp_loc_a101

--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_loc_a101)
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 
(cid, cntry)
SELECT 
	REPLACE(cid, '-', '') cid, --Standardization so value is the same with counterpart on other table
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' --Normalize and Handle missing value or NULLs
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
	ELSE TRIM(cntry) 
	END cntry
FROM bronze.erp_loc_a101;

--===============================================================================================================================
--Check for Unwanted Space
SELECT * 
FROM bronze.erp_px_cat_g1v2
--FROM silver.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat) OR  cat != TRIM(cat) OR maintenance != TRIM(maintenance)


SELECT DISTINCT
--cat 
TRIM(subcat)
FROM bronze.erp_px_cat_g1v2

--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_px_cat_g1v2)
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 
(id, cat, subcat, maintenance)
SELECT [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
FROM [bronze].[erp_px_cat_g1v2];

--6.Silver Layer (Stored Procedure)
--Stored Procedure (Silver Layer Load from Bronze)
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT 'Loading Silver Layer';
        PRINT '=====================================';

        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------';
	--Cleaning Queries and Inserting into from bronze -> silver layer (crm_cust_info)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		[cst_id]
	   ,[cst_key]
	   ,TRIM(cst_firstname) cst_firstname
	   ,TRIM(cst_lastname) cst_lastname
	   ,CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status --Normalize marital status values to readable format
	   ,CASE UPPER(TRIM(cst_gndr))
			WHEN 'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr --Normalize gender values to readable format
	   ,[cst_create_date]
	FROM(
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last --Select the most recent record per customer
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t 
	WHERE flag_last = 1;
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';
	--Cleaning Queries and Inserting into from bronze -> silver layer (crm_prd_info)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT 'Inserting Data Into: silver.crm_prd_info';
	INSERT INTO [silver].[crm_prd_info]
			   ([prd_id]
			   ,[cat_id] --(derived column)
			   ,[prd_key]
			   ,[prd_nm]
			   ,[prd_cost]
			   ,[prd_line]
			   ,[prd_start_dt]
			   ,[prd_end_dt]
	)
	SELECT [prd_id]
		  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id --Extract category id from prd_key (derived column)
		  ,SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key --Extract product key (separate it from cat_id)
		  ,[prd_nm]
		  ,ISNULL(prd_cost, 0) prd_cost --If there's NULL change value to 0
		  ,CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line --Map prd_line to descriptive values
		  ,CAST(prd_start_dt AS DATE) prd_start_dt --Changing DATETIME to DATE
		  ,CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) prd_end_dt --Calculate prd_end_date as one day before the next start date (data enrichment)
	FROM [DataWarehouse].[bronze].[crm_prd_info];
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';
	--Cleaning Queries and Inserting into from bronze -> silver layer (silver.crm_sales_details)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT 'Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		   [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  ,[sls_order_dt]
		  ,[sls_ship_dt]
		  ,[sls_due_dt]
		  ,[sls_sales]
		  ,[sls_quantity]
		  ,[sls_price]
	)
	SELECT 
		   [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  ,CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL --Handling invalid data
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)		   --Cast from VARCHAR to DATE
		   END sls_order_dt
		  ,CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		   END sls_ship_dt
		  ,CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		   END sls_due_dt
		  ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) --Handling invalid data
				THEN sls_quantity * ABS(sls_price)	--Use formula(doing calculation) to make derived value
				ELSE sls_sales
		   END sls_sales
		  ,[sls_quantity]
		  ,CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
				ELSE sls_price
		   END sls_price
	FROM [bronze].[crm_sales_details];
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';

	PRINT 'Loading ERP Tables';
    PRINT '-------------------------------------';

	--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_cust_az12)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT 'Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove NAS prefix if present
			ELSE cid
		END cid,
		CASE
			WHEN bdate < '1926-01-01' OR bdate > GETDATE() --Set 100 year olds and future birthdate to NULL
			THEN NULL
			ELSE bdate
		END bdate,
			CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' --Normalize gender values and handle unknown cases
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END gen
	FROM bronze.erp_cust_az12;
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';

	--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_loc_a101)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT 'Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 
	(cid, cntry)
	SELECT 
		REPLACE(cid, '-', '') cid, --Standardization so value is the same with counterpart on other table
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' --Normalize and Handle missing value or NULLs
			WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry) 
		END cntry
	FROM bronze.erp_loc_a101;
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';
	--Cleaning Queries and Inserting into from bronze -> silver layer (silver.erp_px_cat_g1v2)
	SET @start_time = GETDATE();
    PRINT 'Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT 'Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 
	(id, cat, subcat, maintenance)
	SELECT [id]
		  ,[cat]
		  ,[subcat]
		  ,[maintenance]
	FROM [bronze].[erp_px_cat_g1v2];
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '--------------------------------------------';

	SET @batch_end_time = GETDATE();
        PRINT 'Loading Silver Layer is Completed';
        PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR(10));
    END CATCH
END

EXEC silver.load_silver

--7.Gold Layer
--gold.dim_customers
--If there's no primary key in data warehouse, create surrogate key
/*SELECT 
    cst_id,
    COUNT(1)
FROM  */
CREATE VIEW gold.dim_customers AS
SELECT 
        ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key
      ,ci.cst_id AS customer_id
      ,ci.cst_key AS customer_number
      ,ci.cst_firstname AS first_name
      ,ci.cst_lastname AS last_name
      ,la.cntry AS country
      ,ci.cst_marital_status AS marital_status
      ,CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the Master of gender info
        ELSE COALESCE(ca.gen, 'n/a') --Data Integration
       END AS gender
      ,ca.bdate AS birthdate
      ,ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
/* )t
GROUP BY cst_id
HAVING COUNT(1) > 1 */

SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

EXEC sp_updatestats;
DBCC SHOWCONTIG;

--SELECT TOP 20 * FROM gold.dim_customers
SELECT distinct gender FROM gold.dim_customers

--==========================================================================================
--gold.dim_products

--SELECT prd_key,COUNT(1) FROM (
CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) product_key
      ,pn.prd_id product_id
      ,pn.prd_key product_number
      ,pn.prd_nm product_name
      ,pn.cat_id category_id
      ,pc.cat category
      ,pc.subcat subcategory
      ,pc.maintenance 
      ,pn.prd_cost product_cost
      ,pn.prd_line product_line
      ,pn.prd_start_dt 'start_date'
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL --Filter out all historical data
/* )t 
GROUP BY prd_key
HAVING COUNT(1) > 1 */

--===============================================================================================
--Data Lookup (Building Fact -> use the dimension's surrogate keys instead of IDs to easily connect facts table with dimension table)
--gold.dim_sales

CREATE VIEW gold.fact_sales AS
SELECT sd.sls_ord_num order_number
      ,pr.product_key               --Dimension Keys
      ,cu.customer_key
      ,sd.sls_order_dt order_date
      ,sd.sls_ship_dt shipping_date --Dates
      ,sd.sls_due_dt due_date
      ,sd.sls_sales sales_amount
      ,sd.sls_quantity quantity     --Measures
      ,sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

--Foreign Key Integrity
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
--WHERE c.customer_key IS NULL
WHERE p.product_key IS NULL

--8.Data Catalog

-- ============================================
-- Data Catalog: gold.dim_customers
-- ============================================
SELECT 
    [customer_key]      -- INT, Surrogate key uniquely identifying each customer record in the dimension table
  , [customer_id]       -- INT, Natural/business identifier assigned to each customer
  , [customer_number]   -- NVARCHAR, Alphanumeric identifier used for tracking and referencing customers
  , [first_name]        -- NVARCHAR, Customer’s given name
  , [last_name]         -- NVARCHAR, Customer’s family name
  , [country]           -- NVARCHAR, Country of residence or registration
  , [marital_status]    -- NVARCHAR, Customer’s marital status (e.g., Single, Married)
  , [gender]            -- NVARCHAR, Gender of the customer (e.g., Male, Female)
  , [birthdate]         -- DATE, Date of birth of the customer
  , [create_date]       -- DATETIME, Date when the customer record was created in the warehouse
FROM [DataWarehouse].[gold].[dim_customers];


-- ============================================
-- Data Catalog: gold.dim_products
-- ============================================
SELECT 
    [product_key]       -- INT, Surrogate key uniquely identifying each product record in the dimension table
  , [product_id]        -- INT, Natural/business identifier assigned to each product
  , [product_number]    -- NVARCHAR, Alphanumeric identifier used for tracking and referencing products
  , [product_name]      -- NVARCHAR, Name of the product
  , [category_id]       -- NVARCHAR, Identifier for the product’s category
  , [category]          -- NVARCHAR, Name of the product category (e.g., Components, Bikes)
  , [subcategory]       -- NVARCHAR, Subdivision of the category for finer classification
  , [maintenance]       -- NVARCHAR, Indicates maintenance status (e.g., Yes, No)
  , [product_cost]      -- INT, Cost of producing or acquiring the product
  , [product_line]      -- NVARCHAR, Product line grouping (e.g., Road, Mountain)
  , [start_date]        -- DATE, Date when the product became available
FROM [DataWarehouse].[gold].[dim_products];


-- ============================================
-- Data Catalog: gold.fact_sales
-- ============================================
SELECT 
    [order_number]      -- NVARCHAR, Unique identifier for the sales order
  , [product_key]       -- INT, Foreign key linking to gold.dim_products.product_key
  , [customer_key]      -- INT, Foreign key linking to gold.dim_customers.customer_key
  , [order_date]        -- DATE, Date when the order was placed
  , [shipping_date]     -- DATE, Date when the order was shipped
  , [due_date]          -- DATE, Expected delivery or payment due date
  , [sales_amount]      -- INT, Total monetary value of the sales transaction (Quantity * Price)
  , [quantity]          -- INT, Number of product units sold
  , [price]             -- INT, Unit price of the product at the time of sale
FROM [DataWarehouse].[gold].[fact_sales];

--Database Exploration

--Explore All Objects in the Database
SELECT * FROM INFORMATION_SCHEMA.TABLES

--Explore All Columns in the Database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'

--Dimensions Exploration

--Explore All Countries our customer come from
SELECT DISTINCT country FROM gold.dim_customers	

--Explore All Categories "The major Divisions"
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1, 2, 3

--Date Exploration

--Find the First and Last Order, How many Years of Sales are available
SELECT 
	MIN(order_date) first_order_date,
	MAX(order_date) last_order_date,
	DATEDIFF(year, MIN(order_date), MAX(order_date)) orderdt_ranges_years,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) orderdt_ranges_months
FROM gold.fact_sales

--Find the Youngest and Oldest Customer
SELECT
	MIN(birthdate) youngest_customer,
	MAX(birthdate) oldest_customer,
	DATEDIFF(year, MIN(birthdate), MAX(birthdate)) customers_age_range,
	DATEDIFF(year, MIN(birthdate), GETDATE()) oldest_customer,
	DATEDIFF(year, MAX(birthdate), GETDATE()) youngest_customer
FROM gold.dim_customers

--Measures Exploration

--Find the Total Sales, how many items are sold, Average Selling Price, Total Number of Orders
SELECT
	SUM(sales_amount) total_sales,
	SUM(quantity) total_quantity,
	AVG(price) avg_price,
	COUNT(DISTINCT(order_number)) total_orders
FROM gold.fact_sales

--Find the Total Number of Products
SELECT
	COUNT(product_name) total_products
FROM gold.dim_products

--Find the Total Number of Customers
SELECT 
	COUNT(customer_key) total_customers
FROM gold.dim_customers

--Find the Total Number of Customers that has placed an order
SELECT 
	COUNT(DISTINCT(customer_key)) total_customers
FROM gold.fact_sales
	
--Generate a Report that shows all key metrics of the business	

SELECT 'Total Sales' measure_name, SUM(sales_amount) measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT (order_number)) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers
UNION ALL
SELECT 'Total Customers that Order', COUNT(DISTINCT (customer_key)) FROM gold.fact_sales

--Magnitude (Compare the Measure Values by Categories)

--Find Total Customers by Countries
SELECT 
	country, 
	COUNT(customer_key) total_customers 
FROM gold.dim_customers 
GROUP BY country 
ORDER BY total_customers DESC

--Find Total Customers by Gender
SELECT 
	gender, 
	COUNT(customer_key) total_customers 
FROM gold.dim_customers 
GROUP BY gender 
ORDER BY total_customers DESC

--Find Total Products by Category
SELECT 
	category, 
	COUNT(product_key) total_products 
FROM gold.dim_products 
GROUP BY category 
ORDER BY total_products DESC

--What is the average costs in each category?
SELECT 
	category, 
	AVG(product_cost) avg_cost 
FROM gold.dim_products 
GROUP BY category
ORDER BY avg_cost DESC

--What is the total revenue generated for each category?
SELECT 
	p.category, 
	SUM(s.sales_amount) total_revenue 
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC

--Find total revenue is generated by each_customer
SELECT 
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(s.sales_amount) total_revenue 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC

--What is the distribution of sold items across countries?
SELECT
	c.country,
	SUM(quantity) item_sold 
FROM gold.fact_sales s 
LEFT JOIN gold.dim_customers c 
ON s.customer_key = c.customer_key 
GROUP BY c.country
ORDER BY item_sold DESC

--Ranking Top N - Bottom N

--Which 5 products generate the highest revenue?
SELECT *
FROM(
SELECT 
	p.product_name,
	SUM(s.sales_amount) total_revenue,
	ROW_NUMBER() OVER(ORDER BY SUM(s.sales_amount) DESC) rev_rank
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.product_name
--ORDER BY total_revenue DESC
)t
WHERE rev_rank <= 5


--What are the 5 worst-performing products in terms of sales?
SELECT *
FROM(
SELECT TOP 5
	p.product_name,
	SUM(s.sales_amount) total_revenue,
	ROW_NUMBER() OVER(ORDER BY SUM(s.sales_amount)) rev_rank
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.product_name
--ORDER BY total_revenue 
)t
WHERE rev_rank <= 5

--Find the Top 10 Customers who have generated the Highest Revenue
SELECT TOP 10
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(s.sales_amount) total_revenue 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC

SELECT TOP 10 
	sales_amount
FROM gold.fact_sales

--Find the 3 Customers with the Fewer Orders Placed
SELECT TOP 3
	c.customer_key,
	c.first_name,
	c.last_name,
	COUNT(DISTINCT(order_number)) total_orders
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_orders

--=====================================================================================================

--Advanced Analytics

--Change Over-Time Trends
SELECT 
	--YEAR(order_date) order_year
	DATETRUNC(year, order_date) order_date,
	FORMAT(order_date, 'yyyy-MMM') order_date2,
	SUM(sales_amount) total_sales,
	COUNT(DISTINCT(customer_key)) total_customers,
	SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(year, order_date), FORMAT(order_date, 'yyyy-MMM')
ORDER BY DATETRUNC(year, order_date), FORMAT(order_date, 'yyyy-MMM')

--Cumulative Analysis

--Calculate the Total Sales per year and the Running Total of Sales Over Time
SELECT
	order_year,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_year) running_total_sales,
	AVG(avg_price) OVER(ORDER BY order_year) moving_avg_price
FROM(
SELECT
	DATETRUNC(year, order_date) order_year,
	SUM(sales_amount) total_sales,
	AVG(price) avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(year, order_date)
)t;

--Performance Analysis

/* Analyze the Yearly Performance of Products by Comparing their Sales to Both the Average Sale Performance of the Product and the Previous Year's Sales */
WITH yearly_product_sales AS (
SELECT
	YEAR(s.order_date) order_year,
	p.product_name,
	SUM(s.sales_amount) total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE order_date IS NOT NULL
GROUP BY YEAR(s.order_date), p.product_name
)
SELECT
	order_year,
	product_name,
	total_sales,
	AVG(total_sales) OVER(PARTITION BY product_name) avg_sales,
	total_sales - AVG(total_sales) OVER(PARTITION BY product_name) diff_avg,
	CASE 
		WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Average'
		WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Average'
		ELSE 'Avg' 
	END avg_change,
	LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) py_sales,
	total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) diff_py,
	CASE 
		WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year)  > 0 THEN 'Increase' --YoY Analysis
		WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year)  < 0 THEN 'Decrease'
		ELSE 'No Change' 
	END py_change
FROM yearly_product_sales
ORDER BY product_name, order_year

--Part-to-Whole Proportional

--Which Categories Contribute the Most to Overall Sales?
WITH category_sales AS (
SELECT
	p.category,
	SUM(s.sales_amount) total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.category
)
SELECT 
	category,
	total_sales,
	SUM(total_sales) OVER() overall_sales,
	CONCAT(ROUND(CAST(total_sales AS FLOAT) / SUM(total_sales) OVER()*100, 2), '%') percentage_of_sales
FROM category_sales
ORDER BY total_sales DESC

--Data Segmentation

/* Segment Products into Cost Ranges and Count how many Products fall into each Segment */
WITH product_segments AS (
SELECT
	product_key,
	product_name,
	product_cost,
	CASE
		WHEN product_cost < 100 THEN 'Below 100'
		WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
		WHEN product_cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	END cost_range
FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT(product_key) total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC

/* Group Customers into Three Segments based on their sales Behaviour:
	-VIP : Customers with at least 12 months of history and sales more than 5000.
	-Regular : Customers with at least 12 months of history but sales 5000 or less.
	-New : Customers with a lifespan less than 12 months.
And find the total number of customers by each group */

WITH costumer_sales AS (
SELECT 
	c.customer_key,
	SUM(s.sales_amount) total_sales,
	MIN(order_date) first_order,
	MAX(order_date) last_order,
	DATEDIFF(month, MIN(s.order_date), MAX(s.order_date)) lifespan
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT
	customer_status,
	COUNT(customer_key) total_customer
FROM(
SELECT
	customer_key,
	total_sales,
	lifespan,
	CASE
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END customer_status
FROM costumer_sales)t
GROUP BY customer_status
ORDER BY COUNT(customer_key) DESC


--Reporting

/* 
=================================================================================================
Customer Report
=================================================================================================
Purpose:
	-This report consolidates key consumer metrics and behaviours

Highlights:
	1.Gathers essential fields such as names, ages, and transaction details.
	2.Segments customers into categories (VIP, Regular, New) and age groups.
	3.Aggregates customer-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan (in months)
	4.Calculate valuable KPIs:
		-recency (months since last order)
		-average order value
		-average monthly spend
==================================================================================================
*/
CREATE VIEW gold.report_customers AS
--1.Base Query : Retrieves core columns from tables
	WITH base_query AS (
	SELECT
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales_amount,
		s.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name, ' ', c.last_name) customer_name,
		DATEDIFF(year, c.birthdate, GETDATE()) age
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	WHERE order_date IS NOT NULL
	)
	--2.Customer Aggregation
	,customer_aggregation AS (
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) total_orders,
		SUM(sales_amount) total_sales,
		SUM(quantity) total_quantity,
		COUNT(DISTINCT product_key) total_products,
		MAX(order_date) last_order_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) lifespan
	FROM base_query
	GROUP BY
		customer_key,
		customer_number,
		customer_name,
		age
	)
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		CASE
			WHEN age < 20 THEN 'Under 20'
			WHEN age between 20 and 29 THEN '20-29'
			WHEN age between 30 and 39 THEN '30-39'
			WHEN age between 40 and 49 THEN '40-49'
			ELSE '50 and Above'
		END age_group,
		CASE
			WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END customer_status,
		total_orders,
		total_sales,
		total_quantity,
		total_products,
		last_order_date,
		DATEDIFF(month, last_order_date, GETDATE()) recency,
		lifespan,
		--Compute Average Order Value (AVO)
		CASE 
			WHEN total_sales = 0 THEN 0
			ELSE total_sales / total_orders 
		END avg_order_value,
		--Compute Average Monthly Spend 
		CASE
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan
		END avg_monthly_spend
	FROM customer_aggregation

SELECT 
	age_group,
	COUNT(customer_number) total_customers,
	SUM(total_sales) total_sales
FROM gold.report_customers
GROUP BY age_group

/* 
=================================================================================================
Product Report
=================================================================================================
Purpose:
	-This report consolidates key consumer metrics and behaviours

Highlights:
	1.Gathers essential fields such as names, ages, and transaction details.
	2.Segments customers into categories (VIP, Regular, New) and age groups.
	3.Aggregates customer-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan (in months)
	4.Calculate valuable KPIs:
		-recency (months since last order)
		-average order value
		-average monthly spend
==================================================================================================
*/
CREATE VIEW gold.report_products AS
--1.Base Query : Retrieves core columns from tables
	WITH base_query_p AS (
	SELECT
		s.order_number,
		s.customer_key,
		s.order_date,
		s.sales_amount,
		s.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.product_cost
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE order_date IS NOT NULL
	)
	--2.Product Aggregation
	,product_aggregation AS (
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		product_cost,
		COUNT(DISTINCT order_number) total_orders,
		SUM(sales_amount) total_sales,
		SUM(quantity) total_quantity,
		COUNT(DISTINCT customer_key) total_customers,
		MAX(order_date) last_sale_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) lifespan,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) avg_selling_price
	FROM base_query_p
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		product_cost
	)
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		product_cost,
		last_sale_date,
		DATEDIFF(month, last_sale_date, GETDATE()) recency_in_months,
		CASE
			WHEN total_sales > 50000 THEN 'High-Performer'
			WHEN total_sales >= 10000 THEN 'Mid-Range'
			ELSE 'Low-Performer'
		END product_segment,
		lifespan,
		total_orders,
		total_sales,
		total_quantity,
		total_customers,
		avg_selling_price,
		--Compute Average Order Revenue (AOR)
		CASE 
			WHEN total_orders = 0 THEN 0
			ELSE total_sales / total_orders 
		END avg_order_revenue,
		--Compute Average Monthly Revenue
		CASE
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan
		END avg_monthly_revenue
	FROM product_aggregation

	SELECT
		*
	FROM gold.report_products


