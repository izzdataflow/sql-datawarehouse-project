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