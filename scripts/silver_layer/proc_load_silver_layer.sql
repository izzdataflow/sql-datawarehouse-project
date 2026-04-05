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