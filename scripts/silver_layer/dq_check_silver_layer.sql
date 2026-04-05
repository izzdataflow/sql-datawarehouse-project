--Silver Layer

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