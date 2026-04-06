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

