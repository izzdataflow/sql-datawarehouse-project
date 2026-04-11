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
