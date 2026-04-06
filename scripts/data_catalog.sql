--Data Catalog
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

