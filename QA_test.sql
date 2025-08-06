
-- Schema Validation
--Verify that the following fields exist in the raw/external tables:
SELECT
  table_name,
  column_name,
  data_type,
  is_nullable
FROM
  `fluted-legacy-468017-v3.PEI_Staging_EXT.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name IN ('Customer','Order','Shipping')
  AND column_name IN (
    'Date_of_Birth','Country_ISO',
    'Order_Date','Product_ID','Quantity',
    'Order_ID','Shipping_Date','Status_Update_Date'
);

--Referential-Integrity--
-- Orphan shipments (Shipping.Order_ID--Fact_Sales.Order_ID)--expect zero orphan rows.
SELECT
  COUNT(*) AS orphan_shipments
FROM
  `fluted-legacy-468017-v3.SALES_DIM.DIM_SHIP_STATUS` ss
LEFT JOIN
  `fluted-legacy-468017-v3.SALES_FACT.FACT_SALES` f
ON ss.Order_ID = f.Order_ID
WHERE f.Order_ID IS NULL;

-- Orphan orders--customers--expect zero orphan rows.
  COUNT(*) AS orphan_orders_customers
FROM
  `fluted-legacy-468017-v3.SALES_FACT.FACT_SALES` f
LEFT JOIN
  `fluted-legacy-468017-v3.SALES_DIM.DIM_CUSTOMER` c
ON f.Customer_SK = c.Customer_SK
WHERE c.Customer_SK IS NULL;

-- Bridge--product--expect zero orphan rows.
SELECT
  COUNT(*) AS orphan_bridge_products
FROM
  `fluted-legacy-468017-v3.SALES_FACT.BRIDGE_ORDER_PRODUCT` br
LEFT JOIN
  `fluted-legacy-468017-v3.SALES_DIM.DIM_PRODUCT` p
ON br.Product_SK = p.Product_SK
WHERE p.Product_SK IS NULL;

-- Check for SCD operationality--expect more than one row
SELECT
  Customer_ID,
  COUNT(*) AS version_count
FROM
  `fluted-legacy-468017-v3.SALES_DIM.DIM_CUSTOMER`
GROUP BY Customer_ID
HAVING COUNT(*) > 1;

-- Under 30 flag operationality--expect zero rows.
SELECT
  Customer_SK,
  DOB,
  Under30_Flag,
  DATE_DIFF(CURRENT_DATE(), DOB, YEAR) < 30 AS expected_flag
FROM
  `fluted-legacy-468017-v3.SALES_DIM.DIM_CUSTOMER`
WHERE
  Under30_Flag != (DATE_DIFF(CURRENT_DATE(), DOB, YEAR) < 30);
