
WITH schema_info AS (
  SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
  FROM
    `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.INFORMATION_SCHEMA.COLUMNS
  WHERE
    table_name IN ('Customer','Order','Shipping')
),

row_counts AS (
  SELECT 'Customer' AS table_name, COUNT(*) AS row_count
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  UNION ALL

  SELECT 'Order'    AS table_name, COUNT(*) AS row_count  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Order

  UNION ALL

  SELECT 'Shipping' AS table_name, COUNT(*) AS row_count
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Shipping
),

null_counts AS (
  -- CUSTOMER
  SELECT 'Customer' AS table_name, 'Customer_ID' AS column_name,
         COUNTIF(Customer_ID IS NULL) AS null_count, COUNT(*) AS total_count
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  UNION ALL
  SELECT 'Customer','First',
         COUNTIF(First IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  UNION ALL
  SELECT 'Customer','Last',
         COUNTIF(Last IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  UNION ALL
  SELECT 'Customer','Age',
         COUNTIF(Age IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  UNION ALL
  SELECT 'Customer','Country',
         COUNTIF(Country IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Customer

  -- ORDER
  UNION ALL
  SELECT 'Order','Order_ID',
         COUNTIF(Order_ID IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Order

  UNION ALL
  SELECT 'Order','Item',
         COUNTIF(Item IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Order

  UNION ALL
  SELECT 'Order','Amount',
         COUNTIF(Amount IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Order

  UNION ALL
  SELECT 'Order','Customer_ID',
         COUNTIF(Customer_ID IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Order

  -- SHIPPING
  UNION ALL
  SELECT 'Shipping','Shipping_ID',
         COUNTIF(Shipping_ID IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Shipping

  UNION ALL
  SELECT 'Shipping','Status',
         COUNTIF(Status IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Shipping

  UNION ALL
  SELECT 'Shipping','Customer_ID',
         COUNTIF(Customer_ID IS NULL), COUNT(*)
  FROM   `fluted-legacy-468017-v3`.`PEI_Staging_EXT`.Shipping
)

SELECT
  s.table_name,
  s.column_name,
  s.data_type,
  s.is_nullable,
  IFNULL(n.null_count, 0)    AS null_count,
  r.row_count
FROM schema_info AS s
LEFT JOIN null_counts AS n
  ON s.table_name  = n.table_name
 AND s.column_name = n.column_name
LEFT JOIN row_counts AS r
  ON s.table_name = r.table_name
ORDER BY
  s.table_name,
  s.column_name;
