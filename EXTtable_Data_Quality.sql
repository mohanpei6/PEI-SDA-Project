
CREATE OR REPLACE TABLE `fluted-legacy-468017-v3.Data_Quality.Data_Quality_log`
(
  check_timestamp        TIMESTAMP
    OPTIONS(description = 'UTC timestamp when this data-quality run began'),

  table_name             STRING
    OPTIONS(description = 'Name of the source table evaluated in this row'),

  completeness_errors    INT64
    OPTIONS(description = 'Rows that fail completeness (required-column) rules'),

  domain_errors          INT64
    OPTIONS(description = 'Rows with invalid domain values (e.g., bad Status)'),

  referential_errors     INT64
    OPTIONS(description = 'Rows violating referential integrity (orphan FKs)'),

  uniqueness_errors      INT64
    OPTIONS(description = 'Duplicate primary-key occurrences in the source'),

  range_errors           INT64
    OPTIONS(description = 'Rows with out-of-range numeric values'),

  future_date_errors     INT64
    OPTIONS(description = 'Rows with date fields later than the run timestamp'),

  ship_order_link_errors INT64
    OPTIONS(description = 'Shipping records lacking a linked Order record'),

  overall_result         STRING
    OPTIONS(description = 'Overall outcome of the checks: PASS or FAIL')
);



--Declarations
DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

DECLARE table_name             STRING;

DECLARE completeness_errors    INT64;
DECLARE domain_errors          INT64;
DECLARE ref_errors             INT64;
DECLARE dup_errors             INT64;
DECLARE range_errors           INT64;
DECLARE future_date_errors     INT64;
DECLARE ship_order_link_errors INT64;
DECLARE overall_result         STRING;

--CUSTOMER
SET table_name = 'Customer';

BEGIN TRANSACTION;

SET (completeness_errors, domain_errors, ref_errors, dup_errors,
     range_errors, future_date_errors, ship_order_link_errors) = (0,0,0,0,0,0,0);

SET completeness_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Customer`
  WHERE Customer_ID IS NULL OR `First` IS NULL OR Last IS NULL OR Age IS NULL
);--check

SET dup_errors = (
  SELECT COUNT(*) FROM (
    SELECT Customer_ID, COUNT(*) AS cnt
    FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Customer`
    GROUP BY Customer_ID HAVING cnt > 1
  )
);

SET range_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Customer`
  WHERE Age IS NOT NULL AND (Age < 0 OR Age > 120)
);

SET overall_result = IF(
     completeness_errors = 0
 AND dup_errors         = 0
 AND range_errors       = 0,
 'PASS','FAIL');

COMMIT TRANSACTION;

INSERT INTO `fluted-legacy-468017-v3.Data_Quality.Data_Quality_log`
(check_timestamp, table_name, completeness_errors, domain_errors,
 referential_errors, uniqueness_errors, range_errors,
 future_date_errors, ship_order_link_errors, overall_result)
VALUES
(run_ts, table_name, completeness_errors, domain_errors,
 ref_errors, dup_errors, range_errors,
 future_date_errors, ship_order_link_errors, overall_result);

--ORDER
SET table_name = 'Order';

BEGIN TRANSACTION;

SET (completeness_errors, domain_errors, ref_errors, dup_errors,
     range_errors, future_date_errors, ship_order_link_errors) = (0,0,0,0,0,0,0);

SET completeness_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Order`
  WHERE Order_ID IS NULL OR Customer_ID IS NULL OR Amount IS NULL
);

SET ref_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Order` o
  LEFT JOIN `fluted-legacy-468017-v3.PEI_Staging_EXT.Customer` c
  USING (Customer_ID)
  WHERE c.Customer_ID IS NULL
);

SET dup_errors = (
  SELECT COUNT(*) FROM (
    SELECT Order_ID, COUNT(*) AS cnt
    FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Order`
    GROUP BY Order_ID HAVING cnt > 1
  )
);

SET range_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Order`
  WHERE Amount <= 0
);

SET overall_result = IF(
     completeness_errors = 0
 AND ref_errors         = 0
 AND dup_errors         = 0
 AND range_errors       = 0,
 'PASS','FAIL');

COMMIT TRANSACTION;

INSERT INTO `fluted-legacy-468017-v3.Data_Quality.Data_Quality_log`
VALUES
(run_ts, table_name, completeness_errors, domain_errors,
 ref_errors, dup_errors, range_errors,
 future_date_errors, ship_order_link_errors, overall_result);

--SHIPPING DATA

SET table_name = 'Shipping';

BEGIN TRANSACTION;

SET (completeness_errors, domain_errors, ref_errors, dup_errors,
     range_errors, future_date_errors, ship_order_link_errors) = (0,0,0,0,0,0,0);

SET completeness_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Shipping`
  WHERE Shipping_ID IS NULL OR Customer_ID IS NULL OR Status IS NULL
);

SET domain_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Shipping`
  WHERE Status NOT IN ('Pending','Delivered')
);

SET ref_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Shipping` s
  LEFT JOIN `fluted-legacy-468017-v3.PEI_Staging_EXT.Customer` c
  USING (Customer_ID)
  WHERE c.Customer_ID IS NULL
);

SET dup_errors = (
  SELECT COUNT(*) FROM (
    SELECT Shipping_ID, COUNT(*) AS cnt
    FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Shipping`
    GROUP BY Shipping_ID HAVING cnt > 1
  )
);

SET ship_order_link_errors = (
  SELECT COUNT(*)
  FROM `fluted-legacy-468017-v3.PEI_Staging_EXT.Shipping` s
  LEFT JOIN `fluted-legacy-468017-v3.PEI_Staging_EXT.Order` o
    ON s.Customer_ID = o.Customer_ID
  WHERE o.Order_ID IS NULL
);

SET overall_result = IF(
     completeness_errors    = 0
 AND domain_errors          = 0
 AND ref_errors             = 0
 AND dup_errors             = 0
 AND ship_order_link_errors = 0,
 'PASS','FAIL');

COMMIT TRANSACTION;

INSERT INTO `fluted-legacy-468017-v3.Data_Quality.Data_Quality_log`
VALUES
(run_ts, table_name, completeness_errors, domain_errors,
 ref_errors, dup_errors, range_errors,
 future_date_errors, ship_order_link_errors, overall_result);
