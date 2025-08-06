-- DATASETS
CREATE SCHEMA IF NOT EXISTS `fluted-legacy-468017-v3.SALES_DIM`
  OPTIONS(location = 'asia-southeast1',
          description = 'Type-2 dimensions for the Sales DW');

CREATE SCHEMA IF NOT EXISTS `fluted-legacy-468017-v3.SALES_FACT`
  OPTIONS(location = 'asia-southeast1',
          description = 'Facts & bridge tables for the Sales DW');

-- DIMENSION TABLE
CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_DIM.DIM_COUNTRY`
(
  Country_ISO   STRING NOT NULL,
  Country_Name  STRING,
  CONSTRAINT pk_dim_country PRIMARY KEY (Country_ISO) NOT ENFORCED
)
OPTIONS(description = 'ISO-3166 country reference list');

CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_DIM.DIM_PRODUCT`
(
  Product_SK     INT64   NOT NULL,
  Product_ID     INT64,
  Product_Name   STRING,
  Cost           NUMERIC,
  Effective_From DATE    NOT NULL,
  Effective_To   DATE,
  Is_Current     BOOL,
  CONSTRAINT pk_dim_product   PRIMARY KEY (Product_SK) NOT ENFORCED,
  CONSTRAINT uq_product_nat   UNIQUE (Product_ID, Effective_From) NOT ENFORCED
)
PARTITION BY DATE(Effective_From)
CLUSTER BY Product_ID
OPTIONS(description = 'Slowly-changing product dimension (T2)');

CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_DIM.DIM_CUSTOMER`
(
  Customer_SK     INT64   NOT NULL,
  Customer_ID     INT64,
  First_Name      STRING,
  Last_Name       STRING,
  DOB             DATE,
  Under30_Flag    BOOL,
  Effective_From  DATE    NOT NULL,
  Effective_To    DATE,
  Is_Current      BOOL,
  CONSTRAINT pk_dim_customer  PRIMARY KEY (Customer_SK) NOT ENFORCED,
  CONSTRAINT uq_customer_nat  UNIQUE (Customer_ID, Effective_From) NOT ENFORCED
)
PARTITION BY DATE(Effective_From)
CLUSTER BY Customer_ID
OPTIONS(description = 'Slowly-changing customer dimension (T2)');

CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_DIM.DIM_SHIP_STATUS`
(
  ShipStatus_SK   INT64   NOT NULL,
  Order_ID        INT64,          -- natural tie-back to the order
  Status          STRING,
  Effective_From  DATE    NOT NULL,
  Effective_To    DATE,
  Is_Current      BOOL,
  CONSTRAINT pk_dim_shipstatus PRIMARY KEY (ShipStatus_SK) NOT ENFORCED,
  CONSTRAINT uq_ship_nat       UNIQUE (Order_ID, Effective_From) NOT ENFORCED
)
PARTITION BY DATE(Effective_From)
CLUSTER BY Order_ID
OPTIONS(description = 'Shipping status history (T2)');

-- FACT TABLES
CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_FACT.FACT_SALES`
(
  Order_ID        INT64      NOT NULL,
  Order_Date      DATETIME,
  Customer_SK     INT64      NOT NULL,
  Country_ISO     STRING,
  ShipStatus_SK   INT64,
  Amount          NUMERIC,   -- store money in NUMERIC for 9-dp accuracy
  CONSTRAINT pk_fact_sales     PRIMARY KEY (Order_ID) NOT ENFORCED,
  CONSTRAINT fk_fact_customer  FOREIGN KEY (Customer_SK)
    REFERENCES `fluted-legacy-468017-v3.SALES_DIM.DIM_CUSTOMER` (Customer_SK) NOT ENFORCED,
  CONSTRAINT fk_fact_country   FOREIGN KEY (Country_ISO)
    REFERENCES `fluted-legacy-468017-v3.SALES_DIM.DIM_COUNTRY` (Country_ISO) NOT ENFORCED,
  CONSTRAINT fk_fact_shipstat  FOREIGN KEY (ShipStatus_SK)
    REFERENCES `fluted-legacy-468017-v3.SALES_DIM.DIM_SHIP_STATUS` (ShipStatus_SK) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(DATE(Order_Date), MONTH)
CLUSTER BY Customer_SK, Country_ISO, ShipStatus_SK
OPTIONS(description = 'Sales fact – one row per order');

-- BRIDGE TABLES
CREATE TABLE IF NOT EXISTS `fluted-legacy-468017-v3.SALES_FACT.BRIDGE_ORDER_PRODUCT`
(
  Order_ID    INT64 NOT NULL,
  Product_SK  INT64 NOT NULL,
  Quantity    INT64,
  CONSTRAINT pk_bridge_op        PRIMARY KEY (Order_ID, Product_SK) NOT ENFORCED,
  CONSTRAINT fk_bridge_order     FOREIGN KEY (Order_ID)
    REFERENCES `fluted-legacy-468017-v3.SALES_FACT.FACT_SALES` (Order_ID) NOT ENFORCED,
  CONSTRAINT fk_bridge_product   FOREIGN KEY (Product_SK)
    REFERENCES `fluted-legacy-468017-v3.SALES_DIM.DIM_PRODUCT` (Product_SK) NOT ENFORCED
)
CLUSTER BY Order_ID, Product_SK
OPTIONS(description = 'Bridge table to explode order-level facts by product');
