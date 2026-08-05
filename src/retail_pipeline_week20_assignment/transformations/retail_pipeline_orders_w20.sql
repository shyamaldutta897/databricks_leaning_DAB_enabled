--Landing to Bronze
CREATE OR REFRESH STREAMING TABLE orders_w20
COMMENT "loading raw data for week 20 assignment to bronze layer"
TBLPROPERTIES('quality'='bronze')
AS
SELECT *,
_metadata.file_name as file_name,
current_timestamp() as ingest_time
FROM cloud_files ('abfss://week20assignment@stgsdpersonaldev.dfs.core.windows.net/landingzone/orders','csv',map('inferColumnTypes','true'));

--Bronze to Silver

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_cleaned_w20
(
    CONSTRAINT order_id_check EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT customer_id_check EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT order_date_check EXPECT (order_date IS NOT NULL OR typeof(order_date)='date') ON VIOLATION DROP ROW,
    CONSTRAINT order_amt_check EXPECT (order_amount>0) ON VIOLATION DROP ROW,
    CONSTRAINT status_check EXPECT (status IS NOT NULL)
)
AS
SELECT
orderid AS order_id,
customerid AS customer_id,
orderdate AS order_date,
order_amount,
order_status AS status
FROM STREAM(orders_w20);

--Enabling SCD

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_cleaned_scd_w20;

CREATE FLOW orders_scd1_flow AS
AUTO CDC INTO skd_personal_silver.cleaned.orders_cleaned_scd_w20
FROM STREAM(skd_personal_silver.cleaned.orders_cleaned_w20)
KEYS(order_id)
SEQUENCE BY(order_date)
STORED AS SCD TYPE 1;
