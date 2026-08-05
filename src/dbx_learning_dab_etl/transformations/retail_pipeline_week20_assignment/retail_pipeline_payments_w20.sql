--Landing to Bronze
CREATE OR REFRESH STREAMING TABLE payments_w20
COMMENT "loading raw data for week 20 assignment to bronze layer"
TBLPROPERTIES('quality'='bronze')
AS
SELECT *,
_metadata.file_name as file_name,
current_timestamp() as ingest_time
FROM cloud_files ('abfss://week20assignment@stgsdpersonaldev.dfs.core.windows.net/landingzone/payments','csv',map('inferColumnTypes','true'));

--Bronze to Silver
CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.payments_cleaned_w20
(
    CONSTRAINT payment_id_check EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT order_id_check EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT status_check EXPECT (payment_status IN ('SUCCESS','FAILED'))
)
AS
SELECT
payment_id,
order_id,
payment_date,
payment_mode,
payment_status
FROM STREAM(payments_w20);

--Enabling SCD
CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.payments_cleaned_scd_w20;

CREATE FLOW payments_scd1_flow AS
AUTO CDC INTO skd_personal_silver.cleaned.payments_cleaned_scd_w20
FROM STREAM(skd_personal_silver.cleaned.payments_cleaned_w20)
KEYS(order_id)
SEQUENCE BY(payment_date)
STORED AS SCD TYPE 1;