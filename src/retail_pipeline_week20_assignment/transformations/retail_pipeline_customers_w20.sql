--Landing to Bronze

CREATE OR REFRESH STREAMING TABLE customers_w20
COMMENT "loading raw data for week 20 assignment to bronze layer"
TBLPROPERTIES('quality'='bronze')
AS
SELECT *,
_metadata.file_name as file_name,
current_timestamp() as ingest_time
FROM cloud_files ('abfss://week20assignment@stgsdpersonaldev.dfs.core.windows.net/landingzone/customers','csv',map('inferColumnTypes','true'));

--Bronze to Silver

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_w20
(
    CONSTRAINT customer_id_check EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT customer_name_check EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT email_check EXPECT (email like '%@%'),
    CONSTRAINT city_check EXPECT (city IS NOT NULL),
    CONSTRAINT created_at_check EXPECT (created_date IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
customer_id,
customer_name,
email,
city,
created_date
FROM STREAM(customers_w20);

--Enabling SCD2

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_scd2_w20;

CREATE FLOW customers_scd2 AS
AUTO CDC INTO skd_personal_silver.cleaned.customers_cleaned_scd2_w20
FROM STREAM(skd_personal_silver.cleaned.customers_cleaned_w20)
KEYS(customer_id)
SEQUENCE BY(created_date)
STORED AS SCD TYPE 2