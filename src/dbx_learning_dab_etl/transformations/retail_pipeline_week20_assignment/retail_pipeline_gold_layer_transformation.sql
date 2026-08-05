-- Silver to Gold
-- Making a trusted dataset, using Silver layer tabled (SCD enabled)

CREATE MATERIALIZED VIEW skd_personal_gold.curated.centralised_order_data_w20 AS
SELECT
o.order_id,
CAST(o.order_date AS date),
o.order_amount,
o.status AS order_status,
c.customer_name,
c.city,
p.payment_date,
p.payment_status

FROM
skd_personal_silver.cleaned.orders_cleaned_scd_w20 o
LEFT JOIN skd_personal_silver.cleaned.customers_cleaned_scd2_w20 c 
ON o.customer_id=c.customer_id
AND c.__END_AT IS NULL
LEFT JOIN skd_personal_silver.cleaned.payments_cleaned_scd_w20 p
ON o.order_id=p.payment_id;

--Preparing specific views for business analysis
-- Revenue by city
CREATE OR REFRESH MATERIALIZED VIEW skd_personal_gold.curated.revenue_by_city_w20 AS
SELECT
city,
SUM(order_amount) as revenue_total
FROM
skd_personal_gold.curated.centralised_order_data_w20
GROUP BY ALL;

-- Revenue by month
CREATE OR REFRESH MATERIALIZED VIEW skd_personal_gold.curated.revenue_by_month_w20 AS
SELECT
MONTH(order_date) AS payment_month,
SUM(order_amount) as revenue_total
FROM
skd_personal_gold.curated.centralised_order_data_w20
GROUP BY ALL


