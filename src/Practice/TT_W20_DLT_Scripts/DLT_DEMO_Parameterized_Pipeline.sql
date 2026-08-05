-- Databricks notebook source
--This is a example of parameterizing a script and then setting the parameters up in the pipeline.
CREATE OR REFRESH STREAMING TABLE baby_names_raw
COMMENT "Popular baby names since 1880"
AS
SELECT Year, `First Name`as name, County, Sex, Count 
FROM STREAM( read_files
(
"/Volumes/${my_catalog}/${my_schema}/${my_volume}", 
--"/Volumes/skd_personal_bronze/new_schema/delta_volume_new",
format=>'csv',
header=>'true',
mode=>'FAILFAST'

))


-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW skd_personal_silver.cleaned.baby_names_prepared
(
    CONSTRAINT valid_name EXPECT (baby_name IS NOT NULL),
    CONSTRAINT valid_pct EXPECT (baby_count>=0) ON VIOLATION FAIL UPDATE
)
COMMENT "baby names preparing for analysis"
AS
SELECT Year AS yob, name as baby_name, Count as baby_count
FROM baby_names_raw

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW baby_names_count
COMMENT "total baby count"
SELECT baby_name, sum(baby_count) AS total_count
FROM skd_personal_silver.cleaned.baby_names_prepared
GROUP BY ALL
ORDER BY sum(baby_count) DESC
