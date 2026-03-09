{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Download_1_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('49_2024_Day_3_Day_3', 'Download_1_1') }}

),

Download_1_1_cleanup_0 AS (

  SELECT 
    api_data AS DownloadData,
    NULL AS DownloadHeaders,
    * EXCEPT (`_headers`, `_paramsOrData`, `_processedUrl`, `api_data`)
  
  FROM Download_1_1 AS in0

)

SELECT *

FROM Download_1_1_cleanup_0
