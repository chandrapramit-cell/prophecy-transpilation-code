{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Union_159 AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Union_159')}}

),

Summarize_158 AS (

  SELECT COUNT(
           (
             CASE
               WHEN ((idf_pers_ods IS NULL) OR (CAST(idf_pers_ods AS STRING) = ''))
                 THEN NULL
               ELSE 1
             END
           )) AS `Count`
  
  FROM Union_159 AS in0

)

SELECT *

FROM Summarize_158
