{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_135 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sort_135')}}

),

Sample_142 AS (

  {{ prophecy_basics.Sample('Sort_135', ['MBR_INDV_BE_KEY'], 1002, 'firstN', 3) }}

)

SELECT *

FROM Sample_142
