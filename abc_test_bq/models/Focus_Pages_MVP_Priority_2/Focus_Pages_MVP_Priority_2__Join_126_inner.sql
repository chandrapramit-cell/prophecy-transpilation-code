{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_125 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Unique_125')}}

),

AlteryxSelect_133 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__AlteryxSelect_133')}}

),

Join_126_inner AS (

  SELECT in0.*
  
  FROM AlteryxSelect_133 AS in0
  INNER JOIN Unique_125 AS in1
     ON (in0.MBR_SK = in1.MBR_SK)

)

SELECT *

FROM Join_126_inner
