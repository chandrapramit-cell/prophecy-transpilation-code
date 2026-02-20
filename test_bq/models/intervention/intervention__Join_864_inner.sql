{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_860 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_860')}}

),

Unique_1018 AS (

  SELECT *
  
  FROM {{ ref('intervention__Unique_1018')}}

),

Join_864_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM Unique_1018 AS in0
  INNER JOIN AlteryxSelect_860 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

)

SELECT *

FROM Join_864_inner
