{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_106 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Unique_106')}}

),

Union_103 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Union_103')}}

),

Join_107_inner AS (

  SELECT in0.*
  
  FROM Union_103 AS in0
  INNER JOIN Unique_106 AS in1
     ON (in0.MBR_ID = in1.MBR_ID)

)

SELECT *

FROM Join_107_inner
