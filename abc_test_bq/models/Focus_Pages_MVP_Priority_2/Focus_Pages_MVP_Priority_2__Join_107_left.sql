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

Join_107_left AS (

  SELECT in0.*
  
  FROM Union_103 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MBR_ID
    
    FROM Unique_106 AS in1
    
    WHERE in1.MBR_ID IS NOT NULL
  ) AS in1_keys
     ON (in0.MBR_ID = in1_keys.MBR_ID)
  
  WHERE (in1_keys.MBR_ID IS NULL)

)

SELECT *

FROM Join_107_left
