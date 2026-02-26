{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Filter_111 AS (

  SELECT *
  
  FROM {{ ref('Debtors_Unmatched_Cash_Comment_App__Filter_111')}}

),

Unique_118 AS (

  SELECT * 
  
  FROM Filter_111 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY KEY_9AX ORDER BY KEY_9AX) = 1

)

SELECT *

FROM Unique_118
