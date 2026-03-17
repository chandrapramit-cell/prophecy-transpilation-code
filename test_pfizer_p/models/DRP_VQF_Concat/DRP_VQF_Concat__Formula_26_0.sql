{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Cleanse_21 AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__Cleanse_21')}}

),

Formula_26_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, ' ', '_')) AS string) AS Name,
    * EXCEPT (`name`)
  
  FROM Cleanse_21 AS in0

)

SELECT *

FROM Formula_26_0
