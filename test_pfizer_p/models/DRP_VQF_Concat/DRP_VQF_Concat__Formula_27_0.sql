{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Cleanse_24 AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__Cleanse_24')}}

),

Formula_27_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(C1071007, ' ', '_')) AS string) AS C1071007,
    CAST((REGEXP_REPLACE(c10071032, ' ', '_')) AS string) AS c10071032,
    CAST((REGEXP_REPLACE(old_field, ' ', '_')) AS string) AS old_field,
    * EXCEPT (`c1071007`, `c10071032`, `old_field`)
  
  FROM Cleanse_24 AS in0

)

SELECT *

FROM Formula_27_0
