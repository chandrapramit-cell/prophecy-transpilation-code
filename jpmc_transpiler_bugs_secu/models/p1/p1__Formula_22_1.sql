{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH p1_input_data AS (

  SELECT * 
  
  FROM {{ ref('p1_input_data')}}

),

Formula_22_1 AS (

  {#Standardizes the Notional field formatting to remove problematic characters so financial totals, reporting, and downstream calculations remain accurate and reliable.#}
  SELECT 
    CAST((REGEXP_REPLACE(Notional, '()', '-')) AS string) AS Notional,
    * EXCEPT (`Notional`)
  
  FROM p1_input_data AS in0

)

SELECT *

FROM Formula_22_1
