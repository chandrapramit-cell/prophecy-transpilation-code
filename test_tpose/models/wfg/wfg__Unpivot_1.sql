{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH store_sales_qtrly_csv AS (

  {#Updates the seed dataset and refreshes the target table to reflect latest source data.#}
  SELECT * 
  
  FROM {{ ref('s')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM store_sales_qtrly_csv AS in0

),

Unpivot_1 AS (

  {#Unnests quarterly sales data for each store to expose individual quarter values for analysis.#}
  SELECT 
    StoreID,
    Location,
    Name,
    Value
  
  FROM Reformat_1 AS in0
  UNPIVOT (
    Value
    FOR Name IN (
      Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
    )
  )

)

SELECT *

FROM Unpivot_1
