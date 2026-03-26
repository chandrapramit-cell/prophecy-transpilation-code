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

transpose_store_sales_quad_to_dimensions AS (

  {#Reformats store sales data quarterly for easier analysis and naming of value-focused rows.#}
  {{
    prophecy_basics.Transpose(
      ['store_sales_qtrly_csv'], 
      ['StoreID', 'Location'], 
      ['Q1_Sales', 'Q2_Sales', 'Q3_Sales', 'Q4_Sales'], 
      'Name', 
      'Value', 
      ['StoreID', 'Location', 'Q1_Sales', 'Q2_Sales', 'Q3_Sales', 'Q4_Sales'], 
      false
    )
  }}

)

SELECT *

FROM transpose_store_sales_quad_to_dimensions
