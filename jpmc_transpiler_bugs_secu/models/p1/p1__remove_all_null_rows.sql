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

),

remove_all_null_rows AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_22_1'], 
      [
        { "name": "Notional", "dataType": "String" }, 
        { "name": "ISIN", "dataType": "String" }, 
        { "name": "Trade Date", "dataType": "Date" }, 
        { "name": "Status", "dataType": "String" }
      ], 
      'Keep original', 
      [], 
      false, 
      'NA', 
      false, 
      0, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      true, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM remove_all_null_rows
