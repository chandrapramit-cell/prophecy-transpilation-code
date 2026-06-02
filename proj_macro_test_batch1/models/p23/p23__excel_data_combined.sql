{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH read_excel_sheets AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p23', 'read_excel_sheets') }}

),

excel_data_combined AS (

  SELECT 
    *,
    current_timestamp() AS loaded_at
  
  FROM read_excel_sheets

)

SELECT *

FROM excel_data_combined
