{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH excel_file_list AS (

  SELECT * 
  
  FROM {{ ref('excel_file_list')}}

),

Reformat_1 AS (

  SELECT CONCAT(file_path, '|||', sheet_name) AS raw_path
  
  FROM excel_file_list

),

path_struct AS (

  SELECT {{ parse_excel_path('/data/sales_report.xlsx|||Q1_Sales', 'raw_path', 'path') }} AS parsed
  
  FROM Reformat_1 AS reformat_input

),

parse_excel_path_1 AS (

  SELECT 
    parsed.file_path AS file_path,
    parsed.sheet_name AS sheet_name
  
  FROM path_struct

)

SELECT *

FROM parse_excel_path_1
