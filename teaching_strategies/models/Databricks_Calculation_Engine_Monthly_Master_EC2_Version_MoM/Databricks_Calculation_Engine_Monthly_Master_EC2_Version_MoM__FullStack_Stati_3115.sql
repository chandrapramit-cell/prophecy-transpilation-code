{{
  config({    
    "materialized": "table",
    "alias": "FullStack_Stati_3115",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH FullStack_Stati_2085 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'FullStack_Stati_2085'
    )
  }}

),

Formula_2982_0 AS (

  SELECT 
    CAST((
      CONCAT(
        'C:\\Users\\Public\\Calculation Engine\\Calculation Engine - Monthly_Master - EC2 Version\\Outputs\\Static History\\Full Stack - Static History - ', 
        (
          REGEXP_REPLACE(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST((DATE_TRUNC('hour', CURRENT_TIMESTAMP)) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            ':', 
            '')
        ), 
        '.yxdb')
    ) AS string) AS `Output Name`,
    * EXCEPT (`output name`)
  
  FROM FullStack_Stati_2085 AS in0

)

SELECT *

FROM Formula_2982_0
