{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Mappings_xlsx_Q_578 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'Mappings_xlsx_Q_578') }}

),

Formula_579_0 AS (

  SELECT 
    CAST((CONCAT((SUBSTRING(FileName, 1, (((LOCATE('|||', FileName)) - 1) + 3))), `Sheet Names`)) AS STRING) AS File_Path_Sheet,
    *
  
  FROM Mappings_xlsx_Q_578 AS in0

)

SELECT *

FROM Formula_579_0
