{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('boeing__AlteryxSelect_25')}}

),

Unique_97 AS (

  SELECT * 
  
  FROM AlteryxSelect_25 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DPTR_DT, FLT_NBR, LEG_OD, REGION, OPER_CARR_AIRLINE_CODE, EQUIPMENT_TYPE, IATA_CODE, CABIN ORDER BY DPTR_DT, FLT_NBR, LEG_OD, REGION, OPER_CARR_AIRLINE_CODE, EQUIPMENT_TYPE, IATA_CODE, CABIN) = 1

)

SELECT *

FROM Unique_97
