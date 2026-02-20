{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_alxaa2_Quer_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_1_ref') }}

),

Filter_5 AS (

  SELECT * 
  
  FROM aka_alxaa2_Quer_1 AS in0
  
  WHERE ((CAST((SUBSTRING(GRP_ID, 1, 2)) AS STRING) <> '65') OR ((SUBSTRING(GRP_ID, 1, 2)) IS NULL))

),

Sort_3 AS (

  SELECT * 
  
  FROM Filter_5 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, ACTVTY_YR_MO_SK ASC, PROD_RANK ASC

),

Unique_4 AS (

  SELECT * 
  
  FROM Sort_3 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_INDV_BE_KEY, ACTVTY_YR_MO_SK ORDER BY MBR_INDV_BE_KEY, ACTVTY_YR_MO_SK) = 1

)

SELECT *

FROM Unique_4
