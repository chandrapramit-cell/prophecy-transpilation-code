{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Filter_480 AS (

  SELECT *
  
  FROM {{ ref('p1__Filter_480')}}

),

Formula_215_0 AS (

  SELECT *
  
  FROM {{ ref('p1__Formula_215_0')}}

),

Join_332_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_480 AS in0
  INNER JOIN Formula_215_0 AS in1
     ON (in0.SKU = in1.SOURCE_SKU)

),

AlteryxSelect_338 AS (

  SELECT 
    SKU_STANDARD AS SKU_STANDARD,
    "13_WK_WKLY_AVG_SALES" AS "13_WK_WKLY_AVG_SALES"
  
  FROM Join_332_inner AS in0

)

SELECT *

FROM AlteryxSelect_338
