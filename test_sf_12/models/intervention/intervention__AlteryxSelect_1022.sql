{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_1021_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_1021_0')}}

),

AlteryxSelect_1022 AS (

  {#VisualGroup: STEP1#}
  SELECT * EXCEPT (`Right_AVG_12_MONTH_BIN_PRED`, 
         `Right_Inflection Index Pred`, 
         `Right_Inflected Indicator Pred`, 
         `MBR_INDV_BE_KEY`)
  
  FROM Formula_1021_0 AS in0

)

SELECT *

FROM AlteryxSelect_1022
