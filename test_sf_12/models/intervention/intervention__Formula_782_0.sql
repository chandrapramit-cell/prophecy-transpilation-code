{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Productioned_xl_1669 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'Productioned_xl_1669') }}

),

AlteryxSelect_781 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    NONEMERGENT_RATE AS NONEMERGENT_RATE,
    NONEMERGENT_COUNT_PAST_60 AS NONEMERGENT_COUNT_PAST_60,
    ER_COUNT_PAST_2_MONTHS AS ER_COUNT_PAST_2_MONTHS,
    ER_COUNT_PAST_3_MONTHS AS ER_COUNT_PAST_3_MONTHS,
    ED_DSCHG_DT AS ED_DSCHG_DT,
    ER_COUNT AS ER_COUNT,
    LATEST_ED_DX AS LATEST_ED_DX,
    CAST(`Member Individual Business Entity Key` AS string) AS `Member Individual Business Entity Key`,
    `ED Prediction Score` AS `ED Prediction Score`,
    `ED Prediction Value` AS `ED Prediction Value`
  
  FROM Productioned_xl_1669 AS in0

),

Formula_782_0 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    CAST(1 AS DOUBLE) AS EDPREDICTION,
    *
  
  FROM AlteryxSelect_781 AS in0

)

SELECT *

FROM Formula_782_0
