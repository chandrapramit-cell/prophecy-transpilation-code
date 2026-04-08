{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_814_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_814_0')}}

),

Join_834_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_834_left_UnionLeftOuter')}}

),

Join_815_left AS (

  {#VisualGroup: STEP1#}
  SELECT in0.*
  
  FROM Join_834_left_UnionLeftOuter AS in0
  ANTI JOIN Formula_814_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

),

Filter_816 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_815_left AS in0
  
  WHERE (
          (
            (
              ((READMITPREDICTION = 1) OR (`ED Prediction Score` = 'CRITICAL'))
              OR (`ED Prediction Score` = 'HIGH')
            )
            OR (NOT(AdvancedCKD_Risk IS NULL))
          )
          OR (SOURCE_ID = 'FEP')
        )

)

SELECT *

FROM Filter_816
