{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_834_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_834_left_UnionLeftOuter')}}

),

Formula_814_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_814_0')}}

),

Join_815_left AS (

  SELECT in0.*
  
  FROM Join_834_left_UnionLeftOuter AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Formula_814_0 AS in1
  ) AS in1_keys
     ON (in0.`Member Individual Business Entity Key` = in1_keys.`Member Individual Business Entity Key`)

),

Filter_816 AS (

  SELECT * 
  
  FROM Join_815_left AS in0
  
  WHERE (
          (
            (
              ((READMITPREDICTION = 1) OR (`ED Prediction Score` = 'CRITICAL'))
              OR (`ED Prediction Score` = 'HIGH')
            )
            OR (AdvancedCKD_Risk IS NOT NULL)
          )
          OR (SOURCE_ID = 'FEP')
        )

)

SELECT *

FROM Filter_816
