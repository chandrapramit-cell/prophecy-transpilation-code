{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_850_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_850_left_UnionLeftOuter')}}

),

Filter_1649_reject AS (

  SELECT * 
  
  FROM Join_850_left_UnionLeftOuter AS in0
  
  WHERE (
          (`Member Individual Business Entity Key` <> '10000010052')
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_1649_reject
