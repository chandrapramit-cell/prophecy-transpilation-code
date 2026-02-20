{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_815_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_815_inner')}}

),

Filter_880_reject AS (

  SELECT * 
  
  FROM Join_815_inner AS in0
  
  WHERE (
          (
            (`Member Individual Business Entity Key` <> '10000010052')
            OR (`Member Individual Business Entity Key` IS NULL)
          )
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_880_reject
