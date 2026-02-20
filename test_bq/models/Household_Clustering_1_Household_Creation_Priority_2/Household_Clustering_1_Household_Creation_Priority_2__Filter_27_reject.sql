{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_13 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Filter_13')}}

),

Filter_27_reject AS (

  SELECT * 
  
  FROM Filter_13 AS in0
  
  WHERE (
          (
            NOT(
              (
                CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('suite'))) > 0) AS BOOL)
                OR CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('PO box'))) > 0) AS BOOL)
              )
              OR CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('state highway'))) > 0) AS BOOL))
          )
          OR (
               (
                 (
                   CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('suite'))) > 0) AS BOOL)
                   OR CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('PO box'))) > 0) AS BOOL)
                 )
                 OR CAST(((STRPOS((coalesce(LOWER(Address), '')), LOWER('state highway'))) > 0) AS BOOL)
               ) IS NULL
             )
        )

)

SELECT *

FROM Filter_27_reject
