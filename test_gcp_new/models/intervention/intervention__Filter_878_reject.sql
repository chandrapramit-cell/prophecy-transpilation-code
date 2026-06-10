{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AlteryxSelect_838 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_838')}}

),

Filter_878_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_838 AS in0
  
  WHERE (
          (
            (
              NOT(
                SOURCE_ID = 'FEP')
            ) OR (SOURCE_ID IS NULL)
          )
          OR ((SOURCE_ID = 'FEP') IS NULL)
        )

)

SELECT *

FROM Filter_878_reject
