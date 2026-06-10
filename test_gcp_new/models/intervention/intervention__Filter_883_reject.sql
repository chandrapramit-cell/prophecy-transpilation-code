{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Union_866 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_866')}}

),

Filter_883_reject AS (

  SELECT * 
  
  FROM Union_866 AS in0
  
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

FROM Filter_883_reject
