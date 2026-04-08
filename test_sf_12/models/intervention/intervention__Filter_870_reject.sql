{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_784 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_784')}}

),

Filter_870_reject AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Union_784 AS in0
  
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

FROM Filter_870_reject
