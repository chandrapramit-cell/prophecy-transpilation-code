{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AlteryxSelect_890 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_890')}}

),

Filter_1020_reject AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM AlteryxSelect_890 AS in0
  
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

FROM Filter_1020_reject
