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

Formula_1021_0 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    CAST((
      CASE
        WHEN (NOT(MBR_INDV_BE_KEY IS NULL))
          THEN MBR_INDV_BE_KEY
        ELSE `Member Individual Business Entity Key`
      END
    ) AS string) AS `Member Individual Business Entity Key`,
    * EXCEPT (`member individual business entity key`)
  
  FROM AlteryxSelect_890 AS in0

)

SELECT *

FROM Formula_1021_0
