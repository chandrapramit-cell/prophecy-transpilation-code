{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_890 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_890')}}

),

Formula_1021_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (MBR_INDV_BE_KEY IS NOT NULL)
          THEN MBR_INDV_BE_KEY
        ELSE `Member Individual Business Entity Key`
      END
    ) AS STRING) AS `Member Individual Business Entity Key`,
    * EXCEPT (`member individual business entity key`)
  
  FROM AlteryxSelect_890 AS in0

)

SELECT *

FROM Formula_1021_0
