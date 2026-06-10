{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_838 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_838')}}

),

Filter_878 AS (

  SELECT * 
  
  FROM AlteryxSelect_838 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_878
