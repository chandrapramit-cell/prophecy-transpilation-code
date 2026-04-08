{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AlteryxSelect_838 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_838')}}

),

Filter_878 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM AlteryxSelect_838 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_878
