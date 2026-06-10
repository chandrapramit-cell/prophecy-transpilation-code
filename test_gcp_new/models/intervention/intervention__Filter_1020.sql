{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AlteryxSelect_890 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_890')}}

),

Filter_1020 AS (

  SELECT * 
  
  FROM AlteryxSelect_890 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_1020
