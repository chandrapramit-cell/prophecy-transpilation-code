{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_866 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_866')}}

),

Filter_883 AS (

  SELECT * 
  
  FROM Union_866 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_883
