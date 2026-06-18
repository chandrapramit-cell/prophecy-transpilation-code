{{
  config({    
    "materialized": "table",
    "alias": var('BarrierBendingM_102'),
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_85_0 AS (

  SELECT *
  
  FROM {{ ref('Barrier_Bend_Monitoring_1___Formula_85_0')}}

)

SELECT *

FROM Formula_85_0
