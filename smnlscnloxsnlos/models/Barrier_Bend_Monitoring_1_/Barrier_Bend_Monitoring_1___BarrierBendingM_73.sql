{{
  config({    
    "materialized": "table",
    "alias": var('BarrierBendingM_73'),
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_72_3 AS (

  SELECT *
  
  FROM {{ ref('Barrier_Bend_Monitoring_1___Formula_72_3')}}

)

SELECT *

FROM Formula_72_3
