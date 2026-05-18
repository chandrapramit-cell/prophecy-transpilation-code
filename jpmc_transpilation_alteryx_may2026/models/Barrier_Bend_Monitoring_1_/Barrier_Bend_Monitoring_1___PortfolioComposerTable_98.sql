{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_84 AS (

  SELECT *
  
  FROM {{ ref('Barrier_Bend_Monitoring_1___AlteryxSelect_84')}}

),

PortfolioComposerTable_98 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

)

SELECT *

FROM PortfolioComposerTable_98
