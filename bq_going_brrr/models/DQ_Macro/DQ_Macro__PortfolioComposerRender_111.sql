{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_112 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__AlteryxSelect_112')}}

),

PortfolioComposerRender_111 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_111
