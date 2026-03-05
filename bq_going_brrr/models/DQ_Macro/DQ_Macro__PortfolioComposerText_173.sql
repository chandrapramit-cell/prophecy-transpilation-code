{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH PortfolioComposerText_173 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

)

SELECT *

FROM PortfolioComposerText_173
