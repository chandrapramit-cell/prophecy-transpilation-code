{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH FolderBrowse_74 AS (

  {{
    prophecy_basics.ToDo(
      'Component type: AlteryxGuiToolkit.Questions.FolderBrowse.FolderBrowse is not supported.'
    )
  }}

)

SELECT *

FROM FolderBrowse_74
