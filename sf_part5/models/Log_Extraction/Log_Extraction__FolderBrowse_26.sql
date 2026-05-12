{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH FolderBrowse_26 AS (

  {{
    prophecy_basics.ToDo(
      'Component type: AlteryxGuiToolkit.Questions.FolderBrowse.FolderBrowse is not supported.'
    )
  }}

)

SELECT *

FROM FolderBrowse_26
