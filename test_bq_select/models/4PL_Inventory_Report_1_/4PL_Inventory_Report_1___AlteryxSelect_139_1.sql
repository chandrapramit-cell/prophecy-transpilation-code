{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_138_cast AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___TextInput_138_cast')}}

),

AlteryxSelect_139_1 AS (

  {#Converts file names to text for downstream processing.#}
  SELECT CAST(FileName AS STRING) AS `FileName`
  
  FROM TextInput_138_cast AS in0

)

SELECT *

FROM AlteryxSelect_139_1
