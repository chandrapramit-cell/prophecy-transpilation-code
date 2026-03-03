{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH random AS (

  SELECT * 
  
  FROM {{ ref('random')}}

),

exclude_id_column AS (

  {#Creates a modified dataset by adding 10 to the id column and including all other fields.#}
  SELECT 
    id + 10 AS id,
    *
  
  FROM random AS in0

)

SELECT *

FROM exclude_id_column
