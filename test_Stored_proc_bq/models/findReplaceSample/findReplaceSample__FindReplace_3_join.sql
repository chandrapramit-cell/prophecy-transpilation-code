{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_2')}}

),

TextInput_2_cast AS (

  SELECT 
    CAST(search_term AS STRING) AS search_term,
    CAST(replacement AS STRING) AS replacement
  
  FROM TextInput_2 AS in0

),

FindReplace_3_allRules AS (

  SELECT ARRAY_AGG(struct(search_term AS search_term, replacement AS replacement)) AS _rules
  
  FROM TextInput_2_cast AS in0

),

TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT 
    CAST(id AS STRING) AS id,
    CAST(description AS STRING) AS description
  
  FROM TextInput_1 AS in0

),

FindReplace_3_join AS (

  SELECT 
    in0.id AS id,
    in0.description AS description,
    in1.`_rules` AS _rules
  
  FROM TextInput_1_cast AS in0
  FULL JOIN FindReplace_3_allRules AS in1
     ON TRUE

)

SELECT *

FROM FindReplace_3_join
