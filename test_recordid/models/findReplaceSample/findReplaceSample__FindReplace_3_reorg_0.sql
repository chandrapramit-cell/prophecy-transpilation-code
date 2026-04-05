{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT 
    CAST(id AS string) AS id,
    CAST(description AS string) AS description
  
  FROM TextInput_1 AS in0

),

TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_2')}}

),

TextInput_2_cast AS (

  SELECT 
    CAST(search_term AS string) AS search_term,
    CAST(replacement AS string) AS replacement
  
  FROM TextInput_2 AS in0

),

FindReplace_3_allRules AS (

  SELECT collect_list(struct(search_term AS search_term, replacement AS replacement)) AS _rules
  
  FROM TextInput_2_cast AS in0

),

FindReplace_3_join AS (

  SELECT 
    in0.id AS id,
    in0.description AS description,
    in1._rules AS _rules
  
  FROM TextInput_1_cast AS in0
  FULL JOIN FindReplace_3_allRules AS in1
     ON TRUE

),

FindReplace_3_0 AS (

  SELECT 
    aggregate(
      _rules, 
      `description`, 
      (acc, rule) -> regexp_replace(acc, concat('(?i)', rule['search_term']), rule['replacement'])) AS description,
    * EXCEPT (`description`)
  
  FROM FindReplace_3_join AS in0

),

FindReplace_3_reorg_0 AS (

  SELECT * EXCEPT (`_rules`)
  
  FROM FindReplace_3_0 AS in0

)

SELECT *

FROM FindReplace_3_reorg_0
