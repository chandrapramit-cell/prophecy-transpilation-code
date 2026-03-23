{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "QA_SCHEMA"
  })
}}

WITH OrchestrationSource_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'OrchestrationSource_1') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM OrchestrationSource_1 AS in0

),

FuzzyMatch_0 AS (

  {{ prophecy_basics.FuzzyMatch(['Reformat_1'], '', '', '', [], 80, false) }}

)

SELECT *

FROM FuzzyMatch_0
