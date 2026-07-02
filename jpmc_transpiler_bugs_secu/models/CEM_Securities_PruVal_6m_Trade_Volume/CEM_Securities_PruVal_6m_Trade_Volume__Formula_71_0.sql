{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_36 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'Configuration_t_36') }}

),

Unique_56_window AS (

  SELECT 
    *,
    row_number() OVER (PARTITION BY variableDate, Currency ORDER BY variableDate ASC NULLS FIRST, Currency ASC NULLS FIRST) AS row_number
  
  FROM Configuration_t_36 AS in0

),

Unique_56_filter AS (

  SELECT * 
  
  FROM Unique_56_window AS in0
  
  WHERE (row_number > 1)

),

Unique_56_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM Unique_56_filter AS in0

),

Summarize_70 AS (

  SELECT 
    DISTINCT variableDate AS variableDate,
    Currency AS Currency
  
  FROM Unique_56_drop_0 AS in0

),

Formula_71_0 AS (

  SELECT 
    CAST('Duplicate FX Rate' AS string) AS Name,
    *
  
  FROM Summarize_70 AS in0

)

SELECT *

FROM Formula_71_0
