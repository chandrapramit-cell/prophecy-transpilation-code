{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Table_0 AS (

  SELECT * 
  
  FROM {{ ref('sf')}}

),

Reformat_1 AS (

  SELECT 
    RPAD(REVERSE(bqutil.fn.to_binary(val)), 10, '0') AS val_bin,
    *
  
  FROM Table_0 AS in0

)

SELECT *

FROM Reformat_1
