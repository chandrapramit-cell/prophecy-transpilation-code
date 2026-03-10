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

Reformat_2 AS (

  SELECT 
    SPLIT(col, ',') AS col_arr,
    * EXCEPT (col)
  
  FROM Table_0 AS in0

),

FlattenSchema_1 AS (

  SELECT 
    * EXCEPT (col_arr, target_col),
    CONCAT(target_col, "STR") AS target_col
  
  FROM Reformat_2 AS in0
  LEFT JOIN UNNEST(col_arr) AS target_col

)

SELECT *

FROM FlattenSchema_1
