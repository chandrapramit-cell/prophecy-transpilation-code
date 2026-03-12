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

SQLStatement_1 AS (

  SELECT 
    id * 2 AS id_2,
    * EXCEPT (id)
  
  FROM Table_0

)

SELECT *

FROM SQLStatement_1
