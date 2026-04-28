{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SEQ_clid_0 AS (

  SELECT 
    (((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) + 1) AS NEXTVAL,
    ((((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) + 1) - 1) AS CURRVAL
  
  FROM `` AS in0

)

SELECT *

FROM SEQ_clid_0
