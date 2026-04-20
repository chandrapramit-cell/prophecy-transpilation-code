{{
  config({    
    "materialized": "table",
    "alias": "REP_INF_WORKFLOW",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_DBATTRIBUTE AS (

  SELECT /*+ parallel(db)*/
         rtrim(to_char(to_date(currvalue, 'YYYYMMDD'), 'DAY')) AS day_of_week
  
  FROM dbattribute AS db
  
  WHERE name = 'businessdate' AND DAY_OF_WEEK != 'SATURDAY'

),

EXPTRANS AS (

  SELECT '1' AS DECISION
  
  FROM SQ_DBATTRIBUTE AS in0

)

SELECT *

FROM EXPTRANS
