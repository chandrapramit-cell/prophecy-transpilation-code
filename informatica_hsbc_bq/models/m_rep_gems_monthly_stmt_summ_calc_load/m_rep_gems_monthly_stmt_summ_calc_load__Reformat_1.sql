{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Table_1 AS (

  SELECT * 
  
  FROM {{ ref('edgef')}}

),

Reformat_1 AS (

  SELECT (
           CASE
             WHEN (
               (
                 CASE
                   WHEN (DATE_ADD(DATE(2006, 4, 30), INTERVAL -1 MONTH) IS NULL)
                     THEN 'Y'
                   ELSE 'N'
                 END
               ) = 'Y'
             )
               THEN PREV_VAL_CREDITS
             ELSE "0"
           END
         ) AS targ
  
  FROM Table_1 AS in0

)

SELECT *

FROM Reformat_1
