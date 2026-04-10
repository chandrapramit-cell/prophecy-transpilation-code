{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_62 AS (

  SELECT CASE
           WHEN (
             NOT(
               CAST((
                 DATEDIFF(
                   'day', 
                   (
                     TO_DATE(
                       CAST({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }} AS STRING), 
                       'YYYY-MM-DD HH2424:MI:SS.FF4')
                   ), 
                   (TO_DATE(CAST(CURRENT_DATE AS STRING), 'YYYY-MM-DD HH2424:MI:SS.FF4')))
               ) AS INTEGER) < 0)
           )
             THEN TRUE
           ELSE 1 / 0
         END AS check_config62

)

SELECT *

FROM Error_62
