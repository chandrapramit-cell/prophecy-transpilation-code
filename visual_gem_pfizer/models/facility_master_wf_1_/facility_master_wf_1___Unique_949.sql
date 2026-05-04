{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default2"
  })
}}

WITH Summarize_944 AS (

  SELECT COUNT((
           CASE
             WHEN ((1 IS NULL) OR (CAST(1 AS string) = ''))
               THEN NULL
             ELSE 1
           END
         )) AS `Count`
  
  FROM `` AS in0

),

Summarize_945 AS (

  SELECT 
    *,
    MAX(Count) OVER (PARTITION BY city_town_village, GroupID ORDER BY 1 ASC NULLS FIRST) AS Max_Count
  
  FROM Summarize_944 AS in0

),

Summarize_945_filter AS (

  SELECT * 
  
  FROM Summarize_945 AS in0
  
  WHERE (Max_Count = Count)

),

Join_948_inner_formula_0 AS (

  SELECT 
    Count AS `Count`,
    * EXCEPT (`count`)
  
  FROM Summarize_945_filter AS in0

),

Summarize_950 AS (

  SELECT * 
  
  FROM Join_948_inner_formula_0 AS in0

),

Filter_951 AS (

  SELECT * 
  
  FROM Summarize_950 AS in0
  
  WHERE (NOT(GROUPID IS NULL))

),

Unique_949 AS (

  SELECT * 
  
  FROM Filter_951 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CITY_TOWN_VILLAGE, NEW_ORG_NAME_ALTERYX, GROUPID ORDER BY CITY_TOWN_VILLAGE, NEW_ORG_NAME_ALTERYX, GROUPID) = 1

)

SELECT *

FROM Unique_949
