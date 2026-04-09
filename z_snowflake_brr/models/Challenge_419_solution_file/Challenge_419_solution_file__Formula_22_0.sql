{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiRowFormula_19_0 AS (

  SELECT *
  
  FROM {{ ref('Challenge_419_solution_file__MultiRowFormula_19_0')}}

),

Filter_20 AS (

  {#VisualGroup: Task2#}
  SELECT * 
  
  FROM MultiRowFormula_19_0 AS in0
  
  WHERE ("PURCHASE YEAR" = '2023')

),

Summarize_21 AS (

  {#VisualGroup: Task2#}
  SELECT 
    AVG("PERCENT INCREASE YOY") AS "AVG_PERCENT INCREASE 2023",
    AVG("BASKET PRICE") AS "AVG_BASKET PRICE 2023"
  
  FROM Filter_20 AS in0

),

Formula_22_0 AS (

  {#VisualGroup: Task2#}
  SELECT 
    (("AVG_BASKET PRICE 2023" * 100) + ("AVG_BASKET PRICE 2023" * "AVG_PERCENT INCREASE 2023")) AS "FUNDRAISING GOAL 2024",
    *
  
  FROM Summarize_21 AS in0

)

SELECT *

FROM Formula_22_0
