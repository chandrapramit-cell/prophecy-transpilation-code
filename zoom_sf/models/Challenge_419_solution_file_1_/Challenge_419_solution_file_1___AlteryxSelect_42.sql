{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiRowFormula_19_0 AS (

  SELECT *
  
  FROM {{ ref('Challenge_419_solution_file_1___MultiRowFormula_19_0')}}

),

Formula_45_0 AS (

  {#VisualGroup: Task1#}
  SELECT 
    (
      (
        CASE
          WHEN (
            (("PERCENT INCREASE YOY" / 0.01) < 0)
            AND ((("PERCENT INCREASE YOY" / 0.01) - FLOOR(("PERCENT INCREASE YOY" / 0.01))) = 0.5)
          )
            THEN CEIL(("PERCENT INCREASE YOY" / 0.01))
          ELSE ROUND(("PERCENT INCREASE YOY" / 0.01))
        END
      )
      * 0.01
    ) AS "PERCENT INCREASE YOY",
    * EXCLUDE ("PERCENT INCREASE YOY")
  
  FROM MultiRowFormula_19_0 AS in0

),

CrossTab_41 AS (

  {#VisualGroup: Task1#}
  SELECT 
    1 AS BASET_LIST
  
  FROM Formula_45_0 AS in0
  PIVOT (
    SUM("PERCENT INCREASE YOY")
    FOR "PURCHASE YEAR"
    IN (
      '2021', '2022', '2023'
    )
  )

),

CrossTab_41_rename AS (

  {#VisualGroup: Task1#}
  SELECT 
    1 AS "2021",
    1 AS "2022",
    1 AS "2023"
  
  FROM CrossTab_41 AS in0

),

AlteryxSelect_42 AS (

  {#VisualGroup: Task1#}
  SELECT 
    2022 AS "PERCENT INCREASE 2021 TO 2022",
    2023 AS "PERCENT INCREASE 2022 TO 2023",
    * EXCLUDE ("2021", "2022", "2023")
  
  FROM CrossTab_41_rename AS in0

)

SELECT *

FROM AlteryxSelect_42
