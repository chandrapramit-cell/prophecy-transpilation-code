{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_107_left AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Join_107_left')}}

),

Formula_131_0 AS (

  SELECT 
    CAST((
      CAST((DATEDIFF((PARSE_DATE('%Y-%m-%d', CLM_SVC_STRT_DT_SK)), (PARSE_DATE('%Y-%m-%d', CAST(CURRENT_DATE AS STRING))))) AS INTEGER)
      / 7
    ) AS DOUBLE) AS Timed,
    *
  
  FROM Join_107_left AS in0

),

Formula_131_1 AS (

  SELECT 
    (
      CASE
        WHEN (
          (
            (
              (((Trimester = '> 40 Weeks') AND (Timed < -4)) OR ((Trimester = 'Third Trimester') AND (Timed < -17)))
              OR ((Trimester = 'Second Trimester') AND (Timed < -31))
            )
            OR ((Trimester = 'First Trimester') AND (Timed < -44))
          )
          OR ((Trimester = 'Unspecified Trimester') AND (Timed < -44))
        )
          THEN 1
        ELSE 0
      END
    ) AS Timeout,
    *
  
  FROM Formula_131_0 AS in0

),

Filter_132 AS (

  SELECT * 
  
  FROM Formula_131_1 AS in0
  
  WHERE (Timeout <> CAST('1' AS INT64))

),

AlteryxSelect_133 AS (

  SELECT * EXCEPT (`Timed`, `Timeout`)
  
  FROM Filter_132 AS in0

)

SELECT *

FROM AlteryxSelect_133
