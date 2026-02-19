{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_125 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Unique_125')}}

),

AlteryxSelect_133 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__AlteryxSelect_133')}}

),

Join_126_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_133 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MBR_SK
    
    FROM Unique_125 AS in1
    
    WHERE in1.MBR_SK IS NOT NULL
  ) AS in1_keys
     ON (in0.MBR_SK = in1_keys.MBR_SK)
  
  WHERE (in1_keys.MBR_SK IS NULL)

),

Summarize_108 AS (

  SELECT 
    (MAX(Trimester_INT) OVER (PARTITION BY MBR_ID ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS Max_Trimester_INT,
    (MAX(CLM_SVC_STRT_DT_SK) OVER (PARTITION BY MBR_ID ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS Max_CLM_SVC_STRT_DT_SK,
    *
  
  FROM Join_126_left AS in0

),

Summarize_108_filter AS (

  SELECT * 
  
  FROM Summarize_108 AS in0
  
  WHERE ((Max_Trimester_INT = Trimester_INT) AND (Max_CLM_SVC_STRT_DT_SK = CLM_SVC_STRT_DT_SK))

),

Join_109_inner_formula_0 AS (

  SELECT 
    CLM_SVC_STRT_DT_SK AS `Service Date`,
    CLM_ID AS `Claim ID`,
    *
  
  FROM Summarize_108_filter AS in0

)

SELECT *

FROM Join_109_inner_formula_0
