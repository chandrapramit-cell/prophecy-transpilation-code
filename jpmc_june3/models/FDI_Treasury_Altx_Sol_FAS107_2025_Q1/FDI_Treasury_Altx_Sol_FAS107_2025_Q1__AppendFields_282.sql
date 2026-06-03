{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_220_to_Formula_286_2 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2')}}

),

Formula_273_1 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_273_1')}}

),

Formula_280_0 AS (

  SELECT 
    CAST(year(CD_NXTMATDT) AS STRING) AS Year_CD_NXTMATDT,
    *
  
  FROM Formula_273_1 AS in0

),

Summarize_279 AS (

  SELECT 
    SUM(CAST(CD_VALUE AS DECIMAL (19, 9))) AS Sum_CD_VALUE,
    COUNT(
      (
        CASE
          WHEN ((RecordID IS NULL) OR (CAST(RecordID AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS CNT,
    Year_CD_NXTMATDT AS Year_CD_NXTMATDT
  
  FROM Formula_280_0 AS in0
  
  GROUP BY Year_CD_NXTMATDT

),

AppendFields_282 AS (

  SELECT 
    in1.Year_CD_NXTMATDT AS Year_CD_NXTMATDT,
    in1.Sum_CD_VALUE AS Sum_CD_VALUE,
    in1.CNT AS CNT,
    in0.FilePath AS FilePath
  
  FROM Formula_220_to_Formula_286_2 AS in0
  INNER JOIN Summarize_279 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_282
