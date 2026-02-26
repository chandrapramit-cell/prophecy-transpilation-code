{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_97 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_97') }}

),

Formula_99_to_Formula_183_0 AS (

  SELECT 
    CAST(pPeriod AS INTEGER) AS nTargetPeriod,
    *
  
  FROM DynamicInput_97 AS in0

),

Formula_99_to_Formula_183_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (v_Period0 <= nTargetPeriod)
          THEN v_BalOut0
        ELSE 0
      END
    ) AS DOUBLE) AS nBalOut0,
    CAST((
      CASE
        WHEN (v_Period1 <= nTargetPeriod)
          THEN v_BalOut1
        ELSE 0
      END
    ) AS DOUBLE) AS nBalOut1,
    CAST((
      CASE
        WHEN (v_Period2 <= nTargetPeriod)
          THEN v_BalOut2
        ELSE 0
      END
    ) AS DOUBLE) AS nBalOut2,
    CAST((
      CASE
        WHEN (v_Period3 <= nTargetPeriod)
          THEN v_BalOut3
        ELSE 0
      END
    ) AS DOUBLE) AS nBalOut3,
    *
  
  FROM Formula_99_to_Formula_183_0 AS in0

),

Formula_99_to_Formula_183_2 AS (

  SELECT 
    CAST((((nBalOut0 + nBalOut1) + nBalOut2) + nBalOut3) AS DOUBLE) AS vROLValue,
    CAST(v_PolBranch AS STRING) AS v_Branch,
    *
  
  FROM Formula_99_to_Formula_183_1 AS in0

)

SELECT *

FROM Formula_99_to_Formula_183_2
