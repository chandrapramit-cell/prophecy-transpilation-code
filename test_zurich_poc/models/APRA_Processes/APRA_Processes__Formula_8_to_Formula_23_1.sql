{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_427 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_427') }}

),

Formula_8_to_Formula_23_0 AS (

  SELECT 
    CAST(pPeriod AS STRING) AS vPeriod,
    CAST('Actual' AS STRING) AS vVersion,
    CAST((
      CASE
        WHEN (v_TZAC = '')
          THEN 'Unallocated'
        ELSE v_TZAC
      END
    ) AS STRING) AS v_TZAC,
    CAST(v_PolBranch AS STRING) AS v_Branch,
    * EXCEPT (`v_tzac`)
  
  FROM DynamicInput_427 AS in0

),

Formula_8_to_Formula_23_1 AS (

  SELECT 
    CAST((SUBSTRING(vPeriod, 1, 4)) AS STRING) AS v_AccidentYear,
    CAST('1' AS STRING) AS v_LineNo,
    *
  
  FROM Formula_8_to_Formula_23_0 AS in0

)

SELECT *

FROM Formula_8_to_Formula_23_1
