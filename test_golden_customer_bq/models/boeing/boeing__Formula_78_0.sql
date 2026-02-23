{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('boeing__AlteryxSelect_25')}}

),

Database__REPOR_2 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_2_ref') }}

),

Formula_80_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(LEG_SC_DPTR_DATE, MONTH)))) AS MO,
    *
  
  FROM Database__REPOR_2 AS in0

),

Summarize_73 AS (

  SELECT 
    SUM(REV) AS Sum_REV,
    MO AS MO
  
  FROM Formula_80_0 AS in0
  
  GROUP BY MO

),

Formula_81_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(DPTR_DT, MONTH)))) AS MO,
    *
  
  FROM AlteryxSelect_25 AS in0

),

Summarize_74 AS (

  SELECT 
    SUM(SPILL_REV) AS Sum_SPILL_REV,
    MO AS MO
  
  FROM Formula_81_0 AS in0
  
  GROUP BY MO

),

Join_75_inner AS (

  SELECT 
    in0.Sum_REV AS Sum_REV,
    in1.Sum_SPILL_REV AS Sum_SPILL_REV,
    in1.* EXCEPT (`Sum_SPILL_REV`)
  
  FROM Summarize_73 AS in0
  INNER JOIN Summarize_74 AS in1
     ON (in0.MO = in1.MO)

),

Formula_78_0 AS (

  SELECT 
    (Sum_SPILL_REV / Sum_REV) AS percent,
    *
  
  FROM Join_75_inner AS in0

)

SELECT *

FROM Formula_78_0
