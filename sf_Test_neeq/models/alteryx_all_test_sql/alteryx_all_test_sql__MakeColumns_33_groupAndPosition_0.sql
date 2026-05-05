{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_31_cast AS (

  SELECT *
  
  FROM {{ ref('alteryx_all_test_sql__TextInput_31_cast')}}

),

MakeColumns_33_sequenceNumber AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY '1' ASC NULLS FIRST) AS _SEQUENCE_ID
  
  FROM TextInput_31_cast AS in0

),

MakeColumns_33_window AS (

  SELECT 
    *,
    COUNT(*) OVER (ORDER BY '1' ASC NULLS FIRST) AS _TOTAL_RECORDS
  
  FROM MakeColumns_33_sequenceNumber AS in0

),

MakeColumns_33_groupAndPosition_0 AS (

  SELECT 
    (MOD((_SEQUENCE_ID - 1), CEIL((_TOTAL_RECORDS / 4)))) AS _GROUP_ID,
    (CONCAT('Column', '_', (FLOOR(((_SEQUENCE_ID - 1) / CEIL((_TOTAL_RECORDS / 4)))) + 1))) AS _POSITION_ID,
    *
  
  FROM MakeColumns_33_window AS in0

)

SELECT *

FROM MakeColumns_33_groupAndPosition_0
