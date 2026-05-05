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

MakeColumns_32_sequenceNumber AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY '1' ASC NULLS FIRST) AS _SEQUENCE_ID
  
  FROM TextInput_31_cast AS in0

),

MakeColumns_32_window AS (

  SELECT 
    *,
    COUNT(*) OVER (ORDER BY '1' ASC NULLS FIRST) AS _TOTAL_RECORDS
  
  FROM MakeColumns_32_sequenceNumber AS in0

),

MakeColumns_32_groupAndPosition_0 AS (

  SELECT 
    FLOOR(((_SEQUENCE_ID - 1) / 3)) AS _GROUP_ID,
    (CONCAT('Column', '_', ((MOD((_SEQUENCE_ID - 1), 3)) + 1))) AS _POSITION_ID,
    *
  
  FROM MakeColumns_32_window AS in0

)

SELECT *

FROM MakeColumns_32_groupAndPosition_0
