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

MakeColumns_37_sequenceNumber AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY "STORE NUMBER", UNITCOST ORDER BY '1' ASC NULLS FIRST) AS _SEQUENCE_ID
  
  FROM TextInput_31_cast AS in0

),

MakeColumns_37_window AS (

  SELECT 
    *,
    COUNT(*) OVER (PARTITION BY "STORE NUMBER", UNITCOST ORDER BY '1' ASC NULLS FIRST) AS _TOTAL_RECORDS
  
  FROM MakeColumns_37_sequenceNumber AS in0

),

MakeColumns_37_groupAndPosition_0 AS (

  SELECT 
    (MOD((_SEQUENCE_ID - 1), CEIL((_TOTAL_RECORDS / 8)))) AS _GROUP_ID,
    (CONCAT('Column', '_', (FLOOR(((_SEQUENCE_ID - 1) / CEIL((_TOTAL_RECORDS / 8)))) + 1))) AS _POSITION_ID,
    *
  
  FROM MakeColumns_37_window AS in0

)

SELECT *

FROM MakeColumns_37_groupAndPosition_0
