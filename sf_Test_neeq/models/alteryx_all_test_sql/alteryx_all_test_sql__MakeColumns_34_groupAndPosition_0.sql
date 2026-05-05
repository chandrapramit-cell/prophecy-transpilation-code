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

MakeColumns_34_sequenceNumber AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY "STORE NUMBER" ORDER BY '1' ASC NULLS FIRST) AS _SEQUENCE_ID
  
  FROM TextInput_31_cast AS in0

),

MakeColumns_34_window AS (

  SELECT 
    *,
    COUNT(*) OVER (PARTITION BY "STORE NUMBER" ORDER BY '1' ASC NULLS FIRST) AS _TOTAL_RECORDS
  
  FROM MakeColumns_34_sequenceNumber AS in0

),

MakeColumns_34_groupAndPosition_0 AS (

  SELECT 
    FLOOR(((_SEQUENCE_ID - 1) / 5)) AS _GROUP_ID,
    (CONCAT('Column', '_', ((MOD((_SEQUENCE_ID - 1), 5)) + 1))) AS _POSITION_ID,
    *
  
  FROM MakeColumns_34_window AS in0

)

SELECT *

FROM MakeColumns_34_groupAndPosition_0
