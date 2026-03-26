{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_22 AS (

  {#VisualGroup: GI#}
  SELECT * 
  
  FROM {{ ref('seed_22')}}

),

TextInput_22_cast AS (

  {#VisualGroup: GI#}
  SELECT 
    CAST(LIST AS string) AS LIST,
    CAST(VALUE AS BOOLEAN) AS `VALUE`
  
  FROM TextInput_22 AS in0

),

AlteryxSelect_122 AS (

  {#VisualGroup: GI#}
  SELECT 
    CAST(VALUE AS INT) AS `VALUE`,
    * EXCEPT (`VALUE`)
  
  FROM TextInput_22_cast AS in0

),

CrossTab_24 AS (

  {#VisualGroup: GI#}
  SELECT *
  
  FROM (
    SELECT 
      LIST,
      VALUE
    
    FROM AlteryxSelect_122 AS in0
  )
  PIVOT (
    SUM(VALUE) AS Sum
    FOR LIST
    IN (
      'Cancellation_letter', 
      'Unmatched_Mismatched_cash', 
      'Client_is_in_administration_liquidation', 
      'Uncategorised', 
      'Hold_cover', 
      'Debt_part_paid', 
      'Claims_exist', 
      'Debt_Timetable'
    )
  )

)

SELECT *

FROM CrossTab_24
