{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_66 AS (

  {#VisualGroup: RI#}
  SELECT * 
  
  FROM {{ ref('seed_66')}}

),

TextInput_66_cast AS (

  {#VisualGroup: RI#}
  SELECT 
    CAST(LIST AS string) AS LIST,
    CAST(VALUE AS BOOLEAN) AS `VALUE`
  
  FROM TextInput_66 AS in0

),

AlteryxSelect_123 AS (

  {#VisualGroup: RI#}
  SELECT 
    CAST(VALUE AS INT) AS `VALUE`,
    * EXCEPT (`VALUE`)
  
  FROM TextInput_66_cast AS in0

),

CrossTab_67 AS (

  {#VisualGroup: RI#}
  SELECT *
  
  FROM (
    SELECT 
      LIST,
      VALUE
    
    FROM AlteryxSelect_123 AS in0
  )
  PIVOT (
    SUM(VALUE) AS Sum
    FOR LIST
    IN (
      'Waiting_support_documentation_from_Claims', 
      'To_be_reconciled_at_the_end_of_the_claim', 
      'Supporting_documentation_saved_received', 
      'Fees__To_be_collected_at_the_end_of_the_claim', 
      'Incorrect_claim_registration', 
      'Debit_Note_issued_and_sent_to_Broker', 
      'Correction', 
      'Waiting_further_advice_from_Claims'
    )
  )

)

SELECT *

FROM CrossTab_67
