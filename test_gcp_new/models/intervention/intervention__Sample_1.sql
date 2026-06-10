{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH ENROLLMENT_EXPE_1672 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'ENROLLMENT_EXPE_1672') }}

),

Sample_1 AS (

  {{
    prophecy_basics.Sample(
      ['ENROLLMENT_EXPE_1672'], 
      '[{"name": "DOB", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "First Name", "dataType": "String"}, {"name": "MATCHLEVEL", "dataType": "String"}, {"name": "Last Name", "dataType": "String"}, {"name": "Physical Address", "dataType": "String"}, {"name": "ZIP Code", "dataType": "String"}, {"name": "Person 1: Ethnic - Religion", "dataType": "String"}, {"name": "City", "dataType": "String"}, {"name": "Person 1: Ethnic - Assimilation", "dataType": "String"}, {"name": "Person 1: Ethnic - Country Of Origin", "dataType": "String"}, {"name": "Person 1: Ethnic - Group", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Person 1: Ethnic - Ethnic", "dataType": "String"}, {"name": "Person 1: Ethnic - Language Preference", "dataType": "String"}, {"name": "MBR_UNIQ_KEY", "dataType": "String"}]', 
      'sampleGroup', 
      ['First Name'], 
      1002, 
      'firstN', 
      80, 
      []
    )
  }}

)

SELECT *

FROM Sample_1
