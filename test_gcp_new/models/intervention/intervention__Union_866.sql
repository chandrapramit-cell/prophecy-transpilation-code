{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Unique_1018 AS (

  SELECT *
  
  FROM {{ ref('intervention__Unique_1018')}}

),

AlteryxSelect_860 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_860')}}

),

Join_864_left AS (

  SELECT in0.*
  
  FROM Unique_1018 AS in0
  ANTI JOIN AlteryxSelect_860 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

),

Join_864_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_864_inner')}}

),

Union_866 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_864_left', 'Join_864_inner'], 
      [
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "_Null_", "dataType": "Integer"}, {"name": "Asthma", "dataType": "Integer"}, {"name": "CAD", "dataType": "Integer"}, {"name": "Cancer", "dataType": "Integer"}, {"name": "CHF", "dataType": "Integer"}, {"name": "COPD", "dataType": "Integer"}, {"name": "Diabetes", "dataType": "Integer"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Integer"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "_Null_", "dataType": "Integer"}, {"name": "Asthma", "dataType": "Integer"}, {"name": "CAD", "dataType": "Integer"}, {"name": "Cancer", "dataType": "Integer"}, {"name": "CHF", "dataType": "Integer"}, {"name": "COPD", "dataType": "Integer"}, {"name": "Diabetes", "dataType": "Integer"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Integer"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "TOTAL_ALLOWED_AMT", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_866
