{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_864_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_864_inner')}}

),

Unique_1018 AS (

  SELECT *
  
  FROM {{ ref('intervention__Unique_1018')}}

),

AlteryxSelect_860 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_860')}}

),

Join_864_left AS (

  {#VisualGroup: STEP1#}
  SELECT in0.*
  
  FROM Unique_1018 AS in0
  ANTI JOIN AlteryxSelect_860 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

),

Union_866 AS (

  {#VisualGroup: STEP1#}
  {{
    prophecy_basics.UnionByName(
      ['Join_864_left', 'Join_864_inner'], 
      [
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "_Null_", "dataType": "Integer"}, {"name": "Asthma", "dataType": "Integer"}, {"name": "CAD", "dataType": "Integer"}, {"name": "Cancer", "dataType": "Integer"}, {"name": "CHF", "dataType": "Integer"}, {"name": "COPD", "dataType": "Integer"}, {"name": "Diabetes", "dataType": "Integer"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Integer"}, {"name": "Breathing_Conditions", "dataType": "Integer"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "Integer"}, {"name": "Digestive_System_Disorders", "dataType": "Integer"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "Integer"}, {"name": "Mental_Health", "dataType": "Integer"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "Integer"}, {"name": "Non_Chronic_Condition", "dataType": "Integer"}, {"name": "Other_Chronic_Conditions", "dataType": "Integer"}, {"name": "Pulmonary_Hypertension", "dataType": "Integer"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "Integer"}, {"name": "Transplant", "dataType": "Integer"}, {"name": "Unknown", "dataType": "Integer"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "_Null_", "dataType": "Integer"}, {"name": "Asthma", "dataType": "Integer"}, {"name": "CAD", "dataType": "Integer"}, {"name": "Cancer", "dataType": "Integer"}, {"name": "CHF", "dataType": "Integer"}, {"name": "COPD", "dataType": "Integer"}, {"name": "Diabetes", "dataType": "Integer"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Integer"}, {"name": "Breathing_Conditions", "dataType": "Integer"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "Integer"}, {"name": "Digestive_System_Disorders", "dataType": "Integer"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "Integer"}, {"name": "Mental_Health", "dataType": "Integer"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "Integer"}, {"name": "Non_Chronic_Condition", "dataType": "Integer"}, {"name": "Other_Chronic_Conditions", "dataType": "Integer"}, {"name": "Pulmonary_Hypertension", "dataType": "Integer"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "Integer"}, {"name": "Transplant", "dataType": "Integer"}, {"name": "Unknown", "dataType": "Integer"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "TOTAL_ALLOWED_AMT", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_866
