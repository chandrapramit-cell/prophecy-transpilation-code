{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
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
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MBR_INDV_BE_KEY
    
    FROM AlteryxSelect_860 AS in1
    
    WHERE in1.MBR_INDV_BE_KEY IS NOT NULL
  ) AS in1_keys
     ON (in0.`Member Individual Business Entity Key` = in1_keys.MBR_INDV_BE_KEY)
  
  WHERE (in1_keys.MBR_INDV_BE_KEY IS NULL)

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
        '[{"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "CAD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Double"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Cancer", "dataType": "Double"}, {"name": "Transplant", "dataType": "String"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "String"}, {"name": "Other_Chronic_Conditions", "dataType": "String"}, {"name": "Non_Chronic_Condition", "dataType": "String"}, {"name": "Pulmonary_Hypertension", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "Digestive_System_Disorders", "dataType": "String"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "CHF", "dataType": "Double"}, {"name": "_Null_", "dataType": "Double"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "String"}, {"name": "Diabetes", "dataType": "Double"}, {"name": "COPD", "dataType": "Double"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "Breathing_Conditions", "dataType": "String"}, {"name": "Asthma", "dataType": "Double"}, {"name": "Unknown", "dataType": "String"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "String"}, {"name": "Mental_Health", "dataType": "String"}, {"name": "SOURCE_ID", "dataType": "String"}]', 
        '[{"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "CAD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Double"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Cancer", "dataType": "Double"}, {"name": "Transplant", "dataType": "String"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "String"}, {"name": "Other_Chronic_Conditions", "dataType": "String"}, {"name": "Non_Chronic_Condition", "dataType": "String"}, {"name": "Pulmonary_Hypertension", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "Digestive_System_Disorders", "dataType": "String"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "TOTAL_ALLOWED_AMT", "dataType": "String"}, {"name": "CHF", "dataType": "Double"}, {"name": "_Null_", "dataType": "Double"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "String"}, {"name": "Diabetes", "dataType": "Double"}, {"name": "COPD", "dataType": "Double"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "Breathing_Conditions", "dataType": "String"}, {"name": "Asthma", "dataType": "Double"}, {"name": "Unknown", "dataType": "String"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "String"}, {"name": "Mental_Health", "dataType": "String"}, {"name": "SOURCE_ID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_866
