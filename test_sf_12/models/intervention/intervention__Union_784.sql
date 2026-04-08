{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_782_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_782_0')}}

),

Join_776_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_776_left_UnionFullOuter')}}

),

Join_783_right AS (

  {#VisualGroup: STEP1#}
  SELECT in0.*
  
  FROM Formula_782_0 AS in0
  ANTI JOIN Join_776_left_UnionFullOuter AS in1
     ON (in1.`Member Individual Business Entity Key` = in0.`Member Individual Business Entity Key`)

),

Filter_787 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_783_right AS in0
  
  WHERE (CAST(`ED Prediction Score` AS string) IN ('CRITICAL', 'HIGH', 'MODERATE'))

),

Join_783_left_UnionLeftOuter AS (

  {#VisualGroup: STEP1#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Join_776_left_UnionFullOuter AS in0
  LEFT JOIN Formula_782_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

),

Union_784 AS (

  {#VisualGroup: STEP1#}
  {{
    prophecy_basics.UnionByName(
      ['Filter_787', 'Join_783_left_UnionLeftOuter'], 
      [
        '[{"name": "EDPREDICTION", "dataType": "Double"}, {"name": "NONEMERGENT_RATE", "dataType": "Double"}, {"name": "NONEMERGENT_COUNT_PAST_60", "dataType": "Double"}, {"name": "ER_COUNT_PAST_2_MONTHS", "dataType": "Double"}, {"name": "ER_COUNT_PAST_3_MONTHS", "dataType": "Double"}, {"name": "ED_DSCHG_DT", "dataType": "String"}, {"name": "ER_COUNT", "dataType": "Double"}, {"name": "LATEST_ED_DX", "dataType": "String"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "ED Prediction Score", "dataType": "String"}, {"name": "ED Prediction Value", "dataType": "Double"}]', 
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "_Null_", "dataType": "Integer"}, {"name": "Asthma", "dataType": "Integer"}, {"name": "CAD", "dataType": "Integer"}, {"name": "Cancer", "dataType": "Integer"}, {"name": "CHF", "dataType": "Integer"}, {"name": "COPD", "dataType": "Integer"}, {"name": "Diabetes", "dataType": "Integer"}, {"name": "ESRD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Integer"}, {"name": "Breathing_Conditions", "dataType": "Integer"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "Integer"}, {"name": "Digestive_System_Disorders", "dataType": "Integer"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "Integer"}, {"name": "Mental_Health", "dataType": "Integer"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "Integer"}, {"name": "Non_Chronic_Condition", "dataType": "Integer"}, {"name": "Other_Chronic_Conditions", "dataType": "Integer"}, {"name": "Pulmonary_Hypertension", "dataType": "Integer"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "Integer"}, {"name": "Transplant", "dataType": "Integer"}, {"name": "Unknown", "dataType": "Integer"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "TOTAL_ALLOWED_AMT", "dataType": "String"}, {"name": "READMITPREDICTION", "dataType": "Double"}, {"name": "READMIN_DSCHG_DT", "dataType": "Date"}, {"name": "Prediction Value", "dataType": "Double"}, {"name": "Prediction Score", "dataType": "String"}, {"name": "ADMITTING_DX", "dataType": "String"}, {"name": "UNIQUE_DRUG_CT_CURRENT", "dataType": "Double"}, {"name": "EDPREDICTION", "dataType": "Double"}, {"name": "NONEMERGENT_RATE", "dataType": "Double"}, {"name": "NONEMERGENT_COUNT_PAST_60", "dataType": "Double"}, {"name": "ER_COUNT_PAST_2_MONTHS", "dataType": "Double"}, {"name": "ER_COUNT_PAST_3_MONTHS", "dataType": "Double"}, {"name": "ED_DSCHG_DT", "dataType": "String"}, {"name": "ER_COUNT", "dataType": "Double"}, {"name": "LATEST_ED_DX", "dataType": "String"}, {"name": "ED Prediction Score", "dataType": "String"}, {"name": "ED Prediction Value", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_784
