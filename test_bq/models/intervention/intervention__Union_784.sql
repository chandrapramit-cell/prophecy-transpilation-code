{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_776_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_776_left_UnionFullOuter')}}

),

Formula_782_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_782_0')}}

),

Join_783_right AS (

  SELECT in0.*
  
  FROM Formula_782_0 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Join_776_left_UnionFullOuter AS in1
  ) AS in1_keys
     ON (in1_keys.`Member Individual Business Entity Key` = in0.`Member Individual Business Entity Key`)

),

Filter_787 AS (

  SELECT * 
  
  FROM Join_783_right AS in0
  
  WHERE (
          (
            (CAST(`ED Prediction Score` AS STRING) = 'CRITICAL')
            OR (CAST(`ED Prediction Score` AS STRING) = 'HIGH')
          )
          OR (CAST(`ED Prediction Score` AS STRING) = 'MODERATE')
        )

),

Join_783_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Join_776_left_UnionFullOuter AS in0
  LEFT JOIN Formula_782_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

),

Union_784 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_787', 'Join_783_left_UnionLeftOuter'], 
      [
        '[{"name": "ED Prediction Value", "dataType": "Double"}, {"name": "LATEST_ED_DX", "dataType": "String"}, {"name": "ER_COUNT", "dataType": "Double"}, {"name": "ER_COUNT_PAST_3_MONTHS", "dataType": "Double"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "NONEMERGENT_COUNT_PAST_60", "dataType": "Double"}, {"name": "ED_DSCHG_DT", "dataType": "String"}, {"name": "NONEMERGENT_RATE", "dataType": "Double"}, {"name": "ED Prediction Score", "dataType": "String"}, {"name": "ER_COUNT_PAST_2_MONTHS", "dataType": "Double"}, {"name": "EDPREDICTION", "dataType": "Double"}]', 
        '[{"name": "Prospective Normalized Risk Score", "dataType": "String"}, {"name": "Current 12 Months Total Allowable Amount", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "Concurrent Normalized Risk Score", "dataType": "String"}, {"name": "CAD", "dataType": "Double"}, {"name": "Hypertension", "dataType": "Double"}, {"name": "ED Prediction Value", "dataType": "Double"}, {"name": "ESRD", "dataType": "Double"}, {"name": "LATEST_ED_DX", "dataType": "String"}, {"name": "Prediction Score", "dataType": "String"}, {"name": "Cancer", "dataType": "Double"}, {"name": "Transplant", "dataType": "String"}, {"name": "Renal_Failure__Chronic__ESRD", "dataType": "String"}, {"name": "Other_Chronic_Conditions", "dataType": "String"}, {"name": "Prediction Value", "dataType": "Double"}, {"name": "READMITPREDICTION", "dataType": "Double"}, {"name": "Non_Chronic_Condition", "dataType": "String"}, {"name": "Pulmonary_Hypertension", "dataType": "String"}, {"name": "ER_COUNT", "dataType": "Double"}, {"name": "ER_COUNT_PAST_3_MONTHS", "dataType": "Double"}, {"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "Digestive_System_Disorders", "dataType": "String"}, {"name": "Developmental_Delays__ADHD__Autism", "dataType": "String"}, {"name": "Right_MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "TOTAL_ALLOWED_AMT", "dataType": "String"}, {"name": "READMIN_DSCHG_DT", "dataType": "Date"}, {"name": "CHF", "dataType": "Double"}, {"name": "_Null_", "dataType": "Double"}, {"name": "Muscle__Bone___Joint_Conditions", "dataType": "String"}, {"name": "ADMITTING_DX", "dataType": "String"}, {"name": "Diabetes", "dataType": "Double"}, {"name": "COPD", "dataType": "Double"}, {"name": "Recent 3 Months Total Allowable Amount", "dataType": "String"}, {"name": "NONEMERGENT_COUNT_PAST_60", "dataType": "Double"}, {"name": "ED_DSCHG_DT", "dataType": "String"}, {"name": "NONEMERGENT_RATE", "dataType": "Double"}, {"name": "Risk Cost Code", "dataType": "String"}, {"name": "ED Prediction Score", "dataType": "String"}, {"name": "Breathing_Conditions", "dataType": "String"}, {"name": "UNIQUE_DRUG_CT_CURRENT", "dataType": "Double"}, {"name": "Asthma", "dataType": "Double"}, {"name": "ER_COUNT_PAST_2_MONTHS", "dataType": "Double"}, {"name": "Unknown", "dataType": "String"}, {"name": "Heart_and_Blood_Vessel_Conditions", "dataType": "String"}, {"name": "Mental_Health", "dataType": "String"}, {"name": "SOURCE_ID", "dataType": "String"}, {"name": "EDPREDICTION", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_784
