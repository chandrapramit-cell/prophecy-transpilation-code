{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_850_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_850_left_UnionLeftOuter')}}

),

AlteryxSelect_856 AS (

  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    `_Null_` AS _Null_,
    Asthma AS Asthma,
    CAD AS CAD,
    Cancer AS Cancer,
    CHF AS CHF,
    COPD AS COPD,
    Diabetes AS Diabetes,
    ESRD AS ESRD,
    Hypertension AS Hypertension,
    NULL AS Breathing_Conditions,
    NULL AS Developmental_Delays__ADHD__Autism,
    NULL AS Digestive_System_Disorders,
    NULL AS Heart_and_Blood_Vessel_Conditions,
    NULL AS Mental_Health,
    NULL AS Muscle__Bone___Joint_Conditions,
    NULL AS Non_Chronic_Condition,
    NULL AS Other_Chronic_Conditions,
    NULL AS Pulmonary_Hypertension,
    NULL AS Renal_Failure__Chronic__ESRD,
    NULL AS Transplant,
    NULL AS Unknown
  
  FROM Join_850_left_UnionLeftOuter AS in0

),

MultiFieldFormula_772 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_856'], 
      "CASE WHEN (column_value > 0) THEN 1 ELSE 0 END", 
      [
        'CAD', 
        'Hypertension', 
        'ESRD', 
        'Cancer', 
        'Transplant', 
        'Renal_Failure__Chronic__ESRD', 
        'Other_Chronic_Conditions', 
        'Non_Chronic_Condition', 
        'Pulmonary_Hypertension', 
        'Member Individual Business Entity Key', 
        'Digestive_System_Disorders', 
        'Developmental_Delays__ADHD__Autism', 
        'CHF', 
        '_Null_', 
        'Muscle__Bone___Joint_Conditions', 
        'Diabetes', 
        'COPD', 
        'Breathing_Conditions', 
        'Asthma', 
        'Unknown', 
        'Heart_and_Blood_Vessel_Conditions', 
        'Mental_Health'
      ], 
      [
        '_Null_', 
        'Asthma', 
        'Breathing_Conditions', 
        'CAD', 
        'Cancer', 
        'CHF', 
        'COPD', 
        'Developmental_Delays__ADHD__Autism', 
        'Diabetes', 
        'Digestive_System_Disorders', 
        'Heart_and_Blood_Vessel_Conditions', 
        'Hypertension', 
        'Mental_Health', 
        'Muscle__Bone___Joint_Conditions', 
        'Non_Chronic_Condition', 
        'Other_Chronic_Conditions', 
        'Pulmonary_Hypertension', 
        'Renal_Failure__Chronic__ESRD', 
        'Transplant', 
        'Unknown'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

)

SELECT *

FROM MultiFieldFormula_772
