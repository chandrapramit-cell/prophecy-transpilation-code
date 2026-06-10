{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Join_850_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_850_left_UnionLeftOuter')}}

),

AlteryxSelect_856 AS (

  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    _Null_ AS _Null_,
    Asthma AS Asthma,
    CAD AS CAD,
    Cancer AS Cancer,
    CHF AS CHF,
    COPD AS COPD,
    Diabetes AS Diabetes,
    ESRD AS ESRD,
    Hypertension AS Hypertension
  
  FROM Join_850_left_UnionLeftOuter AS in0

),

MultiFieldFormula_772 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_856'], 
      "CASE WHEN (column_value > 0) THEN 1 ELSE 0 END", 
      [
        'Member Individual Business Entity Key', 
        '_Null_', 
        'Asthma', 
        'CAD', 
        'Cancer', 
        'CHF', 
        'COPD', 
        'Diabetes', 
        'ESRD', 
        'Hypertension'
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
