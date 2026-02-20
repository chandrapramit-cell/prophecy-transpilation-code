{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_800_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_800_left_UnionLeftOuter')}}

),

experianMappedT_1673 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'experianMappedT_1673') }}

),

AlteryxSelect_804 AS (

  SELECT 
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    est_hh_income AS est_hh_income,
    est_hh_income_range AS est_hh_income_range,
    est_hh_income_range_low AS est_hh_income_range_low,
    est_hh_income_range_high AS est_hh_income_range_high
  
  FROM experianMappedT_1673 AS in0

),

Sample_808 AS (

  {{ prophecy_basics.Sample('AlteryxSelect_804', ['MBR_INDV_BE_KEY'], 1002, 'firstN', 1) }}

),

Join_805_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM Join_800_left_UnionLeftOuter AS in0
  LEFT JOIN Sample_808 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

)

SELECT *

FROM Join_805_left_UnionLeftOuter
