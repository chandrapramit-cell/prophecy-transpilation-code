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

Join_798_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_798_left_UnionLeftOuter')}}

),

AlteryxSelect_791 AS (

  SELECT 
    MBR_INDV_BE_KEY AS `Member Individual Business Entity Key`,
    MATCHLEVEL AS MATCHLEVEL,
    `Person 1: Ethnic - Country Of Origin` AS `Person 1: Ethnic - Country Of Origin`,
    `Person 1: Ethnic - Ethnic` AS `Person 1: Ethnic - Ethnic`,
    `Person 1: Ethnic - Group` AS `Person 1: Ethnic - Group`,
    `Person 1: Ethnic - Religion` AS `Person 1: Ethnic - Religion`,
    `Person 1: Ethnic - Language Preference` AS `Person 1: Ethnic - Language Preference`,
    `Person 1: Ethnic - Assimilation` AS `Person 1: Ethnic - Assimilation`
  
  FROM ENROLLMENT_EXPE_1672 AS in0

),

Sample_807 AS (

  {{
    prophecy_basics.Sample(
      ['AlteryxSelect_791'], 
      '[{"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "MATCHLEVEL", "dataType": "String"}, {"name": "Person 1: Ethnic - Country Of Origin", "dataType": "String"}, {"name": "Person 1: Ethnic - Ethnic", "dataType": "String"}, {"name": "Person 1: Ethnic - Group", "dataType": "String"}, {"name": "Person 1: Ethnic - Religion", "dataType": "String"}, {"name": "Person 1: Ethnic - Language Preference", "dataType": "String"}, {"name": "Person 1: Ethnic - Assimilation", "dataType": "String"}]', 
      'sampleGroup', 
      ['Member Individual Business Entity Key'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Join_800_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Join_798_left_UnionLeftOuter AS in0
  LEFT JOIN Sample_807 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

)

SELECT *

FROM Join_800_left_UnionLeftOuter
