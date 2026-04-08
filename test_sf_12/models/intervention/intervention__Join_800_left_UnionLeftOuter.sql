{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
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

  {#VisualGroup: STEP1#}
  SELECT 
    MBR_INDV_BE_KEY AS `Member Individual Business Entity Key`,
    MATCHLEVEL AS MATCHLEVEL,
    `Person 1colon Ethnic - Country Of Origin` AS `Person 1colon Ethnic - Country Of Origin`,
    `Person 1colon Ethnic - Ethnic` AS `Person 1colon Ethnic - Ethnic`,
    `Person 1colon Ethnic - Group` AS `Person 1colon Ethnic - Group`,
    `Person 1colon Ethnic - Religion` AS `Person 1colon Ethnic - Religion`,
    `Person 1colon Ethnic - Language Preference` AS `Person 1colon Ethnic - Language Preference`,
    `Person 1colon Ethnic - Assimilation` AS `Person 1colon Ethnic - Assimilation`
  
  FROM ENROLLMENT_EXPE_1672 AS in0

),

Sample_807 AS (

  {#VisualGroup: STEP1#}
  {{
    prophecy_basics.Sample(
      'AlteryxSelect_791', 
      ['Member Individual Business Entity Key'], 
      1002, 
      'firstN', 
      1
    )
  }}

),

Join_800_left_UnionLeftOuter AS (

  {#VisualGroup: STEP1#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Join_798_left_UnionLeftOuter AS in0
  LEFT JOIN Sample_807 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

)

SELECT *

FROM Join_800_left_UnionLeftOuter
