{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH PCMH_csv_1671 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'PCMH_csv_1671_ref') }}

),

AlteryxSelect_795 AS (

  SELECT 
    CAST(INDV_BE_KEY AS STRING) AS INDV_BE_KEY,
    * EXCEPT (`MBR_UNIQ_KEY`, `INDV_BE_KEY`)
  
  FROM PCMH_csv_1671 AS in0

),

Unique_794 AS (

  SELECT * 
  
  FROM AlteryxSelect_795 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY INDV_BE_KEY, 
  PROV_SK, 
  MED_HOME_GRP_DESC, 
  MED_HOME_LOC_DESC, 
  PCMH_ATTRIBUTED, 
  ATTRIBUTED, 
  PROV_NM, 
  PROV_REL_GRP_PROV_NM, 
  LATEST_PCP_VISIT_DT, 
  MAX_ROW_EFF_DT ORDER BY INDV_BE_KEY, PROV_SK, MED_HOME_GRP_DESC, MED_HOME_LOC_DESC, PCMH_ATTRIBUTED, ATTRIBUTED, PROV_NM, PROV_REL_GRP_PROV_NM, LATEST_PCP_VISIT_DT, MAX_ROW_EFF_DT) = 1

),

Sample_796 AS (

  {{ prophecy_basics.Sample('Unique_794', ['INDV_BE_KEY'], 1002, 'firstN', 1) }}

),

Join_792_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_792_left_UnionLeftOuter')}}

),

Join_798_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.INDV_BE_KEY)
          THEN NULL
        ELSE in0.MED_HOME_LOC_DESC
      END
    ) AS MED_HOME_LOC_DESC,
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.INDV_BE_KEY)
          THEN NULL
        ELSE in0.MED_HOME_GRP_DESC
      END
    ) AS MED_HOME_GRP_DESC,
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.INDV_BE_KEY)
          THEN NULL
        ELSE in0.PROV_REL_GRP_PROV_NM
      END
    ) AS PROV_REL_GRP_PROV_NM,
    in0.* EXCEPT (`MED_HOME_GRP_DESC`, `MED_HOME_LOC_DESC`, `PROV_REL_GRP_PROV_NM`),
    in1.* EXCEPT (`INDV_BE_KEY`, `MED_HOME_LOC_DESC`, `MED_HOME_GRP_DESC`, `PROV_REL_GRP_PROV_NM`)
  
  FROM Join_792_left_UnionLeftOuter AS in0
  LEFT JOIN Sample_796 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.INDV_BE_KEY)

)

SELECT *

FROM Join_798_left_UnionLeftOuter
