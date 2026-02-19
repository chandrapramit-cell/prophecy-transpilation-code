{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Summarize_173 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Summarize_173')}}

),

Filter_175 AS (

  SELECT * 
  
  FROM Summarize_173 AS in0
  
  WHERE (CountDistinct_YearMonth > CAST('11' AS FLOAT64))

),

TOTAL_INF_predi_171 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'TOTAL_INF_predi_171_ref') }}

),

Join_176_inner AS (

  SELECT 
    in1.YearMonth AS YearMonth,
    in0.* EXCEPT (`CountDistinct_YearMonth`)
  
  FROM Filter_175 AS in0
  INNER JOIN TOTAL_INF_predi_171 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

),

Filter_180 AS (

  SELECT * 
  
  FROM Join_176_inner AS in0
  
  WHERE (CAST(YearMonth AS INT64) >= 202301)

),

AlteryxSelect_170 AS (

  SELECT 
    CAST(`Member Individual Business Entity Key` AS FLOAT64) AS MBR_INDV_BE_KEY,
    NULL AS TotalRVU,
    NULL AS V,
    NULL AS VPP,
    NULL AS ACC,
    NULL AS RunTot_Inflection,
    NULL AS Avg_TotalRVU,
    NULL AS Avg_TotalRVU_Full,
    NULL AS Avg_TotalRVU_Pred,
    * EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Filter_180 AS in0

),

claim_cache_csv_191 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'claim_cache_csv_191_ref') }}

),

Summarize_121 AS (

  SELECT 
    COUNT(DISTINCT CLM_ID) AS CountDistinct_CLM_ID,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM claim_cache_csv_191 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

AlteryxSelect_122 AS (

  SELECT 
    CAST(MBR_INDV_BE_KEY AS FLOAT64) AS MBR_INDV_BE_KEY,
    * EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM Summarize_121 AS in0

),

active_members__172 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'active_members__172_ref') }}

),

AlteryxSelect_178 AS (

  SELECT 
    CAST(MBR_INDV_BE_KEY AS FLOAT64) AS MBR_INDV_BE_KEY,
    * EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM active_members__172 AS in0

),

Join_177_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_RELSHP_CD
      END
    ) AS MBR_RELSHP_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.`32CNTY_FLAG`
      END
    ) AS `32CNTY_FLAG`,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.FNCL_LOB_DESC
      END
    ) AS FNCL_LOB_DESC,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SUB_FLAG
      END
    ) AS SUB_FLAG,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_ZIP_CD_5
      END
    ) AS MBR_HOME_ADDR_ZIP_CD_5,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_UNIQ_KEY
      END
    ) AS MBR_UNIQ_KEY,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.FNCL_MKT_SEG_NM
      END
    ) AS FNCL_MKT_SEG_NM,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.CLS_ID
      END
    ) AS CLS_ID,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_CITY_NM
      END
    ) AS MBR_HOME_ADDR_CITY_NM,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_GNDR_CD
      END
    ) AS MBR_GNDR_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_CELL_PHN_NO
      END
    ) AS MBR_CELL_PHN_NO,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_PHN_NO
      END
    ) AS MBR_HOME_ADDR_PHN_NO,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_EMAIL_ADDR
      END
    ) AS MBR_EMAIL_ADDR,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_ID
      END
    ) AS MBR_ID,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MAX_ENR_EFF_DT
      END
    ) AS MAX_ENR_EFF_DT,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.UNRELIABLE_HOME_ADDR_FLAG
      END
    ) AS UNRELIABLE_HOME_ADDR_FLAG,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.DOB
      END
    ) AS DOB,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.POPULATION_TYPE
      END
    ) AS POPULATION_TYPE,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.PROD_SH_NM
      END
    ) AS PROD_SH_NM,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MAX_ENR_TERM_DT
      END
    ) AS MAX_ENR_TERM_DT,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.DIRECT_PAY_IN
      END
    ) AS DIRECT_PAY_IN,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.TOTAL_POLICY_MEMBERS
      END
    ) AS TOTAL_POLICY_MEMBERS,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.PROD_SK
      END
    ) AS PROD_SK,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.GRP_INDUSTRY_ROLLUP
      END
    ) AS GRP_INDUSTRY_ROLLUP,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_CNTY_NM
      END
    ) AS MBR_HOME_ADDR_CNTY_NM,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.RURAL_FLAG
      END
    ) AS RURAL_FLAG,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_LN_1
      END
    ) AS MBR_HOME_ADDR_LN_1,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.PROD_SH_NM_DLVRY_METH_CD
      END
    ) AS PROD_SH_NM_DLVRY_METH_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SUB_OR_DPNDT_UNDER_18_FLAG
      END
    ) AS SUB_OR_DPNDT_UNDER_18_FLAG,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_LN_2
      END
    ) AS MBR_HOME_ADDR_LN_2,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MIN_RELATIONSHIP_EFF_DT
      END
    ) AS MIN_RELATIONSHIP_EFF_DT,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.CLS_PLN_DESC
      END
    ) AS CLS_PLN_DESC,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.PROD_SH_NM_CAT_CD
      END
    ) AS PROD_SH_NM_CAT_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.EXPRNC_CAT_CD
      END
    ) AS EXPRNC_CAT_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SUB_ID
      END
    ) AS SUB_ID,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_AGE
      END
    ) AS MBR_AGE,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.FUND_CAT_CD
      END
    ) AS FUND_CAT_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SUB_MKTNG_METRO_RURAL_CD
      END
    ) AS SUB_MKTNG_METRO_RURAL_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SPIRA_BNF_ID
      END
    ) AS SPIRA_BNF_ID,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_INDV_BE_KEY
      END
    ) AS MBR_INDV_BE_KEY,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_HOME_ADDR_ST_CD
      END
    ) AS MBR_HOME_ADDR_ST_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.GRP_INDUSTRY
      END
    ) AS GRP_INDUSTRY,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.FNCL_LOB_CD
      END
    ) AS FNCL_LOB_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SPIRA_FLAG
      END
    ) AS SPIRA_FLAG,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.SUB_CNTGS_CNTY_CD
      END
    ) AS SUB_CNTGS_CNTY_CD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.GRP_NM
      END
    ) AS GRP_NM,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.TOTAL_LIVING_IN_HOUSEHOLD
      END
    ) AS TOTAL_LIVING_IN_HOUSEHOLD,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.FUND_CAT_DESC
      END
    ) AS FUND_CAT_DESC,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_FULL_NM
      END
    ) AS MBR_FULL_NM,
    in0.* EXCEPT (`MBR_INDV_BE_KEY`),
    in1.* EXCEPT (`32CNTY_FLAG`, 
    `MBR_CELL_PHN_NO`, 
    `CLS_ID`, 
    `CLS_PLN_DESC`, 
    `DOB`, 
    `EXPRNC_CAT_CD`, 
    `FNCL_LOB_CD`, 
    `FNCL_LOB_DESC`, 
    `FNCL_MKT_SEG_NM`, 
    `FUND_CAT_CD`, 
    `FUND_CAT_DESC`, 
    `GRP_ID`, 
    `GRP_NM`, 
    `GRP_INDUSTRY`, 
    `GRP_INDUSTRY_ROLLUP`, 
    `MAX_ENR_EFF_DT`, 
    `MAX_ENR_TERM_DT`, 
    `MBR_AGE`, 
    `MBR_EMAIL_ADDR`, 
    `MBR_FULL_NM`, 
    `MBR_GNDR_CD`, 
    `MBR_HOME_ADDR_CITY_NM`, 
    `MBR_HOME_ADDR_CNTY_NM`, 
    `MBR_HOME_ADDR_LN_1`, 
    `MBR_HOME_ADDR_LN_2`, 
    `MBR_HOME_ADDR_PHN_NO`, 
    `MBR_HOME_ADDR_ST_CD`, 
    `MBR_HOME_ADDR_ZIP_CD_5`, 
    `MBR_ID`, 
    `MBR_INDV_BE_KEY`, 
    `MBR_RELSHP_CD`, 
    `MBR_UNIQ_KEY`, 
    `POPULATION_TYPE`, 
    `PROD_SH_NM`, 
    `PROD_SH_NM_CAT_CD`, 
    `PROD_SH_NM_DLVRY_METH_CD`, 
    `PROD_SK`, 
    `RURAL_FLAG`, 
    `SPIRA_BNF_ID`, 
    `SPIRA_FLAG`, 
    `SUB_OR_DPNDT_UNDER_18_FLAG`, 
    `SUB_CNTGS_CNTY_CD`, 
    `SUB_FLAG`, 
    `SUB_ID`, 
    `SUB_MKTNG_METRO_RURAL_CD`, 
    `TOTAL_LIVING_IN_HOUSEHOLD`, 
    `TOTAL_POLICY_MEMBERS`, 
    `UNRELIABLE_HOME_ADDR_FLAG`, 
    `DIRECT_PAY_IN`, 
    `MIN_RELATIONSHIP_EFF_DT`)
  
  FROM AlteryxSelect_170 AS in0
  RIGHT JOIN AlteryxSelect_178 AS in1
     ON (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)

),

Join_107_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM Join_177_right_UnionRightOuter AS in0
  LEFT JOIN AlteryxSelect_122 AS in1
     ON (t.MBR_INDV_BE_KEY0 = in1.MBR_INDV_BE_KEY)

),

Formula_147_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (MBR_AGE_RANGE = '0-4')
          THEN '1. 0-4  '
        WHEN (MBR_AGE_RANGE = '5-17')
          THEN '2. 5-17 '
        WHEN (MBR_AGE_RANGE = '18-29')
          THEN '3. 18-29'
        WHEN (MBR_AGE_RANGE = '30-39')
          THEN '4. 30-39'
        WHEN (MBR_AGE_RANGE = '40-49')
          THEN '5. 40-49'
        WHEN (MBR_AGE_RANGE = '50-64')
          THEN '6. 50-64'
        WHEN (MBR_AGE_RANGE = '65-74')
          THEN '7. 65-74'
        WHEN (MBR_AGE_RANGE = '75-84')
          THEN '8. 75-84'
        ELSE '9. 85+  '
      END
    ) AS STRING) AS MBR_AGE_RANGE,
    * EXCEPT (`mbr_age_range`)
  
  FROM Join_107_left_UnionLeftOuter AS in0

),

Cleanse_133 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_147_0'], 
      [
        { "name": "DOB", "dataType": "String" }, 
        { "name": "MBR_INDV_BE_KEY", "dataType": "String" }, 
        { "name": "SUB_ID", "dataType": "String" }, 
        { "name": "32CNTY_FLAG", "dataType": "String" }, 
        { "name": "YearMonth", "dataType": "String" }, 
        { "name": "RURAL_FLAG", "dataType": "String" }, 
        { "name": "MBR_EMAIL_ADDR", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_PHN_NO", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_CITY_NM", "dataType": "String" }, 
        { "name": "MAX_ENR_TERM_DT", "dataType": "String" }, 
        { "name": "Avg_TotalRVU_Pred", "dataType": "Double" }, 
        { "name": "SUB_CNTGS_CNTY_CD", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_2", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String" }, 
        { "name": "UNRELIABLE_HOME_ADDR_FLAG", "dataType": "String" }, 
        { "name": "SUB_OR_DPNDT_UNDER_18_FLAG", "dataType": "String" }, 
        { "name": "PROD_SH_NM_CAT_CD", "dataType": "String" }, 
        { "name": "SUB_MKTNG_METRO_RURAL_CD", "dataType": "String" }, 
        { "name": "MBR_CELL_PHN_NO", "dataType": "String" }, 
        { "name": "GRP_NM", "dataType": "String" }, 
        { "name": "GRP_ID", "dataType": "String" }, 
        { "name": "Avg_TotalRVU", "dataType": "Double" }, 
        { "name": "EXPRNC_CAT_CD", "dataType": "String" }, 
        { "name": "FUND_CAT_CD", "dataType": "String" }, 
        { "name": "GRP_INDUSTRY", "dataType": "String" }, 
        { "name": "MAX_ENR_EFF_DT", "dataType": "String" }, 
        { "name": "TotalRVU", "dataType": "Double" }, 
        { "name": "CLS_PLN_DESC", "dataType": "String" }, 
        { "name": "TOTAL_LIVING_IN_HOUSEHOLD", "dataType": "String" }, 
        { "name": "PROD_SH_NM_DLVRY_METH_CD", "dataType": "String" }, 
        { "name": "SPIRA_FLAG", "dataType": "String" }, 
        { "name": "PROD_SK", "dataType": "String" }, 
        { "name": "VPP", "dataType": "Double" }, 
        { "name": "V", "dataType": "Double" }, 
        { "name": "MBR_HOME_ADDR_CNTY_NM", "dataType": "String" }, 
        { "name": "PROD_SH_NM", "dataType": "String" }, 
        { "name": "MBR_FULL_NM", "dataType": "String" }, 
        { "name": "FNCL_LOB_DESC", "dataType": "String" }, 
        { "name": "CLS_ID", "dataType": "String" }, 
        { "name": "SPIRA_BNF_ID", "dataType": "String" }, 
        { "name": "MBR_GNDR_CD", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_1", "dataType": "String" }, 
        { "name": "FUND_CAT_DESC", "dataType": "String" }, 
        { "name": "MBR_AGE_RANGE", "dataType": "String" }, 
        { "name": "SUB_FLAG", "dataType": "String" }, 
        { "name": "POPULATION_TYPE", "dataType": "String" }, 
        { "name": "GRP_INDUSTRY_ROLLUP", "dataType": "String" }, 
        { "name": "Avg_TotalRVU_Full", "dataType": "Double" }, 
        { "name": "MBR_ID", "dataType": "String" }, 
        { "name": "RunTot_Inflection", "dataType": "Integer" }, 
        { "name": "ACC", "dataType": "Double" }, 
        { "name": "MBR_RELSHP_CD", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ST_CD", "dataType": "String" }, 
        { "name": "MBR_AGE", "dataType": "String" }, 
        { "name": "CountDistinct_CLM_ID", "dataType": "Double" }, 
        { "name": "DIRECT_PAY_IN", "dataType": "String" }, 
        { "name": "FNCL_LOB_CD", "dataType": "String" }, 
        { "name": "FNCL_MKT_SEG_NM", "dataType": "String" }, 
        { "name": "TOTAL_POLICY_MEMBERS", "dataType": "String" }, 
        { "name": "MIN_RELATIONSHIP_EFF_DT", "dataType": "String" }, 
        { "name": "MBR_UNIQ_KEY", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'MBR_INDV_BE_KEY', 
        'YearMonth', 
        'TotalRVU', 
        'V', 
        'VPP', 
        'ACC', 
        'RunTot_Inflection', 
        'Avg_TotalRVU', 
        'Avg_TotalRVU_Full', 
        'Avg_TotalRVU_Pred', 
        'MBR_AGE_RANGE', 
        'CountDistinct_CLM_ID'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Sort_125 AS (

  SELECT * 
  
  FROM Cleanse_133 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC

)

SELECT *

FROM Sort_125
