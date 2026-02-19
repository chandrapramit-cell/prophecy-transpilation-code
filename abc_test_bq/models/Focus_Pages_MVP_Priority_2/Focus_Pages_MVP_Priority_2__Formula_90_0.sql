{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_alxaa2_Quer_98 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_98_ref') }}

),

active_members__99 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'active_members__99_ref') }}

),

Join_100_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`32CNTY_FLAG`, 
    `MBR_AGE_RANGE`, 
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
    `MBR_RELSHP_CD`, 
    `MBR_UNIQ_KEY`, 
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
  
  FROM aka_alxaa2_Quer_98 AS in0
  INNER JOIN active_members__99 AS in1
     ON (in0.MBR_ID = in1.MBR_ID)

),

AlteryxSelect_91 AS (

  SELECT 
    MBR_ID AS MBR_ID,
    MBR_SK AS MBR_SK,
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(CLM_SVC_STRT_DT_SK AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(CLM_SVC_STRT_DT_SK AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(CLM_SVC_STRT_DT_SK AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(CLM_SVC_STRT_DT_SK AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(CLM_SVC_STRT_DT_SK AS STRING)) AS DATE)
      END
    ) AS CLM_SVC_STRT_DT_SK,
    SUB_ID AS SUB_ID,
    PREGNANCYDIAG AS PregnancyDiag,
    PREGNANCYDIAGDESC AS PregnancyDiagDesc,
    CLM_ID AS CLM_ID,
    SRC_SYS_CD AS SRC_SYS_CD,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM Join_100_inner AS in0

),

Sort_128 AS (

  SELECT * 
  
  FROM AlteryxSelect_91 AS in0
  
  ORDER BY MBR_ID ASC, CLM_SVC_STRT_DT_SK ASC

),

Filter_89 AS (

  SELECT * 
  
  FROM Sort_128 AS in0
  
  WHERE (MBR_SK <> CAST('0' AS INT64))

),

Formula_90_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (CONCAT((SUBSTRING(CAST(CLM_SVC_STRT_DT_SK AS STRING), 1, 7)), '-01')))) AS ServiceDate,
    *
  
  FROM Filter_89 AS in0

)

SELECT *

FROM Formula_90_0
