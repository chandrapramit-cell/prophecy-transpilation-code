{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_814_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_814_0')}}

),

Join_834_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_834_left_UnionLeftOuter')}}

),

Join_815_inner AS (

  SELECT 
    in0.AVG_12_MONTH_BIN_PRED AS AVG_12_MONTH_BIN_PRED,
    in0.MBR_RELSHP_CD AS MBR_RELSHP_CD,
    in1.`Inflected Indicator CY` AS `Right_Inflected Indicator CY`,
    in0.`Person 1colon Ethnic - Country Of Origin` AS `Person 1colon Ethnic - Country Of Origin`,
    in0.`Person 1colon Ethnic - Assimilation` AS `Person 1colon Ethnic - Assimilation`,
    in0.Other_Chronic_Conditions AS Other_Chronic_Conditions,
    in0.VULNERABILITY_INDEX AS VULNERABILITY_INDEX,
    in0.READMITPREDICTION AS READMITPREDICTION,
    in0.est_hh_income AS est_hh_income,
    in0.Non_Chronic_Condition AS Non_Chronic_Condition,
    in0.MED_HOME_LOC_DESC AS MED_HOME_LOC_DESC,
    in0.MBR_HOME_ADDR_ZIP_CD_5 AS MBR_HOME_ADDR_ZIP_CD_5,
    in0.MBR_UNIQ_KEY AS MBR_UNIQ_KEY,
    in0.CAD AS CAD,
    in0.MBR_HOME_ADDR_CITY_NM AS MBR_HOME_ADDR_CITY_NM,
    in0.AVG_6_MONTH_BIN AS AVG_6_MONTH_BIN,
    in0.Diabetes AS Diabetes,
    in1.`Inflected Indicator Last 60 Days` AS `Right_Inflected Indicator Last 60 Days`,
    in0.MBR_GNDR_CD AS MBR_GNDR_CD,
    in0.MBR_HOME_ADDR_PHN_NO AS MBR_HOME_ADDR_PHN_NO,
    in0.est_hh_income_range_low AS est_hh_income_range_low,
    in0.TOTAL_ALLOWED_AMT AS TOTAL_ALLOWED_AMT,
    in0.TOTAL_AS_SCORE AS TOTAL_AS_SCORE,
    in0.TOTAL_DX_SCORE AS TOTAL_DX_SCORE,
    in1.AVG_6_MONTH_BIN AS Right_AVG_6_MONTH_BIN,
    in0.ED_DSCHG_DT AS ED_DSCHG_DT,
    in0.SPIRA_CARE_IND AS SPIRA_CARE_IND,
    in1.`MARA INDICATOR` AS `Right_MARA INDICATOR`,
    in0.`Inflected Indicator Pred` AS `Inflected Indicator Pred`,
    in0.`_Null_` AS _Null_,
    in0.DOB AS DOB,
    in1.V AS Right_V,
    in1.`Inflection Index CY` AS `Right_Inflection Index CY`,
    in0.Population AS Population,
    in0.YearMonthDay_ AS YearMonthDay_,
    in0.PROD_SH_NM AS PROD_SH_NM,
    in0.`Inflected Indicator CY` AS `Inflected Indicator CY`,
    in0.Asthma AS Asthma,
    in1.`Inflected Indicator Last 90 Days` AS `Right_Inflected Indicator Last 90 Days`,
    in0.Breathing_Conditions AS Breathing_Conditions,
    in0.PRNT_GRP_SIC_NACIS_NM AS PRNT_GRP_SIC_NACIS_NM,
    in0.`Person 1colon Ethnic - Group` AS `Person 1colon Ethnic - Group`,
    in0.COPD AS COPD,
    in1.`Inflected Indicator Pred` AS `Right_Inflected Indicator Pred`,
    in0.Transplant AS Transplant,
    in0.MAX_ROW_EFF_DT AS MAX_ROW_EFF_DT,
    in0.PCMH_ATTRIBUTED AS PCMH_ATTRIBUTED,
    in0.Digestive_System_Disorders AS Digestive_System_Disorders,
    in0.EDPREDICTION AS EDPREDICTION,
    in0.Unknown AS Unknown,
    in0.AdvancedCKD_Risk AS AdvancedCKD_Risk,
    in1.Avg_6_w_2monthRunout AS Right_Avg_6_w_2monthRunout,
    in0.`Previous CKD Risk` AS `Previous CKD Risk`,
    in0.Hypertension AS Hypertension,
    in0.`ED Prediction Value` AS `ED Prediction Value`,
    in0.MBR_HOME_ADDR_CNTY_NM AS MBR_HOME_ADDR_CNTY_NM,
    in0.AGE_RANGE AS AGE_RANGE,
    in0.`Inflected Indicator CM` AS `Inflected Indicator CM`,
    in0.MBR_HOME_ADDR_LN_1 AS MBR_HOME_ADDR_LN_1,
    in0.`Inflected Indicator Last 60 Days` AS `Inflected Indicator Last 60 Days`,
    in0.Avg_6_w_2monthRunout AS Avg_6_w_2monthRunout,
    in0.V AS V,
    in0.`Inflected Indicator Last 90 Days` AS `Inflected Indicator Last 90 Days`,
    in0.Mental_Health AS Mental_Health,
    in0.MBR_HOME_ADDR_LN_2 AS MBR_HOME_ADDR_LN_2,
    in0.`Comprehensive Risk Score` AS `Comprehensive Risk Score`,
    in1.AVG_12_MONTH_BIN_PRED AS Right_AVG_12_MONTH_BIN_PRED,
    in0.PRNT_GRP_SIC_NACIS_CD AS PRNT_GRP_SIC_NACIS_CD,
    in0.Cancer AS Cancer,
    in0.CELL_PHN_NO AS CELL_PHN_NO,
    in0.GRP_ID AS GRP_ID,
    in0.ER_COUNT_PAST_3_MONTHS AS ER_COUNT_PAST_3_MONTHS,
    in0.TOTAL_TX_SCORE AS TOTAL_TX_SCORE,
    in0.Developmental_Delays__ADHD__Autism AS Developmental_Delays__ADHD__Autism,
    in1.YearMonthDay_ AS Right_YearMonthDay_,
    in0.MBR_AGE AS MBR_AGE,
    in0.SUB_MKTNG_METRO_RURAL_CD AS SUB_MKTNG_METRO_RURAL_CD,
    in0.SPIRA_BNF_ID AS SPIRA_BNF_ID,
    in0.ER_COUNT AS ER_COUNT,
    in0.PROV_NM AS PROV_NM,
    in0.ADMITTING_DX AS ADMITTING_DX,
    in0.MBR_HOME_ADDR_ST_CD AS MBR_HOME_ADDR_ST_CD,
    in0.est_hh_income_range_high AS est_hh_income_range_high,
    in0.`Person 1colon Ethnic - Ethnic` AS `Person 1colon Ethnic - Ethnic`,
    in0.Dialysis_Flag AS Dialysis_Flag,
    in0.`Prediction Value` AS `Prediction Value`,
    in0.LATEST_PCP_VISIT_DT AS LATEST_PCP_VISIT_DT,
    in0.ATTRIBUTED AS ATTRIBUTED,
    in0.Avg_TotalRVU AS Avg_TotalRVU,
    in0.`ED Prediction Score` AS `ED Prediction Score`,
    in0.MATCHLEVEL AS MATCHLEVEL,
    in0.LATEST_ED_DX AS LATEST_ED_DX,
    in0.SUB_CNTGS_CNTY_CD AS SUB_CNTGS_CNTY_CD,
    in0.est_hh_income_range AS est_hh_income_range,
    in1.`Inflected Indicator CM` AS `Right_Inflected Indicator CM`,
    in0.ER_COUNT_PAST_2_MONTHS AS ER_COUNT_PAST_2_MONTHS,
    in0.Heart_and_Blood_Vessel_Conditions AS Heart_and_Blood_Vessel_Conditions,
    in0.READMIN_DSCHG_DT AS READMIN_DSCHG_DT,
    in0.`Person 1colon Ethnic - Religion` AS `Person 1colon Ethnic - Religion`,
    in0.NONEMERGENT_COUNT_PAST_60 AS NONEMERGENT_COUNT_PAST_60,
    in0.`Person 1colon Ethnic - Language Preference` AS `Person 1colon Ethnic - Language Preference`,
    in0.Renal_Failure__Chronic__ESRD AS Renal_Failure__Chronic__ESRD,
    in0.PROV_SK AS PROV_SK,
    in0.`Inflection Index Pred` AS `Inflection Index Pred`,
    in0.`Likelihood Factor` AS `Likelihood Factor`,
    in0.GRP_NM AS GRP_NM,
    in0.`Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    in0.`Prediction Score` AS `Prediction Score`,
    in1.`Inflection Index Pred` AS `Right_Inflection Index Pred`,
    in0.MED_HOME_GRP_DESC AS MED_HOME_GRP_DESC,
    in0.PROV_REL_GRP_PROV_NM AS PROV_REL_GRP_PROV_NM,
    in0.NONEMERGENT_RATE AS NONEMERGENT_RATE,
    in0.CHF AS CHF,
    in0.`Inflection Index CY` AS `Inflection Index CY`,
    in0.`MARA INDICATOR` AS `MARA INDICATOR`,
    in0.Pulmonary_Hypertension AS Pulmonary_Hypertension,
    in0.Muscle__Bone___Joint_Conditions AS Muscle__Bone___Joint_Conditions,
    in0.GRP_FUNDING_ARRANGEMENT AS GRP_FUNDING_ARRANGEMENT,
    in0.UNIQUE_DRUG_CT_CURRENT AS UNIQUE_DRUG_CT_CURRENT,
    in1.`Member Individual Business Entity Key` AS `Right_Member Individual Business Entity Key`,
    in0.PERSISTANT_HIGH_RISK AS PERSISTANT_HIGH_RISK,
    in0.MBR_FULL_NM AS MBR_FULL_NM
  
  FROM Join_834_left_UnionLeftOuter AS in0
  INNER JOIN Formula_814_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

)

SELECT *

FROM Join_815_inner
