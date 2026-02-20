{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_838 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_838')}}

),

Past_120_Rx_cou_1678 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'Past_120_Rx_cou_1678') }}

),

AlteryxSelect_887 AS (

  SELECT 
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    CAST(Unique_Rx_Past_120 AS FLOAT64) AS UNIQUE_DRUG_CT_CURRENT_NEW
  
  FROM Past_120_Rx_cou_1678 AS in0

),

Join_886_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM AlteryxSelect_838 AS in0
  LEFT JOIN AlteryxSelect_887 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.MBR_INDV_BE_KEY)

),

Unique_891_before AS (

  SELECT 
    `Person 1colon Ethnic - Country Of Origin` AS `Person 1colon Ethnic __ Country Of Origin`,
    `Person 1colon Ethnic - Ethnic` AS `Person 1colon Ethnic __ Ethnic`,
    `Person 1colon Ethnic - Group` AS `Person 1colon Ethnic __ Group`,
    `Person 1colon Ethnic - Religion` AS `Person 1colon Ethnic __ Religion`,
    `Person 1colon Ethnic - Language Preference` AS `Person 1colon Ethnic __ Language Preference`,
    `Person 1colon Ethnic - Assimilation` AS `Person 1colon Ethnic __ Assimilation`,
    * EXCEPT (`Person 1colon Ethnic - Country Of Origin`, 
    `Person 1colon Ethnic - Ethnic`, 
    `Person 1colon Ethnic - Group`, 
    `Person 1colon Ethnic - Religion`, 
    `Person 1colon Ethnic - Language Preference`, 
    `Person 1colon Ethnic - Assimilation`)
  
  FROM Join_886_left_UnionLeftOuter AS in0

),

Unique_891 AS (

  SELECT * 
  
  FROM Unique_891_before AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Member Individual Business Entity Key`, 
  TOTAL_ALLOWED_AMT, 
  READMIN_DSCHG_DT, 
  `Prediction Value`, 
  `Prediction Score`, 
  ADMITTING_DX, 
  UNIQUE_DRUG_CT_CURRENT, 
  READMITPREDICTION, 
  NONEMERGENT_RATE, 
  NONEMERGENT_COUNT_PAST_60, 
  ER_COUNT_PAST_2_MONTHS, 
  ER_COUNT_PAST_3_MONTHS, 
  ED_DSCHG_DT, 
  ER_COUNT, 
  LATEST_ED_DX, 
  `ED Prediction Score`, 
  `ED Prediction Value`, 
  EDPREDICTION, 
  MBR_UNIQ_KEY, 
  MBR_FULL_NM, 
  PROD_SH_NM, 
  MBR_GNDR_CD, 
  MBR_AGE, 
  AGE_RANGE, 
  MBR_RELSHP_CD, 
  GRP_FUNDING_ARRANGEMENT, 
  GRP_ID, 
  GRP_NM, 
  PRNT_GRP_SIC_NACIS_CD, 
  PRNT_GRP_SIC_NACIS_NM, 
  SUB_CNTGS_CNTY_CD, 
  SUB_MKTNG_METRO_RURAL_CD, 
  MBR_HOME_ADDR_LN_1, 
  MBR_HOME_ADDR_LN_2, 
  MBR_HOME_ADDR_CITY_NM, 
  MBR_HOME_ADDR_ST_CD, 
  MBR_HOME_ADDR_ZIP_CD_5, 
  MBR_HOME_ADDR_CNTY_NM, 
  SPIRA_BNF_ID, 
  SPIRA_CARE_IND, 
  MBR_HOME_ADDR_PHN_NO, 
  CELL_PHN_NO, 
  TOTAL_DX_SCORE, 
  TOTAL_TX_SCORE, 
  TOTAL_AS_SCORE, 
  VULNERABILITY_INDEX, 
  DOB, 
  PROV_SK, 
  MED_HOME_GRP_DESC, 
  MED_HOME_LOC_DESC, 
  PCMH_ATTRIBUTED, 
  ATTRIBUTED, 
  PROV_NM, 
  PROV_REL_GRP_PROV_NM, 
  LATEST_PCP_VISIT_DT, 
  MAX_ROW_EFF_DT, 
  MATCHLEVEL, 
  `Person 1colon Ethnic __ Country Of Origin`, 
  `Person 1colon Ethnic __ Ethnic`, 
  `Person 1colon Ethnic __ Group`, 
  `Person 1colon Ethnic __ Religion`, 
  `Person 1colon Ethnic __ Language Preference`, 
  `Person 1colon Ethnic __ Assimilation`, 
  est_hh_income, 
  est_hh_income_range, 
  est_hh_income_range_low, 
  est_hh_income_range_high, 
  AdvancedCKD_Risk, 
  Dialysis_Flag, 
  `Previous CKD Risk`, 
  YearMonthDay_, 
  V, 
  Avg_TotalRVU, 
  `Inflection Index CY`, 
  `Inflected Indicator CY`, 
  Avg_6_w_2monthRunout, 
  AVG_6_MONTH_BIN, 
  `Inflected Indicator CM`, 
  `Inflected Indicator Last 90 Days`, 
  `Inflected Indicator Last 60 Days`, 
  AVG_12_MONTH_BIN_PRED, 
  `Inflection Index Pred`, 
  `Inflected Indicator Pred`, 
  `MARA INDICATOR`, 
  PERSISTANT_HIGH_RISK, 
  Right_AVG_12_MONTH_BIN_PRED, 
  `Right_Inflection Index Pred`, 
  `Right_Inflected Indicator Pred`, 
  `Risk Cost Code`, 
  `Concurrent Normalized Risk Score`, 
  `Prospective Normalized Risk Score`, 
  `Recent 3 Months Total Allowable Amount`, 
  `Current 12 Months Total Allowable Amount`, 
  SOURCE_ID, 
  MBR_INDV_BE_KEY, 
  MBR_ID, 
  MIN_RELATIONSHIP_EFF_DT, 
  UNIQUE_DRUG_CT_CURRENT_NEW ORDER BY `Member Individual Business Entity Key`, TOTAL_ALLOWED_AMT, READMIN_DSCHG_DT, `Prediction Value`, `Prediction Score`, ADMITTING_DX, UNIQUE_DRUG_CT_CURRENT, READMITPREDICTION, NONEMERGENT_RATE, NONEMERGENT_COUNT_PAST_60, ER_COUNT_PAST_2_MONTHS, ER_COUNT_PAST_3_MONTHS, ED_DSCHG_DT, ER_COUNT, LATEST_ED_DX, `ED Prediction Score`, `ED Prediction Value`, EDPREDICTION, MBR_UNIQ_KEY, MBR_FULL_NM, PROD_SH_NM, MBR_GNDR_CD, MBR_AGE, AGE_RANGE, MBR_RELSHP_CD, GRP_FUNDING_ARRANGEMENT, GRP_ID, GRP_NM, PRNT_GRP_SIC_NACIS_CD, PRNT_GRP_SIC_NACIS_NM, SUB_CNTGS_CNTY_CD, SUB_MKTNG_METRO_RURAL_CD, MBR_HOME_ADDR_LN_1, MBR_HOME_ADDR_LN_2, MBR_HOME_ADDR_CITY_NM, MBR_HOME_ADDR_ST_CD, MBR_HOME_ADDR_ZIP_CD_5, MBR_HOME_ADDR_CNTY_NM, SPIRA_BNF_ID, SPIRA_CARE_IND, MBR_HOME_ADDR_PHN_NO, CELL_PHN_NO, TOTAL_DX_SCORE, TOTAL_TX_SCORE, TOTAL_AS_SCORE, VULNERABILITY_INDEX, DOB, PROV_SK, MED_HOME_GRP_DESC, MED_HOME_LOC_DESC, PCMH_ATTRIBUTED, ATTRIBUTED, PROV_NM, PROV_REL_GRP_PROV_NM, LATEST_PCP_VISIT_DT, MAX_ROW_EFF_DT, MATCHLEVEL, `Person 1colon Ethnic __ Country Of Origin`, `Person 1colon Ethnic __ Ethnic`, `Person 1colon Ethnic __ Group`, `Person 1colon Ethnic __ Religion`, `Person 1colon Ethnic __ Language Preference`, `Person 1colon Ethnic __ Assimilation`, est_hh_income, est_hh_income_range, est_hh_income_range_low, est_hh_income_range_high, AdvancedCKD_Risk, Dialysis_Flag, `Previous CKD Risk`, YearMonthDay_, V, Avg_TotalRVU, `Inflection Index CY`, `Inflected Indicator CY`, Avg_6_w_2monthRunout, AVG_6_MONTH_BIN, `Inflected Indicator CM`, `Inflected Indicator Last 90 Days`, `Inflected Indicator Last 60 Days`, AVG_12_MONTH_BIN_PRED, `Inflection Index Pred`, `Inflected Indicator Pred`, `MARA INDICATOR`, PERSISTANT_HIGH_RISK, Right_AVG_12_MONTH_BIN_PRED, `Right_Inflection Index Pred`, `Right_Inflected Indicator Pred`, `Risk Cost Code`, `Concurrent Normalized Risk Score`, `Prospective Normalized Risk Score`, `Recent 3 Months Total Allowable Amount`, `Current 12 Months Total Allowable Amount`, SOURCE_ID, MBR_INDV_BE_KEY, MBR_ID, MIN_RELATIONSHIP_EFF_DT, UNIQUE_DRUG_CT_CURRENT_NEW) = 1

),

Unique_891_after AS (

  SELECT 
    `Person 1colon Ethnic __ Country Of Origin` AS `Person 1colon Ethnic - Country Of Origin`,
    `Person 1colon Ethnic __ Ethnic` AS `Person 1colon Ethnic - Ethnic`,
    `Person 1colon Ethnic __ Group` AS `Person 1colon Ethnic - Group`,
    `Person 1colon Ethnic __ Religion` AS `Person 1colon Ethnic - Religion`,
    `Person 1colon Ethnic __ Language Preference` AS `Person 1colon Ethnic - Language Preference`,
    `Person 1colon Ethnic __ Assimilation` AS `Person 1colon Ethnic - Assimilation`,
    * EXCEPT (`Person 1colon Ethnic __ Country Of Origin`, 
    `Person 1colon Ethnic __ Ethnic`, 
    `Person 1colon Ethnic __ Group`, 
    `Person 1colon Ethnic __ Religion`, 
    `Person 1colon Ethnic __ Language Preference`, 
    `Person 1colon Ethnic __ Assimilation`)
  
  FROM Unique_891 AS in0

),

Cleanse_892 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Unique_891_after'], 
      [
        { "name": "DOB", "dataType": "String" }, 
        { "name": "Prospective Normalized Risk Score", "dataType": "String" }, 
        { "name": "Dialysis_Flag", "dataType": "String" }, 
        { "name": "Current 12 Months Total Allowable Amount", "dataType": "String" }, 
        { "name": "MBR_INDV_BE_KEY", "dataType": "String" }, 
        { "name": "Concurrent Normalized Risk Score", "dataType": "String" }, 
        { "name": "TOTAL_DX_SCORE", "dataType": "String" }, 
        { "name": "TOTAL_AS_SCORE", "dataType": "String" }, 
        { "name": "est_hh_income", "dataType": "String" }, 
        { "name": "CAD", "dataType": "Double" }, 
        { "name": "MED_HOME_GRP_DESC", "dataType": "String" }, 
        { "name": "Inflected Indicator CY", "dataType": "String" }, 
        { "name": "Hypertension", "dataType": "Double" }, 
        { "name": "ED Prediction Value", "dataType": "Double" }, 
        { "name": "MATCHLEVEL", "dataType": "String" }, 
        { "name": "AVG_6_MONTH_BIN", "dataType": "String" }, 
        { "name": "ESRD", "dataType": "Double" }, 
        { "name": "AdvancedCKD_Risk", "dataType": "String" }, 
        { "name": "est_hh_income_range_low", "dataType": "String" }, 
        { "name": "LATEST_ED_DX", "dataType": "String" }, 
        { "name": "Comprehensive Risk Score", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_PHN_NO", "dataType": "String" }, 
        { "name": "Prediction Score", "dataType": "String" }, 
        { "name": "Cancer", "dataType": "Double" }, 
        { "name": "MBR_HOME_ADDR_CITY_NM", "dataType": "String" }, 
        { "name": "Right_Inflection Index Pred", "dataType": "String" }, 
        { "name": "MAX_ROW_EFF_DT", "dataType": "String" }, 
        { "name": "Transplant", "dataType": "String" }, 
        { "name": "SUB_CNTGS_CNTY_CD", "dataType": "String" }, 
        { "name": "Renal_Failure__Chronic__ESRD", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_2", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Country Of Origin", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String" }, 
        { "name": "Other_Chronic_Conditions", "dataType": "String" }, 
        { "name": "Inflected Indicator Last 60 Days", "dataType": "String" }, 
        { "name": "GRP_FUNDING_ARRANGEMENT", "dataType": "String" }, 
        { "name": "Prediction Value", "dataType": "Double" }, 
        { "name": "READMITPREDICTION", "dataType": "Double" }, 
        { "name": "Non_Chronic_Condition", "dataType": "String" }, 
        { "name": "MED_HOME_LOC_DESC", "dataType": "String" }, 
        { "name": "Pulmonary_Hypertension", "dataType": "String" }, 
        { "name": "Right_Inflected Indicator Pred", "dataType": "String" }, 
        { "name": "ER_COUNT", "dataType": "Double" }, 
        { "name": "SUB_MKTNG_METRO_RURAL_CD", "dataType": "String" }, 
        { "name": "ER_COUNT_PAST_3_MONTHS", "dataType": "Double" }, 
        { "name": "AVG_12_MONTH_BIN_PRED", "dataType": "String" }, 
        { "name": "Member Individual Business Entity Key", "dataType": "String" }, 
        { "name": "Digestive_System_Disorders", "dataType": "String" }, 
        { "name": "Inflection Index Pred", "dataType": "String" }, 
        { "name": "GRP_NM", "dataType": "String" }, 
        { "name": "GRP_ID", "dataType": "String" }, 
        { "name": "Developmental_Delays__ADHD__Autism", "dataType": "String" }, 
        { "name": "Population", "dataType": "String" }, 
        { "name": "Right_MBR_INDV_BE_KEY", "dataType": "String" }, 
        { "name": "Right_AVG_12_MONTH_BIN_PRED", "dataType": "String" }, 
        { "name": "TOTAL_TX_SCORE", "dataType": "String" }, 
        { "name": "Avg_TotalRVU", "dataType": "String" }, 
        { "name": "TOTAL_ALLOWED_AMT", "dataType": "String" }, 
        { "name": "Avg_6_w_2monthRunout", "dataType": "Double" }, 
        { "name": "READMIN_DSCHG_DT", "dataType": "Date" }, 
        { "name": "PCMH_ATTRIBUTED", "dataType": "String" }, 
        { "name": "Inflection Index CY", "dataType": "String" }, 
        { "name": "V", "dataType": "String" }, 
        { "name": "CHF", "dataType": "Double" }, 
        { "name": "MBR_HOME_ADDR_CNTY_NM", "dataType": "String" }, 
        { "name": "Previous CKD Risk", "dataType": "String" }, 
        { "name": "PROD_SH_NM", "dataType": "String" }, 
        { "name": "ATTRIBUTED", "dataType": "String" }, 
        { "name": "MBR_FULL_NM", "dataType": "String" }, 
        { "name": "_Null_", "dataType": "Double" }, 
        { "name": "UNIQUE_DRUG_CT_CURRENT_NEW", "dataType": "Double" }, 
        { "name": "Inflected Indicator CM", "dataType": "String" }, 
        { "name": "Inflected Indicator Last 90 Days", "dataType": "String" }, 
        { "name": "Muscle__Bone___Joint_Conditions", "dataType": "String" }, 
        { "name": "PROV_REL_GRP_PROV_NM", "dataType": "String" }, 
        { "name": "PERSISTANT_HIGH_RISK", "dataType": "String" }, 
        { "name": "ADMITTING_DX", "dataType": "String" }, 
        { "name": "Diabetes", "dataType": "Double" }, 
        { "name": "Likelihood Factor", "dataType": "String" }, 
        { "name": "COPD", "dataType": "Double" }, 
        { "name": "SPIRA_BNF_ID", "dataType": "String" }, 
        { "name": "Recent 3 Months Total Allowable Amount", "dataType": "String" }, 
        { "name": "PROV_NM", "dataType": "String" }, 
        { "name": "AGE_RANGE", "dataType": "String" }, 
        { "name": "MBR_GNDR_CD", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Religion", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_1", "dataType": "String" }, 
        { "name": "NONEMERGENT_COUNT_PAST_60", "dataType": "Double" }, 
        { "name": "ED_DSCHG_DT", "dataType": "String" }, 
        { "name": "LATEST_PCP_VISIT_DT", "dataType": "String" }, 
        { "name": "CELL_PHN_NO", "dataType": "String" }, 
        { "name": "Inflected Indicator Pred", "dataType": "String" }, 
        { "name": "NONEMERGENT_RATE", "dataType": "Double" }, 
        { "name": "SPIRA_CARE_IND", "dataType": "String" }, 
        { "name": "est_hh_income_range_high", "dataType": "String" }, 
        { "name": "VULNERABILITY_INDEX", "dataType": "String" }, 
        { "name": "Risk Cost Code", "dataType": "String" }, 
        { "name": "ED Prediction Score", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Language Preference", "dataType": "String" }, 
        { "name": "Breathing_Conditions", "dataType": "String" }, 
        { "name": "MBR_ID", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Assimilation", "dataType": "String" }, 
        { "name": "est_hh_income_range", "dataType": "String" }, 
        { "name": "YearMonthDay_", "dataType": "String" }, 
        { "name": "UNIQUE_DRUG_CT_CURRENT", "dataType": "Double" }, 
        { "name": "MBR_RELSHP_CD", "dataType": "String" }, 
        { "name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String" }, 
        { "name": "Asthma", "dataType": "Double" }, 
        { "name": "ER_COUNT_PAST_2_MONTHS", "dataType": "Double" }, 
        { "name": "Unknown", "dataType": "String" }, 
        { "name": "Heart_and_Blood_Vessel_Conditions", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Group", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ST_CD", "dataType": "String" }, 
        { "name": "MBR_AGE", "dataType": "String" }, 
        { "name": "Mental_Health", "dataType": "String" }, 
        { "name": "PRNT_GRP_SIC_NACIS_NM", "dataType": "String" }, 
        { "name": "Person 1colon Ethnic - Ethnic", "dataType": "String" }, 
        { "name": "MARA INDICATOR", "dataType": "Double" }, 
        { "name": "SOURCE_ID", "dataType": "String" }, 
        { "name": "MIN_RELATIONSHIP_EFF_DT", "dataType": "String" }, 
        { "name": "EDPREDICTION", "dataType": "Double" }, 
        { "name": "MBR_UNIQ_KEY", "dataType": "String" }, 
        { "name": "PROV_SK", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['UNIQUE_DRUG_CT_CURRENT', 'UNIQUE_DRUG_CT_CURRENT_NEW'], 
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

Formula_889_0 AS (

  SELECT 
    (
      CASE
        WHEN ((UNIQUE_DRUG_CT_CURRENT = 0) AND (UNIQUE_DRUG_CT_CURRENT_NEW > 0))
          THEN UNIQUE_DRUG_CT_CURRENT_NEW
        ELSE UNIQUE_DRUG_CT_CURRENT
      END
    ) AS UNIQUE_DRUG_CT_CURRENT,
    * EXCEPT (`unique_drug_ct_current`)
  
  FROM Cleanse_892 AS in0

),

AlteryxSelect_890 AS (

  SELECT * EXCEPT (`UNIQUE_DRUG_CT_CURRENT_NEW`)
  
  FROM Formula_889_0 AS in0

)

SELECT *

FROM AlteryxSelect_890
