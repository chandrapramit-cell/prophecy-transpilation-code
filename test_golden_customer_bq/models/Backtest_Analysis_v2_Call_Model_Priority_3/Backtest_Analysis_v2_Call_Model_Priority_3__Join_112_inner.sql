{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH OverallSystemTe_111 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'OverallSystemTe_111_ref') }}

),

Filter_2 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Filter_2')}}

),

AlteryxSelect_18 AS (

  SELECT * EXCEPT (`PredictedLabel`)
  
  FROM Filter_2 AS in0

),

Join_112_inner AS (

  SELECT 
    in0.`Out of Network Dental Claims Paid` AS `Out of Network Dental Claims Paid`,
    in0.Male_Aged_65_74 AS Male_Aged_65_74,
    in0.Male_Aged_15_18 AS Male_Aged_15_18,
    in0.Male_Aged_25_33 AS Male_Aged_25_33,
    in0.Woman_Aged_0_4 AS Woman_Aged_0_4,
    in0.Woman_Aged_40_49 AS Woman_Aged_40_49,
    in0.`Right_PA Denial Count` AS `Right_PA Denial Count`,
    in0.`Right_Allowed Amount of Prior Auth Denial` AS `Right_Allowed Amount of Prior Auth Denial`,
    in0.MBR_HOME_ADDR_ZIP_CD_5 AS MBR_HOME_ADDR_ZIP_CD_5,
    in0.Male_Aged_19_24 AS Male_Aged_19_24,
    in0.`Max CCI Score` AS `Max CCI Score`,
    in0.`Right_Active Cases` AS `Right_Active Cases`,
    in0.`months since last call` AS `months since last call`,
    in0.`Days in Care Paid` AS `Days in Care Paid`,
    in0.`Out of Area Dental Claims Paid` AS `Out of Area Dental Claims Paid`,
    in0.`Days in Care Received` AS `Days in Care Received`,
    in0.GRP_DP_IN AS GRP_DP_IN,
    in0.ACA_SILVER_PLAN AS ACA_SILVER_PLAN,
    in0.`Days Not Covered Paid` AS `Days Not Covered Paid`,
    in0.Woman_Aged_65_74 AS Woman_Aged_65_74,
    in0.RunTot_Inflection AS RunTot_Inflection,
    in0.ACA_CATASTROPHIC_PLAN AS ACA_CATASTROPHIC_PLAN,
    in0.`OP Visits Paid` AS `OP Visits Paid`,
    in0.Male_Aged_40_49 AS Male_Aged_40_49,
    in0.`Days Covered Received` AS `Days Covered Received`,
    in0.ACA_BRONZE_PLAN AS ACA_BRONZE_PLAN,
    in0.`IP Visits Received` AS `IP Visits Received`,
    in0.Woman_Aged_25_33 AS Woman_Aged_25_33,
    in0.ACA_GOLD_PLAN AS ACA_GOLD_PLAN,
    in0.`Total Dental Member Pay Paid` AS `Total Dental Member Pay Paid`,
    in0.Woman_Aged_10_14 AS Woman_Aged_10_14,
    in0.YMD AS YMD,
    in0.MEDSUP AS MEDSUP,
    in0.Prediction AS Prediction,
    in0.PPO AS PPO,
    in0.`IP Visits Paid` AS `IP Visits Paid`,
    in0.`Number of Dental Claims Paid` AS `Number of Dental Claims Paid`,
    in0.`Right_Allowed Amount of Prior Auth Approval` AS `Right_Allowed Amount of Prior Auth Approval`,
    in0.`Number of Claims Received` AS `Number of Claims Received`,
    in0.Woman_Aged_19_24 AS Woman_Aged_19_24,
    in0.`CSAT Score` AS `CSAT Score`,
    in0.MA AS MA,
    in0.`PA Approved Count` AS `PA Approved Count`,
    in1.`Number of Calls` AS `Unweighted Number of Calls`,
    in0.Woman_Aged_74_ AS Woman_Aged_74_,
    in0.`Member Balance Paid` AS `Member Balance Paid`,
    in0.New_ACA_Member AS New_ACA_Member,
    in0.New_Retirement_Member AS New_Retirement_Member,
    in0.Woman_Aged_33_40 AS Woman_Aged_33_40,
    in0.TRAD AS TRAD,
    in0.CallDurationSeconds AS CallDurationSeconds,
    in0.`Out of Area Dental Claims Received` AS `Out of Area Dental Claims Received`,
    in0.`Months Until Call` AS `Months Until Call`,
    in0.`Allowed Amount of Prior Auth Approval` AS `Allowed Amount of Prior Auth Approval`,
    in0.Male_Aged_50_64 AS Male_Aged_50_64,
    in0.ID AS ID,
    in0.row_id AS row_id,
    in0.PRNT_GRP_SIC_NACIS_CD AS PRNT_GRP_SIC_NACIS_CD,
    in0.`Days in Care Not Covered Received` AS `Days in Care Not Covered Received`,
    in0.`Number of Calls` AS `Number of Calls`,
    in0.GRP_ID AS GRP_ID,
    in0.ACA AS ACA,
    in0.`Days Covered Paid` AS `Days Covered Paid`,
    in0.`Out of Network Claims Paid` AS `Out of Network Claims Paid`,
    in0.`Blue Connect Household` AS `Blue Connect Household`,
    in0.`Allowed Amount of Prior Auth Denial` AS `Allowed Amount of Prior Auth Denial`,
    in0.`Claim with Most Reversals Paid` AS `Claim with Most Reversals Paid`,
    in0.Product_Change AS Product_Change,
    in0.`Specialty Visits Paid` AS `Specialty Visits Paid`,
    in0.`Out of Network Dental Claims Received` AS `Out of Network Dental Claims Received`,
    in0.Terminated AS Terminated,
    in0.`Max_April Caller` AS `Max_April Caller`,
    in0.Re_Enroll AS Re_Enroll,
    in0.HMO AS HMO,
    in0.Woman_Aged_50_64 AS Woman_Aged_50_64,
    in0.`Most Complex Case` AS `Most Complex Case`,
    in0.grp_change AS grp_change,
    in0.Right_ID AS Right_ID,
    in0.PPO_BRONZE_PLAN AS PPO_BRONZE_PLAN,
    in0.TotalRVU AS TotalRVU,
    in0.`Active Cases` AS `Active Cases`,
    in0.`Dental Member Month` AS `Dental Member Month`,
    in0.SPIRA_BNF_ID AS SPIRA_BNF_ID,
    in0.Dependents AS Dependents,
    in0.PCP_FLAG AS PCP_FLAG,
    in0.Male_Aged_0_4 AS Male_Aged_0_4,
    in0.Male_Aged_5_9 AS Male_Aged_5_9,
    in0.`ED Visits Received` AS `ED Visits Received`,
    in0.MBR_HOME_ADDR_ST_CD AS MBR_HOME_ADDR_ST_CD,
    in0.`Total Member Pay Paid` AS `Total Member Pay Paid`,
    in0.MBR_DSBLTY_IN AS MBR_DSBLTY_IN,
    in0.`Called in 3` AS `Called in 3`,
    in0.`Specialty Visits Received` AS `Specialty Visits Received`,
    in0.Woman_Aged_5_9 AS Woman_Aged_5_9,
    in0.SUB_CNTGS_CNTY_CD AS SUB_CNTGS_CNTY_CD,
    in0.`OP Visits Received` AS `OP Visits Received`,
    in0.PPO_GOLD_PLAN AS PPO_GOLD_PLAN,
    in0.ACC AS ACC,
    in0.`Number of Dental Claims Received` AS `Number of Dental Claims Received`,
    in0.Household AS Household,
    in0.`Months on Plan` AS `Months on Plan`,
    in0.`Claims Outstanding` AS `Claims Outstanding`,
    in0.Woman_Aged_15_18 AS Woman_Aged_15_18,
    in0.`PA Denial Count` AS `PA Denial Count`,
    in0.HPN AS HPN,
    in0.`Out of Network Claims Received` AS `Out of Network Claims Received`,
    in0.`Max_Non-April Caller` AS `Max_Non-April Caller`,
    in0.`OOA Claims Received` AS `OOA Claims Received`,
    in0.HoldDurationSeconds AS HoldDurationSeconds,
    in0.`Right_Most Complex Case` AS `Right_Most Complex Case`,
    in0.`Number of Claims Paid` AS `Number of Claims Paid`,
    in0.Male_Aged_33_40 AS Male_Aged_33_40,
    in0.`Longest Processing Claim Paid` AS `Longest Processing Claim Paid`,
    in0.`Right_PA Approved Count` AS `Right_PA Approved Count`,
    in0.`OOA Visits Paid` AS `OOA Visits Paid`,
    in0.`Member Balance Received` AS `Member Balance Received`,
    in0.MONTH AS month,
    in0.`ED Visits Paid` AS `ED Visits Paid`,
    in0.MBR_ENR_COBRA_IN AS MBR_ENR_COBRA_IN,
    in0.Male_Aged_74_ AS Male_Aged_74_,
    in0.Male_Aged_10_14 AS Male_Aged_10_14
  
  FROM AlteryxSelect_18 AS in0
  INNER JOIN OverallSystemTe_111 AS in1
     ON ((in0.Household = in1.Household) AND (in0.YMD = in1.YMD))

)

SELECT *

FROM Join_112_inner
