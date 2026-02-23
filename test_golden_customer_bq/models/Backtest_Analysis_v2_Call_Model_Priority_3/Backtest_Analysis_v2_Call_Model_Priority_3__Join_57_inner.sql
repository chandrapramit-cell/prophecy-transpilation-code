{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH post_tree_join__45 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'post_tree_join__45_ref') }}

),

post_tree_join__44 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'post_tree_join__44_ref') }}

),

post_tree_join__43 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'post_tree_join__43_ref') }}

),

Union_46 AS (

  {{
    prophecy_basics.UnionByName(
      ['post_tree_join__43', 'post_tree_join__44', 'post_tree_join__45'], 
      [
        '[{"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_40_0 AS (

  SELECT 
    (
      CASE
        WHEN ((YMD = '2023-03-01') AND (`Months Until Call` = '1'))
          THEN 1
        ELSE 0
      END
    ) AS `April Caller`,
    (
      CASE
        WHEN (((YMD > '2022-09-01') AND (YMD < '2023-03-01')) AND (`Months Until Call` = '1'))
          THEN 1
        ELSE 0
      END
    ) AS `Non-April Caller`,
    *
  
  FROM Union_46 AS in0

),

Summarize_37 AS (

  SELECT 
    MAX(`April Caller`) AS `Max_April Caller`,
    MAX(`Non-April Caller`) AS `Max_Non-April Caller`,
    Household AS Household
  
  FROM Formula_40_0 AS in0
  
  GROUP BY Household

),

Formula_42_0 AS (

  SELECT 
    (
      CASE
        WHEN ((`Max_April Caller` = 0) AND (`Max_Non-April Caller` = 0))
          THEN 1
        ELSE 0
      END
    ) AS `Non-Caller`,
    *
  
  FROM Summarize_37 AS in0

),

Join_38_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.Household = in1.Household)
          THEN NULL
        ELSE in1.Household
      END
    ) AS Household,
    in0.* EXCEPT (`Household`),
    in1.* EXCEPT (`Household`)
  
  FROM Formula_42_0 AS in0
  RIGHT JOIN Union_46 AS in1
     ON (in0.Household = in1.Household)

),

Join_3_inner AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Join_3_inner')}}

),

Unique_34 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Unique_34')}}

),

Summarize_41 AS (

  SELECT 
    DISTINCT `Max_Non-April Caller` AS `Max_Non-April Caller`,
    `Months Until Call` AS `Months Until Call`,
    `Max_April Caller` AS `Max_April Caller`,
    `Non-Caller` AS `Non-Caller`,
    YMD AS YMD,
    Household AS Household
  
  FROM Join_38_right_UnionRightOuter AS in0

),

Join_48_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Household`, `YMD`, `Months Until Call`, `Non-Caller`)
  
  FROM Unique_34 AS in0
  INNER JOIN Summarize_41 AS in1
     ON ((in0.Household = in1.Household) AND (in0.YMD = in1.YMD))

),

Join_57_inner AS (

  SELECT 
    in1.ID AS Right_ID,
    in1.`Most Complex Case` AS `Right_Most Complex Case`,
    in1.`Active Cases` AS `Right_Active Cases`,
    in1.`PA Approved Count` AS `Right_PA Approved Count`,
    in1.`PA Denial Count` AS `Right_PA Denial Count`,
    in1.`Allowed Amount of Prior Auth Approval` AS `Right_Allowed Amount of Prior Auth Approval`,
    in1.`Allowed Amount of Prior Auth Denial` AS `Right_Allowed Amount of Prior Auth Denial`,
    in0.*,
    in1.* EXCEPT (`Household`, 
    `GRP_ID`, 
    `ID`, 
    `YMD`, 
    `PRNT_GRP_SIC_NACIS_CD`, 
    `Male_Aged_0_4`, 
    `Male_Aged_5_9`, 
    `Male_Aged_10_14`, 
    `Male_Aged_15_18`, 
    `Male_Aged_19_24`, 
    `Male_Aged_25_33`, 
    `Male_Aged_33_40`, 
    `Male_Aged_40_49`, 
    `Male_Aged_50_64`, 
    `Woman_Aged_0_4`, 
    `Woman_Aged_5_9`, 
    `Woman_Aged_10_14`, 
    `Woman_Aged_15_18`, 
    `Woman_Aged_19_24`, 
    `Woman_Aged_25_33`, 
    `Woman_Aged_33_40`, 
    `Woman_Aged_40_49`, 
    `Woman_Aged_50_64`, 
    `Male_Aged_65_74`, 
    `Male_Aged_74_`, 
    `Woman_Aged_65_74`, 
    `Woman_Aged_74_`, 
    `Dependents`, 
    `grp_change`, 
    `Dental Member Month`, 
    `Terminated`, 
    `MBR_HOME_ADDR_ST_CD`, 
    `MBR_HOME_ADDR_ZIP_CD_5`, 
    `SUB_CNTGS_CNTY_CD`, 
    `Number of Calls`, 
    `CallDurationSeconds`, 
    `HoldDurationSeconds`, 
    `Months Until Call`, 
    `New_ACA_Member`, 
    `New_Retirement_Member`, 
    `Re_Enroll`, 
    `Product_Change`, 
    `Months on Plan`, 
    `months since last call`, 
    `CSAT Score`, 
    `Days in Care Not Covered Received`, 
    `Out of Network Claims Received`, 
    `OOA Claims Received`, 
    `Days in Care Received`, 
    `Days Covered Received`, 
    `Member Balance Received`, 
    `Specialty Visits Received`, 
    `ED Visits Received`, 
    `OP Visits Received`, 
    `IP Visits Received`, 
    `Number of Claims Received`, 
    `Member Balance Paid`, 
    `Days Not Covered Paid`, 
    `Out of Network Claims Paid`, 
    `OOA Visits Paid`, 
    `Total Member Pay Paid`, 
    `Days in Care Paid`, 
    `Days Covered Paid`, 
    `Claim with Most Reversals Paid`, 
    `Specialty Visits Paid`, 
    `ED Visits Paid`, 
    `OP Visits Paid`, 
    `IP Visits Paid`, 
    `Longest Processing Claim Paid`, 
    `Number of Claims Paid`, 
    `Out of Network Dental Claims Received`, 
    `Out of Area Dental Claims Received`, 
    `Number of Dental Claims Received`, 
    `Total Dental Member Pay Paid`, 
    `Out of Network Dental Claims Paid`, 
    `Out of Area Dental Claims Paid`, 
    `Number of Dental Claims Paid`, 
    `Max CCI Score`, 
    `RunTot_Inflection`, 
    `TotalRVU`, 
    `ACC`, 
    `Claims Outstanding`, 
    `Called in 3`, 
    `MBR_DSBLTY_IN`, 
    `GRP_DP_IN`, 
    `SPIRA_BNF_ID`, 
    `PCP_FLAG`, 
    `MBR_ENR_COBRA_IN`, 
    `ACA`, 
    `ACA_BRONZE_PLAN`, 
    `ACA_CATASTROPHIC_PLAN`, 
    `ACA_GOLD_PLAN`, 
    `ACA_SILVER_PLAN`, 
    `HMO`, 
    `HPN`, 
    `MA`, 
    `MEDSUP`, 
    `PPO`, 
    `PPO_BRONZE_PLAN`, 
    `PPO_GOLD_PLAN`, 
    `TRAD`, 
    `Most Complex Case`, 
    `Active Cases`, 
    `PA Approved Count`, 
    `PA Denial Count`, 
    `Allowed Amount of Prior Auth Approval`, 
    `Allowed Amount of Prior Auth Denial`, 
    `month`, 
    `GRP_ID1`, 
    `GRP_ID2`, 
    `GRP_ID3`, 
    `GRP_ID4`, 
    `GRP_ID5`, 
    `GRP_ID6`, 
    `GRP_ID7`, 
    `GRP_ID8`, 
    `Right_GRP_ID`)
  
  FROM Join_3_inner AS in0
  INNER JOIN Join_48_inner AS in1
     ON ((in0.Household = in1.Household) AND (in0.YMD = in1.YMD))

)

SELECT *

FROM Join_57_inner
