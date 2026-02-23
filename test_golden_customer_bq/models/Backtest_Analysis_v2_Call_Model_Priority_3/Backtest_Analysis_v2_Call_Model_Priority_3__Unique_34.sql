{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_19 AS (

  SELECT * 
  
  FROM {{ ref('seed_19')}}

),

TextInput_19_cast AS (

  SELECT 
    GRP_ID AS GRP_ID,
    Blue_Connect_IND AS Blue_Connect_IND
  
  FROM TextInput_19 AS in0

),

AlteryxSelect_24 AS (

  SELECT 
    CAST(GRP_ID AS STRING) AS GRP_ID,
    * EXCEPT (`GRP_ID`)
  
  FROM TextInput_19_cast AS in0

),

OverallSystemTe_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'OverallSystemTe_1_ref') }}

),

TextToColumns_23 AS (

  {{
    prophecy_basics.TextToColumns(
      ['OverallSystemTe_1'], 
      'GRP_ID', 
      ",", 
      'splitColumns', 
      8, 
      'leaveExtraCharLastCol', 
      'GRP_ID', 
      'GRP_ID', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_23_dropGem_0 AS (

  SELECT 
    GRP_ID_1_GRP_ID AS GRP_ID1,
    GRP_ID_2_GRP_ID AS GRP_ID2,
    GRP_ID_3_GRP_ID AS GRP_ID3,
    GRP_ID_4_GRP_ID AS GRP_ID4,
    GRP_ID_5_GRP_ID AS GRP_ID5,
    GRP_ID_6_GRP_ID AS GRP_ID6,
    GRP_ID_7_GRP_ID AS GRP_ID7,
    GRP_ID_8_GRP_ID AS GRP_ID8,
    * EXCEPT (`GRP_ID_1_GRP_ID`, 
    `GRP_ID_2_GRP_ID`, 
    `GRP_ID_3_GRP_ID`, 
    `GRP_ID_4_GRP_ID`, 
    `GRP_ID_5_GRP_ID`, 
    `GRP_ID_6_GRP_ID`, 
    `GRP_ID_7_GRP_ID`, 
    `GRP_ID_8_GRP_ID`)
  
  FROM TextToColumns_23 AS in0

),

Join_28_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID5)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID5)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID5)

),

Join_29_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID6)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID6)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID6)

),

Join_26_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID3)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID3)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID3)

),

Join_27_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID4)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID4)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID4)

),

Join_20_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID1)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID1)

),

Join_30_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID7)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID7)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID7)

),

Join_25_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID2)
          THEN in1.GRP_ID
        ELSE NULL
      END
    ) AS Right_GRP_ID,
    (
      CASE
        WHEN (in0.GRP_ID = in1.GRP_ID2)
          THEN NULL
        ELSE in1.GRP_ID
      END
    ) AS GRP_ID,
    in0.* EXCEPT (`GRP_ID`),
    in1.* EXCEPT (`GRP_ID`)
  
  FROM AlteryxSelect_24 AS in0
  RIGHT JOIN TextToColumns_23_dropGem_0 AS in1
     ON (in0.GRP_ID = in1.GRP_ID2)

),

Union_31 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Join_20_right_UnionRightOuter', 
        'Join_25_right_UnionRightOuter', 
        'Join_26_right_UnionRightOuter', 
        'Join_27_right_UnionRightOuter', 
        'Join_28_right_UnionRightOuter', 
        'Join_29_right_UnionRightOuter', 
        'Join_30_right_UnionRightOuter'
      ], 
      [
        '[{"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]', 
        '[{"name": "Right_GRP_ID", "dataType": "String"}, {"name": "GRP_ID", "dataType": "String"}, {"name": "Blue_Connect_IND", "dataType": "Integer"}, {"name": "GRP_ID1", "dataType": "String"}, {"name": "GRP_ID2", "dataType": "String"}, {"name": "GRP_ID3", "dataType": "String"}, {"name": "GRP_ID4", "dataType": "String"}, {"name": "GRP_ID5", "dataType": "String"}, {"name": "GRP_ID6", "dataType": "String"}, {"name": "GRP_ID7", "dataType": "String"}, {"name": "GRP_ID8", "dataType": "String"}, {"name": "Days in Care Not Covered Received", "dataType": "String"}, {"name": "MA", "dataType": "String"}, {"name": "ID", "dataType": "String"}, {"name": "HoldDurationSeconds", "dataType": "String"}, {"name": "Months Until Call", "dataType": "String"}, {"name": "Total Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_10_14", "dataType": "String"}, {"name": "Male_Aged_33_40", "dataType": "String"}, {"name": "Days Covered Received", "dataType": "String"}, {"name": "months since last call", "dataType": "String"}, {"name": "Woman_Aged_33_40", "dataType": "String"}, {"name": "Woman_Aged_50_64", "dataType": "String"}, {"name": "YMD", "dataType": "String"}, {"name": "Woman_Aged_5_9", "dataType": "String"}, {"name": "Male_Aged_19_24", "dataType": "String"}, {"name": "ACA_GOLD_PLAN", "dataType": "String"}, {"name": "ED Visits Received", "dataType": "String"}, {"name": "Days Not Covered Paid", "dataType": "String"}, {"name": "Male_Aged_25_33", "dataType": "String"}, {"name": "Months on Plan", "dataType": "String"}, {"name": "Active Cases", "dataType": "String"}, {"name": "PPO_BRONZE_PLAN", "dataType": "String"}, {"name": "OOA Claims Received", "dataType": "String"}, {"name": "SUB_CNTGS_CNTY_CD", "dataType": "String"}, {"name": "TRAD", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String"}, {"name": "Terminated", "dataType": "String"}, {"name": "Woman_Aged_65_74", "dataType": "String"}, {"name": "PCP_FLAG", "dataType": "String"}, {"name": "Woman_Aged_25_33", "dataType": "String"}, {"name": "GRP_DP_IN", "dataType": "String"}, {"name": "OOA Visits Paid", "dataType": "String"}, {"name": "New_Retirement_Member", "dataType": "String"}, {"name": "IP Visits Paid", "dataType": "String"}, {"name": "Product_Change", "dataType": "String"}, {"name": "Max CCI Score", "dataType": "String"}, {"name": "PPO", "dataType": "String"}, {"name": "Days in Care Paid", "dataType": "String"}, {"name": "Woman_Aged_0_4", "dataType": "String"}, {"name": "PPO_GOLD_PLAN", "dataType": "String"}, {"name": "MBR_DSBLTY_IN", "dataType": "String"}, {"name": "Number of Claims Paid", "dataType": "String"}, {"name": "Out of Area Dental Claims Received", "dataType": "String"}, {"name": "TotalRVU", "dataType": "String"}, {"name": "Male_Aged_74_", "dataType": "String"}, {"name": "PA Approved Count", "dataType": "String"}, {"name": "IP Visits Received", "dataType": "String"}, {"name": "Male_Aged_40_49", "dataType": "String"}, {"name": "Male_Aged_15_18", "dataType": "String"}, {"name": "Called in 3", "dataType": "String"}, {"name": "Out of Network Dental Claims Received", "dataType": "String"}, {"name": "HMO", "dataType": "String"}, {"name": "Longest Processing Claim Paid", "dataType": "String"}, {"name": "Male_Aged_0_4", "dataType": "String"}, {"name": "Woman_Aged_74_", "dataType": "String"}, {"name": "Out of Network Dental Claims Paid", "dataType": "String"}, {"name": "Claims Outstanding", "dataType": "String"}, {"name": "Member Balance Received", "dataType": "String"}, {"name": "OP Visits Received", "dataType": "String"}, {"name": "ACA", "dataType": "String"}, {"name": "SPIRA_BNF_ID", "dataType": "String"}, {"name": "PA Denial Count", "dataType": "String"}, {"name": "Number of Calls", "dataType": "String"}, {"name": "ACA_BRONZE_PLAN", "dataType": "String"}, {"name": "MBR_ENR_COBRA_IN", "dataType": "String"}, {"name": "Number of Dental Claims Paid", "dataType": "String"}, {"name": "grp_change", "dataType": "String"}, {"name": "Woman_Aged_15_18", "dataType": "String"}, {"name": "Male_Aged_65_74", "dataType": "String"}, {"name": "Specialty Visits Received", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Denial", "dataType": "String"}, {"name": "Most Complex Case", "dataType": "String"}, {"name": "OP Visits Paid", "dataType": "String"}, {"name": "Number of Claims Received", "dataType": "String"}, {"name": "Out of Area Dental Claims Paid", "dataType": "String"}, {"name": "Total Dental Member Pay Paid", "dataType": "String"}, {"name": "Male_Aged_5_9", "dataType": "String"}, {"name": "Out of Network Claims Paid", "dataType": "String"}, {"name": "MEDSUP", "dataType": "String"}, {"name": "Number of Dental Claims Received", "dataType": "String"}, {"name": "RunTot_Inflection", "dataType": "String"}, {"name": "ACC", "dataType": "String"}, {"name": "Allowed Amount of Prior Auth Approval", "dataType": "String"}, {"name": "Re_Enroll", "dataType": "String"}, {"name": "Male_Aged_50_64", "dataType": "String"}, {"name": "PRNT_GRP_SIC_NACIS_CD", "dataType": "String"}, {"name": "Dental Member Month", "dataType": "String"}, {"name": "ACA_SILVER_PLAN", "dataType": "String"}, {"name": "Woman_Aged_10_14", "dataType": "String"}, {"name": "Member Balance Paid", "dataType": "String"}, {"name": "ACA_CATASTROPHIC_PLAN", "dataType": "String"}, {"name": "Out of Network Claims Received", "dataType": "String"}, {"name": "Claim with Most Reversals Paid", "dataType": "String"}, {"name": "Household", "dataType": "String"}, {"name": "Days Covered Paid", "dataType": "String"}, {"name": "MBR_HOME_ADDR_ST_CD", "dataType": "String"}, {"name": "Days in Care Received", "dataType": "String"}, {"name": "CallDurationSeconds", "dataType": "String"}, {"name": "HPN", "dataType": "String"}, {"name": "Dependents", "dataType": "String"}, {"name": "Specialty Visits Paid", "dataType": "String"}, {"name": "CSAT Score", "dataType": "String"}, {"name": "New_ACA_Member", "dataType": "String"}, {"name": "Woman_Aged_19_24", "dataType": "String"}, {"name": "month", "dataType": "String"}, {"name": "Woman_Aged_40_49", "dataType": "String"}, {"name": "ED Visits Paid", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_21_0 AS (

  SELECT 
    (
      CASE
        WHEN (Blue_Connect_IND = 1)
          THEN Blue_Connect_IND
        ELSE 0
      END
    ) AS Blue_Connect_IND,
    * EXCEPT (`blue_connect_ind`)
  
  FROM Union_31 AS in0

),

Summarize_32 AS (

  SELECT 
    (MAX(Blue_Connect_IND) OVER (PARTITION BY Household ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS Max_Blue_Connect_IND,
    *
  
  FROM Formula_21_0 AS in0

),

Join_33_inner_formula_0 AS (

  SELECT 
    Max_Blue_Connect_IND AS `Blue Connect Household`,
    *
  
  FROM Summarize_32 AS in0

),

Sort_35 AS (

  SELECT * 
  
  FROM Join_33_inner_formula_0 AS in0
  
  ORDER BY Household ASC, YMD ASC, `Blue Connect Household` DESC

),

Unique_34 AS (

  SELECT * 
  
  FROM Sort_35 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Household, YMD ORDER BY Household, YMD) = 1

)

SELECT *

FROM Unique_34
