{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_233_0 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_233_0')}}

),

Formula_220_to_Formula_286_0 AS (

  SELECT 
    CAST('SELECT
    AL1.prd_end_dt,
    AL1.cd_conversion_identifier,
    AL1.cd_conversion_dt,
    AL1.CD_CERTNBR,
    AL1.CD_BANK,
    AL1.cd_cost_center_exp,
    AL4.CCXREF_SAP_COMPANY,
    AL4.CCXREF_SAP_COST_CENTER,
    AL4.HCS_LOB_NODE03_NB,
    AL4.HCS_LOB_NODE04_NB,
    AL1.CD_VALUE,
    AL1.cd_dlyaccrl,
    AL1.CD_INTRATE_R,
    AL1.CD_ACCRTD,
    AL1.CD_INTPDTD,
    AL1.CD_ISSDATE,
    AL1.CD_RENDATE,
    AL1.CD_NXTMATDT,
    AL1.cd_closedt
    FROM
    ICDW_FL_GBL_V.LDA_CSCDS_ACCT_MONTHLY AL1
    LEFT OUTER JOIN (SELECT
    AL2.CCXREF_MSA_COMPANY,
    AL2.CCXREF_MSA_COST_CENTER,
    AL2.CCXREF_SAP_COMPANY,
    AL2.CCXREF_SAP_COST_CENTER,
    AL3.HCS_LOB_NODE03_NB,
    AL3.HCS_LOB_NODE04_NB
    FROM
    ICDW_FL_GBL_V.LDA_SAP_LKP_FISP_MONTHLY AL2,
    ICDW_CB_PRSN_V.CDW_HCS_LOB AL3
    WHERE
    AL2.PRD_END_DT=\'Rpt_Dt_EOM\' AND
    AL3.PRD_END_DT=\'Rpt_Dt_EOM\' AND
    1*AL2.CCXREF_SAP_COST_CENTER=1*AL3.PLN_CC_CD) AL4 ON
    1*AL1.cd_cost_center_exp=1*AL4.CCXREF_MSA_COST_CENTER AND
    1*AL1.CD_BANK=1*AL4.CCXREF_MSA_COMPANY
    WHERE
    AL1.PRD_END_DT=\'Rpt_Dt_EOM\' AND
    NOT AL1.CD_TYPE_ID=200 AND
    AL1.CD_VALUE>0' AS STRING) AS Query,
    *
  
  FROM Formula_233_0 AS in0

),

Formula_220_to_Formula_286_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Query, 'Rpt_Dt_EOM', Rpt_Dt_EOM)) AS string) AS Query,
    CAST('\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\FAS 107 SnowflakeTesting\\Outputs\\06-2022 FAS107_Output_SnowTest.xlsx|||Maturity_date' AS string) AS FilePath,
    CAST('\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\FAS 107 SnowflakeTesting\\Outputs\\06-2022 FAS107_Output_SnowTest.xlsx|||Hierarchy_Input_Reconcile' AS string) AS FilePath2,
    CAST('\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\FAS 107 SnowflakeTesting\\Outputs\\06-2022 FAS107_Output_SnowTest.xlsx|||R_Code_Reconcile' AS string) AS FilePath3,
    CAST('\\NAEAST.ad.jpmorganchase.com\\amercs$\\Group\\wil\\CCBPlanning\\Controller Intelligent Solutions Files\\Treasury\\FAS107 (temp)\\FAS 107 SnowflakeTesting\\Outputs\\06-2022 FAS107_Output_SnowTest.xlsx|||Detail_View' AS string) AS FilePath4,
    * EXCEPT (`query`)
  
  FROM Formula_220_to_Formula_286_0 AS in0

),

Formula_220_to_Formula_286_2 AS (

  SELECT 
    CAST((REGEXP_REPLACE(FilePath, '06-2022', (SUBSTRING(Rpt_Dt_EOM, 1, 7)))) AS string) AS FilePath,
    CAST((REGEXP_REPLACE(FilePath2, '06-2022', (SUBSTRING(Rpt_Dt_EOM, 1, 7)))) AS string) AS FilePath2,
    CAST((REGEXP_REPLACE(FilePath3, '06-2022', (SUBSTRING(Rpt_Dt_EOM, 1, 7)))) AS string) AS FilePath3,
    CAST((REGEXP_REPLACE(FilePath4, '06-2022', (SUBSTRING(Rpt_Dt_EOM, 1, 7)))) AS string) AS FilePath4,
    * EXCEPT (`filepath2`, `filepath3`, `filepath`, `filepath4`)
  
  FROM Formula_220_to_Formula_286_1 AS in0

)

SELECT *

FROM Formula_220_to_Formula_286_2
