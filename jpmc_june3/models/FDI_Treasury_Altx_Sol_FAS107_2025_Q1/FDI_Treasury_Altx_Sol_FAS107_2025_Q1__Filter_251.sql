{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_220_to_Formula_286_2 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2')}}

),

Formula_233_0 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_233_0')}}

),

Formula_248_0 AS (

  SELECT 
    CAST('SELECT AL1.prd_end_dt,
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
    FROM PROD_110575_ICDW_DB.DEPOSITS_V.LDA_CSCDS_ACCT_MONTHLY AS AL1
    LEFT OUTER JOIN
    (SELECT AL2.CCXREF_MSA_COMPANY,
    AL2.CCXREF_MSA_COST_CENTER,
    AL2.CCXREF_SAP_COMPANY,
    AL2.CCXREF_SAP_COST_CENTER,
    AL3.HCS_LOB_NODE03_NB,
    AL3.HCS_LOB_NODE04_NB
    FROM PROD_110575_ICDW_DB.DIGITAL_V.LDA_SAP_LKP_FISP_MONTHLY AS AL2,
    PROD_110575_ICDW_DB.RELATIONSHIP_V.CDW_HCS_LOB AS AL3
    WHERE AL2.PRD_END_DT = \'Rpt_Dt_EOM\'
    AND AL3.PRD_END_DT = \'Rpt_Dt_EOM\'
    AND 1 * AL2.CCXREF_SAP_COST_CENTER = 1 * AL3.PLN_CC_CD) AS AL4 ON 1 * AL1.cd_cost_center_exp = 1 * AL4.CCXREF_MSA_COST_CENTER
    AND 1 * AL1.CD_BANK = 1 * AL4.CCXREF_MSA_COMPANY
    WHERE AL1.PRD_END_DT = \'Rpt_Dt_EOM\'
    AND NOT AL1.CD_TYPE_ID = 200
    AND AL1.CD_VALUE > 0' AS STRING) AS DynamicQuery,
    *
  
  FROM Formula_233_0 AS in0

),

Formula_248_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(DynamicQuery, 'Rpt_Dt_EOM', Rpt_Dt_EOM)) AS string) AS DynamicQuery,
    * EXCEPT (`dynamicquery`)
  
  FROM Formula_248_0 AS in0

),

Macro_243 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file 1/Alteryx_Snowflake_Download_IDA_BlockTD_INDB.yxmc to resolve it.'
    )
  }}

),

AppendFields_252 AS (

  SELECT 
    in1.CD_CLOSEDT AS CD_CLOSEDT,
    in1.CD_ACCRTD AS CD_ACCRTD,
    in1.CCXREF_SAP_COMPANY AS CCXREF_SAP_COMPANY,
    in1.CD_INTRATE_R AS CD_INTRATE_R,
    in1.CCXREF_SAP_COST_CENTER AS CCXREF_SAP_COST_CENTER,
    in1.CD_NXTMATDT AS CD_NXTMATDT,
    in1.CD_RENDATE AS CD_RENDATE,
    in1.CD_VALUE AS CD_VALUE,
    in1.CD_CONVERSION_IDENTIFIER AS CD_CONVERSION_IDENTIFIER,
    in1.HCS_LOB_NODE03_NB AS HCS_LOB_NODE03_NB,
    in1.CD_ISSDATE AS CD_ISSDATE,
    in0.Rpt_Dt_EOM AS Rpt_Dt_EOM,
    in1.CD_COST_CENTER_EXP AS CD_COST_CENTER_EXP,
    in1.PRD_END_DT AS PRD_END_DT,
    in1.CD_BANK AS CD_BANK,
    in1.HCS_LOB_NODE04_NB AS HCS_LOB_NODE04_NB,
    in1.CD_CONVERSION_DT AS CD_CONVERSION_DT,
    in1.CD_DLYACCRL AS CD_DLYACCRL,
    in1.CD_CERTNBR AS CD_CERTNBR,
    in1.CD_INTPDTD AS CD_INTPDTD
  
  FROM Formula_220_to_Formula_286_2 AS in0
  INNER JOIN Macro_243 AS in1
     ON TRUE

),

Filter_251 AS (

  SELECT * 
  
  FROM AppendFields_252 AS in0
  
  WHERE (CD_NXTMATDT > Rpt_Dt_EOM)

)

SELECT *

FROM Filter_251
