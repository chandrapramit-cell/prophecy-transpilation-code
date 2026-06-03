{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_263_to_Formula_259_3 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_263_to_Formula_259_3')}}

),

Filter_293 AS (

  SELECT * 
  
  FROM Formula_263_to_Formula_259_3 AS in0
  
  WHERE ((LENGTH(Node__Description)) = 0)

),

AlteryxSelect_292 AS (

  SELECT 
    CD_VALUE AS BOOK_VALUE,
    * EXCEPT (`CD_VALUE`)
  
  FROM Filter_293 AS in0

),

Formula_291_0 AS (

  SELECT 
    CAST(((WAC1 / BOOK_VALUE) / 100) AS DOUBLE) AS WAC,
    CAST((WAM1 / BOOK_VALUE) AS DOUBLE) AS WAM,
    *
  
  FROM AlteryxSelect_292 AS in0

),

Formula_291_1 AS (

  SELECT 
    CAST(((POW((1 + WAC), (1 / 12))) - 1) AS DOUBLE) AS Periodic_Rate,
    *
  
  FROM Formula_291_0 AS in0

),

Formula_291_2 AS (

  SELECT 
    CAST((COUPON_PAYMENTS1 * Periodic_Rate) AS DOUBLE) AS Coupon_Payments,
    CAST((BOOK_VALUE * (1 + Periodic_Rate)) AS DOUBLE) AS Final_Payments,
    *
  
  FROM Formula_291_1 AS in0

),

Formula_220_to_Formula_286_2 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2')}}

),

AppendFields_290 AS (

  SELECT 
    in1.WAM AS WAM,
    in1.Mat_Dt_Else_str AS Mat_Dt_Else_str,
    in1.CD_NXTMATDT_FLR AS CD_NXTMATDT_FLR,
    in0.FilePath4 AS FilePath4,
    in1.CD_WEIGHT AS CD_WEIGHT,
    in1.Mat_Dt_Then_str AS Mat_Dt_Then_str,
    in1.CD_CLOSEDT AS CD_CLOSEDT,
    in1.CD_ACCRTD AS CD_ACCRTD,
    in1.CCXREF_SAP_COMPANY AS CCXREF_SAP_COMPANY,
    in1.CD_INTRATE_R AS CD_INTRATE_R,
    in1.REMAINING_MONTHS_ACT AS REMAINING_MONTHS_ACT,
    in1.CCXREF_SAP_COST_CENTER AS CCXREF_SAP_COST_CENTER,
    in1.COUPONAGG AS COUPONAGG,
    in1.REMAINING_MONTHS_ADJ AS REMAINING_MONTHS_ADJ,
    in1.WAM1 AS WAM1,
    in1.CD_NXTMATDT_2 AS CD_NXTMATDT_2,
    in1.TERM AS TERM,
    in1.Coupon_Payments AS Coupon_Payments,
    in1.REMAINING_MONTHS AS REMAINING_MONTHS,
    in1.`New Term Double` AS `New Term Double`,
    in1.CD_NXTMATDT AS CD_NXTMATDT,
    in1.CD_RENDATE AS CD_RENDATE,
    in1.Mat_Dt_Then AS Mat_Dt_Then,
    in1.Mat_Dt_Else AS Mat_Dt_Else,
    in1.Total_Coupon_Payments AS Total_Coupon_Payments,
    in1.CD_CONVERSION_IDENTIFIER AS CD_CONVERSION_IDENTIFIER,
    in1.`New Term` AS `New Term`,
    in1.`LOB Lvl2` AS `LOB Lvl2`,
    in1.HCS_LOB_NODE03_NB AS HCS_LOB_NODE03_NB,
    in1.BOOK_VALUE AS BOOK_VALUE,
    in1.CD_ISSDATE AS CD_ISSDATE,
    in1.WAC1 AS WAC1,
    in1.`New Term Calc Last Mo` AS `New Term Calc Last Mo`,
    in1.Rpt_Dt_EOM AS Rpt_Dt_EOM,
    in1.CD_COST_CENTER_EXP AS CD_COST_CENTER_EXP,
    in1.`New Term Calc 1st Mo` AS `New Term Calc 1st Mo`,
    in1.PRD_END_DT AS PRD_END_DT,
    in1.COUPON_PAYMENTS1 AS COUPON_PAYMENTS1,
    in1.CD_BANK AS CD_BANK,
    in1.WAC AS WAC,
    in1.HCS_LOB_NODE04_NB AS HCS_LOB_NODE04_NB,
    in1.Node__Description AS Node__Description,
    in1.CD_NXTMATDT_FLR_str AS CD_NXTMATDT_FLR_str,
    in1.CD_CONVERSION_DT AS CD_CONVERSION_DT,
    in1.Periodic_Rate AS Periodic_Rate,
    in1.`New Term Calc Mid Mo` AS `New Term Calc Mid Mo`,
    in1.CD_DLYACCRL AS CD_DLYACCRL,
    in1.CD_CERTNBR AS CD_CERTNBR,
    in1.CD_INTPDTD AS CD_INTPDTD,
    in1.PRD_END_DT_FLR AS PRD_END_DT_FLR,
    in1.MATURITY_RANGE AS MATURITY_RANGE,
    in1.Final_Payments AS Final_Payments
  
  FROM Formula_220_to_Formula_286_2 AS in0
  INNER JOIN Formula_291_2 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_290
