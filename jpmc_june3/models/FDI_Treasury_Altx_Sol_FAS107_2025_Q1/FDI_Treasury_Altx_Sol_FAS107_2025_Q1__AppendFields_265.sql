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

Formula_263_to_Formula_259_3 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_263_to_Formula_259_3')}}

),

Summarize_260 AS (

  SELECT 
    SUM(CAST(CD_VALUE AS DECIMAL (19, 9))) AS BOOK_VALUE,
    SUM(Total_Coupon_Payments) AS Total_Coupon_Payments,
    SUM(WAC1) AS Sum_WAC1,
    SUM(WAM1) AS Sum_WAM1,
    SUM(COUPON_PAYMENTS1) AS Sum_COUPON_PAYMENTS1,
    SUM(Total_Coupon_Payments) AS Sum_Total_Coupon_Payments,
    `LOB Lvl2` AS `LOB Lvl2`,
    MATURITY_RANGE AS MATURITY_RANGE,
    REMAINING_MONTHS AS REMAINING_MONTHS,
    CD_NXTMATDT_2 AS CD_NXTMATDT_2,
    Node__Description AS Node__Description,
    COUPONAGG AS COUPONAGG
  
  FROM Formula_263_to_Formula_259_3 AS in0
  
  GROUP BY 
    `LOB Lvl2`, MATURITY_RANGE, REMAINING_MONTHS, CD_NXTMATDT_2, Node__Description, COUPONAGG

),

AlteryxSelect_262 AS (

  {#Consolidates portfolio metrics (book value, coupon payments, remaining months and maturity ranges) by line of business to support cash-flow forecasting and portfolio valuation.#}
  SELECT 
    REMAINING_MONTHS AS REMAINING_MONTHS,
    COUPONAGG AS COUPONAGG,
    MATURITY_RANGE AS MATURITY_RANGE,
    CD_NXTMATDT_2 AS CD_NXTMATDT_2,
    BOOK_VALUE AS BOOK_VALUE,
    `LOB Lvl2` AS `LOB Lvl2`,
    CAST(Total_Coupon_Payments AS DOUBLE) AS Total_Coupon_Payments,
    Sum_WAC1 AS Sum_WAC1,
    Sum_WAM1 AS Sum_WAM1,
    Sum_COUPON_PAYMENTS1 AS Sum_COUPON_PAYMENTS1,
    Sum_Total_Coupon_Payments AS Sum_Total_Coupon_Payments,
    * EXCEPT (`REMAINING_MONTHS`, 
    `COUPONAGG`, 
    `MATURITY_RANGE`, 
    `CD_NXTMATDT_2`, 
    `BOOK_VALUE`, 
    `LOB Lvl2`, 
    `Total_Coupon_Payments`, 
    `Sum_WAC1`, 
    `Sum_WAM1`, 
    `Sum_COUPON_PAYMENTS1`, 
    `Sum_Total_Coupon_Payments`)
  
  FROM Summarize_260 AS in0

),

Formula_261_0 AS (

  SELECT 
    CAST(((Sum_WAC1 / BOOK_VALUE) / 100) AS DOUBLE) AS WAC,
    CAST((Sum_WAM1 / BOOK_VALUE) AS DOUBLE) AS WAM,
    *
  
  FROM AlteryxSelect_262 AS in0

),

Formula_261_1 AS (

  SELECT 
    CAST(((POW((1 + WAC), (1 / 12))) - 1) AS DOUBLE) AS Periodic_Rate,
    *
  
  FROM Formula_261_0 AS in0

),

Formula_261_2 AS (

  SELECT 
    CAST((Sum_COUPON_PAYMENTS1 * Periodic_Rate) AS DOUBLE) AS Coupon_Payments,
    CAST((BOOK_VALUE * (1 + Periodic_Rate)) AS DOUBLE) AS Final_Payments,
    *
  
  FROM Formula_261_1 AS in0

),

AlteryxSelect_264 AS (

  SELECT 
    REMAINING_MONTHS AS REMAINING_MONTHS,
    COUPONAGG AS COUPONAGG,
    MATURITY_RANGE AS MATURITY_RANGE,
    CD_NXTMATDT_2 AS CD_NXTMATDT_2,
    Node__Description AS Node__Description,
    BOOK_VALUE AS BOOK_VALUE,
    WAC AS WAC,
    WAM AS WAM,
    Periodic_Rate AS Periodic_Rate,
    Coupon_Payments AS Coupon_Payments,
    Final_Payments AS Final_Payments,
    Total_Coupon_Payments AS Total_Coupon_Payments,
    `LOB Lvl2` AS `LOB Lvl2`,
    * EXCEPT (`Sum_WAC1`, 
    `Sum_WAM1`, 
    `Sum_COUPON_PAYMENTS1`, 
    `Sum_Total_Coupon_Payments`, 
    `REMAINING_MONTHS`, 
    `COUPONAGG`, 
    `MATURITY_RANGE`, 
    `CD_NXTMATDT_2`, 
    `Node__Description`, 
    `BOOK_VALUE`, 
    `WAC`, 
    `WAM`, 
    `Periodic_Rate`, 
    `Coupon_Payments`, 
    `Final_Payments`, 
    `Total_Coupon_Payments`, 
    `LOB Lvl2`)
  
  FROM Formula_261_2 AS in0

),

AppendFields_265 AS (

  SELECT 
    in1.WAM AS WAM,
    in1.COUPONAGG AS COUPONAGG,
    in1.CD_NXTMATDT_2 AS CD_NXTMATDT_2,
    in1.Coupon_Payments AS Coupon_Payments,
    in1.REMAINING_MONTHS AS REMAINING_MONTHS,
    in1.Total_Coupon_Payments AS Total_Coupon_Payments,
    in1.`LOB Lvl2` AS `LOB Lvl2`,
    in1.BOOK_VALUE AS BOOK_VALUE,
    in1.WAC AS WAC,
    in1.Node__Description AS Node__Description,
    in1.Periodic_Rate AS Periodic_Rate,
    in1.MATURITY_RANGE AS MATURITY_RANGE,
    in1.Final_Payments AS Final_Payments,
    in0.FilePath3 AS FilePath3
  
  FROM Formula_220_to_Formula_286_2 AS in0
  INNER JOIN AlteryxSelect_264 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_265
