{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_168 AS (

  SELECT * 
  
  FROM {{ ref('seed_FDI_Treasury_Altx_Sol_FAS107_2025_Q1_168')}}

),

TextInput_168_cast AS (

  SELECT (
           CASE
             WHEN ((TRY_TO_TIMESTAMP(CAST(Rpt_Dt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
               THEN CAST((TRY_TO_TIMESTAMP(CAST(Rpt_Dt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
             WHEN ((TRY_TO_TIMESTAMP(CAST(Rpt_Dt AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
               THEN CAST((TRY_TO_TIMESTAMP(CAST(Rpt_Dt AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
             ELSE CAST((TRY_TO_TIMESTAMP(CAST(Rpt_Dt AS string), 'yyyy-MM-dd')) AS DATE)
           END
         ) AS Rpt_Dt
  
  FROM TextInput_168 AS in0

),

Formula_169_0 AS (

  SELECT 
    (TO_DATE(CURRENT_TIMESTAMP, 'yyyy-MM-dd')) AS Current_Date,
    *
  
  FROM TextInput_168_cast AS in0

),

Formula_169_1 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', CURRENT_DATE)), 'yyyy-MM-dd')) AS FirstofMonth,
    *
  
  FROM Formula_169_0 AS in0

),

Formula_169_2 AS (

  SELECT 
    (DATE_ADD(FirstofMonth, CAST(-1 AS INTEGER))) AS LastofPrevMonth,
    (DATE_ADD((ADD_MONTHS(FirstofMonth, -1)), CAST(-1 AS INTEGER))) AS Lastof2MonthsAgo,
    *
  
  FROM Formula_169_1 AS in0

),

Formula_169_3 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN ((DAY(CURRENT_DATE)) >= 2)
              THEN LastofPrevMonth
            ELSE Lastof2MonthsAgo
          END
        ), 
        'yyyy-MM-dd')
    ) AS Rpt_Dt,
    * EXCEPT (`rpt_dt`)
  
  FROM Formula_169_2 AS in0

),

DateTime_170_0 AS (

  SELECT 
    (DATE_FORMAT(Rpt_Dt, 'yyyy-MM-dd')) AS Rpt_Dt_EOM,
    *
  
  FROM Formula_169_3 AS in0

),

Formula_233_0 AS (

  SELECT 
    CAST('File: aeast.ad.jpmorganchase.com\\\\home2 ahome00529\\\\R484637\\\\jpmDesk\\\\Desktop\\\\TD_Connect.indbc' AS string) AS Connection,
    *
  
  FROM DateTime_170_0 AS in0

)

SELECT *

FROM Formula_233_0
