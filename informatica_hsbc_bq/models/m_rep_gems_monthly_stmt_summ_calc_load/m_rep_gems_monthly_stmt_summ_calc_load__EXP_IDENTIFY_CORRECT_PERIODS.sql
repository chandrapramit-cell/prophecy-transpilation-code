{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_CREDIT_CARD_STATEMENT_SUMMARY AS (

  SELECT 
    /*+ parallel(AMA) parallel(A) parallel(B) use_hash(AMA A B)*/
    AMA.ACCID AS ACCID,
    A.PERIOD AS CURR_PERIOD,
    B.PERIOD AS PREV_PERIOD,
    NVL(A.STATEMENT_BALANCE_TOTAL, 0) AS CURR_STATEMENT_BALANCE_TOTAL,
    NVL(A.TOTAL_INT_THIS_STMT, 0) AS CURR_TOTAL_INT_THIS_STATEMENT,
    NVL(A.VAL_CREDITS, 0) AS CURR_VAL_CREDITS,
    NVL(B.STATEMENT_BALANCE_TOTAL, 0) AS PREV_STATEMENT_BALANCE_TOTAL,
    NVL(B.TOTAL_INT_THIS_STMT, 0) AS PREV_TOTAL_INT_THIS_STATEMENT,
    NVL(B.VAL_CREDITS, 0) AS PREV_VAL_CREDITS,
    AMA.CYCLE_DAY AS CYCLE_DAY,
    A.BUSINESS_DATE
  
  FROM ALL_MONTHLY_ACCOUNTS AS AMA, (
    SELECT /*+ parallel(VEWA,4)*/
           *
    
    FROM (
      SELECT 
        /*+ parallel(CCSS)*/
        ACCID,
        PERIOD,
        STATEMENT_BALANCE_TOTAL,
        TOTAL_INT_THIS_STMT,
        VAL_CREDITS,
        '$$MP_BUSDATE' BUSINESS_DATE,
        RANK() OVER (PARTITION BY accid ORDER BY period DESC, statement_date DESC) RANK_NUM
      
      FROM CREDIT_CARD_STATEMENT_SUMMARY AS CCSS
      
      WHERE PERIOD <= SUBSTR('$$MP_BUSDATE', 1, 6)
    ) AS VEWA
    
    WHERE RANK_NUM = 1
  ) AS A, (
    SELECT /*+ parallel(VEWB)*/
           *
    
    FROM (
      SELECT 
        /*+ parallel(CCSSB)*/
        ACCID,
        PERIOD,
        STATEMENT_BALANCE_TOTAL,
        TOTAL_INT_THIS_STMT,
        VAL_CREDITS,
        RANK() OVER (PARTITION BY accid ORDER BY period DESC, statement_date DESC) RANK_NUM
      
      FROM CREDIT_CARD_STATEMENT_SUMMARY AS CCSSB
      
      WHERE PERIOD <= SUBSTR('$$MP_BUSDATE', 1, 6)
    ) AS VEWB
    
    WHERE RANK_NUM = 2
  ) AS B
  
  WHERE AMA.ACCID = B.ACCID (+) AND AMA.ACCID = A.ACCID AND AMA.ACC_TYPE IN ('410', '420', '510', '520')

),

EXP_IDENTIFY_CORRECT_PERIODS AS (

  SELECT 
    ACCID AS ACCID,
    CURR_PERIOD AS CURR_PERIOD,
    PREV_PERIOD AS PREV_PERIOD,
    CYCLE_DAY AS CYCLE_DAY,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (CURR_PERIOD = CAST(SUBSTRING(BUSINESS_DATE, 0, 6) AS FLOAT64))
                THEN 'Y'
              ELSE 'N'
            END
          ) = 'Y'
        )
          THEN CURR_STATEMENT_BALANCE_TOTAL
        ELSE 0
      END
    ) AS CURR_STATEMENT_BALANCE_TOTAL,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (CURR_PERIOD = CAST(SUBSTRING(BUSINESS_DATE, 0, 6) AS FLOAT64))
                THEN 'Y'
              ELSE 'N'
            END
          ) = 'Y'
        )
          THEN CURR_TOTAL_INT
        ELSE 0
      END
    ) AS CURR_TOTAL_INT,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (CURR_PERIOD = CAST(SUBSTRING(BUSINESS_DATE, 0, 6) AS FLOAT64))
                THEN 'Y'
              ELSE 'N'
            END
          ) = 'Y'
        )
          THEN CURR_VAL_CREDITS
        ELSE 0
      END
    ) AS CURR_VAL_CREDITS,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (
                PREV_PERIOD = CAST(SUBSTRING(
                  (PARSE_TIMESTAMP('%Y%m%d', (DATE_ADD(CAST((PARSE_TIMESTAMP('%Y%m%d', BUSINESS_DATE)) AS DATE), INTERVAL -1 MONTH)))), 
                  0, 
                  6) AS FLOAT64)
              )
                THEN 'Y'
              ELSE 'N'
            END
          ) = 'Y'
        )
          THEN PREV_TOTAL_INT
        ELSE 0
      END
    ) AS PREV_STATEMENT_BALANCE_TOTAL,
    1 AS PREV_TOTAL_INT,
    1 AS PREV_VAL_CREDITS
  
  FROM SQ_CREDIT_CARD_STATEMENT_SUMMARY AS in0

)

SELECT *

FROM EXP_IDENTIFY_CORRECT_PERIODS
