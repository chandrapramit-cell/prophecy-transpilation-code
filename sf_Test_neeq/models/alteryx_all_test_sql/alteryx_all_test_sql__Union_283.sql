{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MakeColumns_36 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_36') }}

),

TextInput_53 AS (

  SELECT * 
  
  FROM {{ ref('seed_53')}}

),

TextInput_53_cast AS (

  SELECT 
    CAST(EMPLOYEE AS STRING) AS EMPLOYEE,
    "ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015",
    "COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    "OTHER SALES 2015" AS "OTHER SALES 2015",
    "NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    "401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015"
  
  FROM TextInput_53 AS in0

),

MultiFieldBinningv2_58_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_58_addMaxMin AS (

  SELECT 
    MAX("ENTERPRISE SALES 2015") AS "_MAX_ENTERPRISE SALES 2015",
    MIN("ENTERPRISE SALES 2015") AS "_MIN_ENTERPRISE SALES 2015",
    MAX("COMMERCIAL SALES 2015") AS "_MAX_COMMERCIAL SALES 2015",
    MIN("COMMERCIAL SALES 2015") AS "_MIN_COMMERCIAL SALES 2015",
    1 AS _DUMMY_COL
  
  FROM MultiFieldBinningv2_58_addDummyColumn_0 AS in0

),

MultiFieldBinningv2_58_join AS (

  SELECT 
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in1."_MAX_COMMERCIAL SALES 2015" AS "_MAX_COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in1."_MIN_ENTERPRISE SALES 2015" AS "_MIN_ENTERPRISE SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in1."_MIN_COMMERCIAL SALES 2015" AS "_MIN_COMMERCIAL SALES 2015",
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015",
    in1."_MAX_ENTERPRISE SALES 2015" AS "_MAX_ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_58_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_58_addMaxMin AS in1
     ON (in0._DUMMY_COL = in1._DUMMY_COL)

),

MultiFieldBinningv2_58_0 AS (

  SELECT 
    (
      CASE
        WHEN ("_MIN_ENTERPRISE SALES 2015" = "_MAX_ENTERPRISE SALES 2015")
          THEN NULL
        ELSE (
          LEAST(
            (WIDTH_BUCKET("ENTERPRISE SALES 2015", "_MIN_ENTERPRISE SALES 2015", "_MAX_ENTERPRISE SALES 2015", 3)), 
            3)
        )
      END
    ) AS "ENTERPRISE SALES 2015_TILE_NUM",
    (
      CASE
        WHEN ("_MIN_COMMERCIAL SALES 2015" = "_MAX_COMMERCIAL SALES 2015")
          THEN NULL
        ELSE (
          LEAST(
            (WIDTH_BUCKET("COMMERCIAL SALES 2015", "_MIN_COMMERCIAL SALES 2015", "_MAX_COMMERCIAL SALES 2015", 3)), 
            3)
        )
      END
    ) AS "COMMERCIAL SALES 2015_TILE_NUM",
    *
  
  FROM MultiFieldBinningv2_58_join AS in0

),

MultiFieldBinningv2_58_cleanup_0 AS (

  SELECT * EXCLUDE ("_DUMMY_COL", 
         "_MIN_ENTERPRISE SALES 2015", 
         "_MAX_ENTERPRISE SALES 2015", 
         "_MIN_COMMERCIAL SALES 2015", 
         "_MAX_COMMERCIAL SALES 2015")
  
  FROM MultiFieldBinningv2_58_0 AS in0

),

TextInput_106 AS (

  SELECT * 
  
  FROM {{ ref('seed_106')}}

),

TextInput_106_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    CAST(CITY AS STRING) AS CITY,
    CAST(STATE AS STRING) AS STATE,
    ZIP AS ZIP,
    CAST(CUSTOMERSEGMENT AS STRING) AS CUSTOMERSEGMENT,
    VISITS AS VISITS,
    CAST(SPEND AS FLOAT) AS SPEND,
    CAST(SPATIALOBJ AS STRING) AS SPATIALOBJ,
    (TRY_TO_TIMESTAMP(CAST(JOINDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS JOINDATE
  
  FROM TextInput_106 AS in0

),

Summarize_98 AS (

  SELECT 
    AVG(SPEND) AS AVG_SPEND,
    (MEDIAN(SPEND)) AS MEDIAN_SPEND,
    STDDEV(SPEND) AS STDDEV_SPEND,
    VARIANCE((
      CASE
        WHEN (SPEND = 0)
          THEN NULL
        ELSE SPEND
      END
    )) AS VARIANCE_SPEND,
    (APPROX_PERCENTILE(CUSTOMERID, 0.5)) AS PERCENTILE_CUSTOMERID,
    AVG((
      CASE
        WHEN (SPEND = 0)
          THEN NULL
        ELSE SPEND
      END
    )) AS AVGNO0_SPEND,
    (APPROX_PERCENTILE(SPEND, 0.5)) AS PCTLNO0_SPEND,
    (
      MEDIAN((
        CASE
          WHEN (SPEND = 0)
            THEN NULL
          ELSE SPEND
        END
      ))
    ) AS MEDIANNO0_SPEND,
    STDDEV((
      CASE
        WHEN (SPEND = 0)
          THEN NULL
        ELSE SPEND
      END
    )) AS STDDEVNO0_SPEND,
    VARIANCE((
      CASE
        WHEN (SPEND = 0)
          THEN NULL
        ELSE SPEND
      END
    )) AS VARIANCENO0_SPEND,
    STATE AS STATE
  
  FROM TextInput_106_cast AS in0
  
  GROUP BY STATE

),

TextInput_290 AS (

  SELECT * 
  
  FROM {{ ref('seed_290')}}

),

TextInput_290_cast AS (

  SELECT 
    "CUSTOMER ID" AS "CUSTOMER ID",
    (TRY_TO_TIMESTAMP(CAST("JOIN DATE" AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS "JOIN DATE",
    CAST(DOB AS STRING) AS DOB,
    CAST("LAST SAVE DATE" AS STRING) AS "LAST SAVE DATE",
    (TRY_TO_TIMESTAMP(CAST("LAST LOGIN DATETIME" AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS "LAST LOGIN DATETIME",
    CAST("LAST LOGIN TIME" AS STRING) AS "LAST LOGIN TIME"
  
  FROM TextInput_290 AS in0

),

DateTime_289_0 AS (

  SELECT 
    (TO_CHAR((TO_TIMESTAMP(DOB, 'MMMM DD, YYYY')), 'YYYY-MM-DD')) AS DOB_NEW,
    *
  
  FROM TextInput_290_cast AS in0

),

MultiFieldBinningv2_57_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_57_addNumRows AS (

  SELECT 
    COUNT(*) AS _NUM_ROWS,
    1 AS _DUMMY_COL
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_57_join_numRows AS (

  SELECT 
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in1._NUM_ROWS AS _NUM_ROWS,
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_57_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_57_addNumRows AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

MultiFieldBinningv257rowNumberEnterpriseSales2015 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY "ENTERPRISE SALES 2015" DESC NULLS FIRST) AS "_ROW_NUMBER_ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_57_join_numRows AS in0

),

MultiFieldBinningv257minRowEnterpriseSales2015 AS (

  SELECT 
    MIN("_ROW_NUMBER_ENTERPRISE SALES 2015") AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    "ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv257rowNumberEnterpriseSales2015 AS in0
  
  GROUP BY "ENTERPRISE SALES 2015"

),

MultiFieldBinningv257joinEnterpriseSales2015 AS (

  SELECT 
    in1."_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0."_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv257rowNumberEnterpriseSales2015 AS in0
  LEFT JOIN MultiFieldBinningv257minRowEnterpriseSales2015 AS in1
     ON (
      ((in0."ENTERPRISE SALES 2015" = in1."ENTERPRISE SALES 2015") OR (in0."ENTERPRISE SALES 2015" IS NULL))
      AND (in1."ENTERPRISE SALES 2015" IS NULL)
    )

),

MultiFieldBinningv257rowNumberCommercialSales2015 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY "COMMERCIAL SALES 2015" DESC NULLS FIRST) AS "_ROW_NUMBER_COMMERCIAL SALES 2015"
  
  FROM MultiFieldBinningv257joinEnterpriseSales2015 AS in0

),

Macro_455 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/prophecy/Documents/qa_test_artefacts/sqltest/macros/Macro1_singleport.yxmc to resolve it.'
    )
  }}

),

TextInput_61 AS (

  SELECT * 
  
  FROM {{ ref('seed_61')}}

),

TextInput_61_cast AS (

  SELECT 
    FIELD1 AS FIELD1,
    CAST(FIELD2 AS FLOAT) AS FIELD2,
    FIELD3 AS FIELD3
  
  FROM TextInput_61 AS in0

),

MultiFieldBinningv2_63_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_63_addMaxMin AS (

  SELECT 
    MAX(FIELD1) AS _MAX_FIELD1,
    MIN(FIELD1) AS _MIN_FIELD1,
    MAX(FIELD2) AS _MAX_FIELD2,
    MIN(FIELD2) AS _MIN_FIELD2,
    MAX(FIELD3) AS _MAX_FIELD3,
    MIN(FIELD3) AS _MIN_FIELD3,
    1 AS _DUMMY_COL
  
  FROM MultiFieldBinningv2_63_addDummyColumn_0 AS in0

),

MultiFieldBinningv2_63_join AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in1._MIN_FIELD1 AS _MIN_FIELD1,
    in1._MAX_FIELD1 AS _MAX_FIELD1,
    in0._DUMMY_COL AS _DUMMY_COL,
    in1._MAX_FIELD3 AS _MAX_FIELD3,
    in0.FIELD3 AS FIELD3,
    in1._MIN_FIELD2 AS _MIN_FIELD2,
    in0.FIELD2 AS FIELD2,
    in1._MAX_FIELD2 AS _MAX_FIELD2,
    in1._MIN_FIELD3 AS _MIN_FIELD3
  
  FROM MultiFieldBinningv2_63_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_63_addMaxMin AS in1
     ON (in0._DUMMY_COL = in1._DUMMY_COL)

),

MultiFieldBinningv2_63_0 AS (

  SELECT 
    1 AS FIELD1_TILE_NUM,
    1 AS FIELD2_TILE_NUM,
    1 AS FIELD3_TILE_NUM,
    *
  
  FROM MultiFieldBinningv2_63_join AS in0

),

TextInput_66 AS (

  SELECT * 
  
  FROM {{ ref('seed_66')}}

),

TextInput_66_cast AS (

  SELECT 
    ITEM_ID AS ITEM_ID,
    "2012_SALES" AS "2012_SALES",
    "2013_SALES" AS "2013_SALES",
    "2014_SALES" AS "2014_SALES"
  
  FROM TextInput_66 AS in0

),

Imputationv3_70_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_66_cast AS in0

),

Imputationv3_70_newValueCols AS (

  SELECT 
    AVG(ITEM_ID) AS _ITEM_ID_AGG,
    AVG("2012_SALES") AS _2012_SALES_AGG,
    AVG("2013_SALES") AS _2013_SALES_AGG,
    AVG("2014_SALES") AS _2014_SALES_AGG,
    '1' AS _DUMMY_COL
  
  FROM Imputationv3_70_addDummyColumn_0 AS in0

),

TextInput_262 AS (

  SELECT * 
  
  FROM {{ ref('seed_262')}}

),

TextInput_262_cast AS (

  SELECT 
    CAST(CHK_BAL AS STRING) AS CHK_BAL,
    DURATION AS DURATION,
    CAST(CREDIT_HIST AS STRING) AS CREDIT_HIST,
    CAST(GENDER_MARITAL AS STRING) AS GENDER_MARITAL,
    CAST(DEBTOR_GUARANTOR AS STRING) AS DEBTOR_GUARANTOR,
    LENGTH_RES AS LENGTH_RES,
    CAST(PROPERTY AS STRING) AS PROPERTY,
    AGE AS AGE,
    CAST(OTR_INSTALL AS STRING) AS OTR_INSTALL,
    CAST(TENURE AS STRING) AS TENURE,
    NUM_LOANS AS NUM_LOANS,
    CAST(JOB_TYPE AS STRING) AS JOB_TYPE,
    DEPENDENTS AS DEPENDENTS,
    CAST(TELEPHONE AS STRING) AS TELEPHONE,
    CAST(FOREIGN_WORKER AS STRING) AS FOREIGN_WORKER,
    CAST("DEFAULT" AS STRING) AS DEFAULT,
    CAST(VARIABLESAMPLE AS STRING) AS VARIABLESAMPLE
  
  FROM TextInput_262 AS in0

),

PredictiveToolsOversampleField_263_numOversampleValues AS (

  SELECT 
    *,
    COUNT(((("DEFAULT" = 'Yes')))) OVER (ORDER BY '1' ASC NULLS FIRST) AS _NUM_OVERSAMPLE_VALUES
  
  FROM TextInput_262_cast AS in0

),

PredictiveToolsOversampleField_263_randomCol_0 AS (

  SELECT 
    (
      CASE
        WHEN ("DEFAULT" = 'Yes')
          THEN 1
        ELSE (uniform(0, 1, random()))
      END
    ) AS _RAND_COL,
    *
  
  FROM PredictiveToolsOversampleField_263_numOversampleValues AS in0

),

PredictiveToolsOversampleField_263_orderByRandomVals AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY _RAND_COL ASC NULLS FIRST) AS _ROW_NUMBER
  
  FROM PredictiveToolsOversampleField_263_randomCol_0 AS in0

),

TextInput_118 AS (

  SELECT * 
  
  FROM {{ ref('seed_118')}}

),

TextInput_118_cast AS (

  SELECT 
    STREAMNUMBER AS STREAMNUMBER,
    ID AS ID,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    CAST(GENDER AS STRING) AS GENDER
  
  FROM TextInput_118 AS in0

),

Union_120_reformat_1 AS (

  SELECT 
    ID AS ID,
    STREAMNUMBER AS STREAMNUMBER,
    FIRSTNAME AS FIRSTNAME,
    LASTNAME AS LASTNAME
  
  FROM TextInput_118_cast AS in0

),

TextInput_110 AS (

  SELECT * 
  
  FROM {{ ref('seed_110')}}

),

TextInput_110_cast AS (

  SELECT 
    STREAMNUMBER AS STREAMNUMBER,
    ID AS ID,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME
  
  FROM TextInput_110 AS in0

),

Union_120_reformat_0 AS (

  SELECT 
    ID AS ID,
    STREAMNUMBER AS STREAMNUMBER,
    FIRSTNAME AS FIRSTNAME,
    LASTNAME AS LASTNAME
  
  FROM TextInput_110_cast AS in0

),

Union_120 AS (

  SELECT * 
  
  FROM Union_120_reformat_0 AS in0
  
  UNION ALL
  
  SELECT * 
  
  FROM Union_120_reformat_1 AS in1

),

Imputationv3_70_join AS (

  SELECT 
    in1._2012_SALES_AGG AS _2012_SALES_AGG,
    in0.ITEM_ID AS ITEM_ID,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."2013_SALES" AS "2013_SALES",
    in1._ITEM_ID_AGG AS _ITEM_ID_AGG,
    in1._2013_SALES_AGG AS _2013_SALES_AGG,
    in0."2012_SALES" AS "2012_SALES",
    in1._2014_SALES_AGG AS _2014_SALES_AGG,
    in0."2014_SALES" AS "2014_SALES"
  
  FROM Imputationv3_70_addDummyColumn_0 AS in0
  LEFT JOIN Imputationv3_70_newValueCols AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

Imputationv3_70_0 AS (

  SELECT 
    (
      CASE
        WHEN (ITEM_ID IS NULL)
          THEN _ITEM_ID_AGG
        ELSE ITEM_ID
      END
    ) AS ITEM_ID,
    (
      CASE
        WHEN ("2012_SALES" IS NULL)
          THEN _2012_SALES_AGG
        ELSE "2012_SALES"
      END
    ) AS "2012_SALES",
    (
      CASE
        WHEN ("2013_SALES" IS NULL)
          THEN _2013_SALES_AGG
        ELSE "2013_SALES"
      END
    ) AS "2013_SALES",
    (
      CASE
        WHEN ("2014_SALES" IS NULL)
          THEN _2014_SALES_AGG
        ELSE "2014_SALES"
      END
    ) AS "2014_SALES",
    * EXCLUDE ("ITEM_ID", "2013_SALES", "2012_SALES", "2014_SALES")
  
  FROM Imputationv3_70_join AS in0

),

Imputationv3_70_cleanup_0 AS (

  SELECT * EXCLUDE ("_ITEM_ID_AGG", "_2012_SALES_AGG", "_2013_SALES_AGG", "_2014_SALES_AGG", "_DUMMY_COL")
  
  FROM Imputationv3_70_0 AS in0

),

Union_224_reformat_3 AS (

  SELECT 
    CAST("2012_SALES" AS FLOAT) AS "2012_SALES",
    CAST("2013_SALES" AS FLOAT) AS "2013_SALES",
    CAST("2014_SALES" AS FLOAT) AS "2014_SALES",
    CAST(ITEM_ID AS FLOAT) AS ITEM_ID
  
  FROM Imputationv3_70_cleanup_0 AS in0

),

MultiFieldBinningv2_62_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_62_addNumRows AS (

  SELECT 
    COUNT(*) AS _NUM_ROWS,
    1 AS _DUMMY_COL
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_62_join_numRows AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in1._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2
  
  FROM MultiFieldBinningv2_62_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_62_addNumRows AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

MultiFieldBinningv262rowNumberField1 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD1 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD1
  
  FROM MultiFieldBinningv2_62_join_numRows AS in0

),

MultiFieldBinningv262minRowField1 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD1) AS _MIN_ROW_NUMBER_FIELD1,
    FIELD1 AS FIELD1
  
  FROM MultiFieldBinningv262rowNumberField1 AS in0
  
  GROUP BY FIELD1

),

MultiFieldBinningv262joinField1 AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in1._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1
  
  FROM MultiFieldBinningv262rowNumberField1 AS in0
  LEFT JOIN MultiFieldBinningv262minRowField1 AS in1
     ON (((in0.FIELD1 = in1.FIELD1) OR (in0.FIELD1 IS NULL)) AND (in1.FIELD1 IS NULL))

),

MultiFieldBinningv262rowNumberField2 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD2 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv262joinField1 AS in0

),

MultiFieldBinningv262minRowField2 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD2) AS _MIN_ROW_NUMBER_FIELD2,
    FIELD2 AS FIELD2
  
  FROM MultiFieldBinningv262rowNumberField2 AS in0
  
  GROUP BY FIELD2

),

MultiFieldBinningv262joinField2 AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in0._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1,
    in1._MIN_ROW_NUMBER_FIELD2 AS _MIN_ROW_NUMBER_FIELD2,
    in0._ROW_NUMBER_FIELD2 AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv262rowNumberField2 AS in0
  LEFT JOIN MultiFieldBinningv262minRowField2 AS in1
     ON (((in0.FIELD2 = in1.FIELD2) OR (in0.FIELD2 IS NULL)) AND (in1.FIELD2 IS NULL))

),

MultiFieldBinningv262rowNumberField3 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD3 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD3
  
  FROM MultiFieldBinningv262joinField2 AS in0

),

MultiFieldBinningv262minRowField3 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD3) AS _MIN_ROW_NUMBER_FIELD3,
    FIELD3 AS FIELD3
  
  FROM MultiFieldBinningv262rowNumberField3 AS in0
  
  GROUP BY FIELD3

),

MultiFieldBinningv262joinField3 AS (

  SELECT 
    in0._ROW_NUMBER_FIELD3 AS _ROW_NUMBER_FIELD3,
    in1._MIN_ROW_NUMBER_FIELD3 AS _MIN_ROW_NUMBER_FIELD3,
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in0._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1,
    in0._MIN_ROW_NUMBER_FIELD2 AS _MIN_ROW_NUMBER_FIELD2,
    in0._ROW_NUMBER_FIELD2 AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv262rowNumberField3 AS in0
  LEFT JOIN MultiFieldBinningv262minRowField3 AS in1
     ON (((in0.FIELD3 = in1.FIELD3) OR (in0.FIELD3 IS NULL)) AND (in1.FIELD3 IS NULL))

),

MultiFieldBinningv2_62_0 AS (

  SELECT 
    (FLOOR((((_MIN_ROW_NUMBER_FIELD1 - 1) * 500) / _NUM_ROWS)) + 1) AS FIELD1_TILE_NUM,
    (FLOOR((((_MIN_ROW_NUMBER_FIELD2 - 1) * 500) / _NUM_ROWS)) + 1) AS FIELD2_TILE_NUM,
    (FLOOR((((_MIN_ROW_NUMBER_FIELD3 - 1) * 500) / _NUM_ROWS)) + 1) AS FIELD3_TILE_NUM,
    *
  
  FROM MultiFieldBinningv262joinField3 AS in0

),

MultiFieldBinningv2_62_cleanup_0 AS (

  SELECT * EXCLUDE ("_NUM_ROWS", 
         "_DUMMY_COL", 
         "_ROW_NUMBER_FIELD1", 
         "_MIN_ROW_NUMBER_FIELD1", 
         "_ROW_NUMBER_FIELD2", 
         "_MIN_ROW_NUMBER_FIELD2", 
         "_ROW_NUMBER_FIELD3", 
         "_MIN_ROW_NUMBER_FIELD3")
  
  FROM MultiFieldBinningv2_62_0 AS in0

),

MultiFieldBinningv2_60_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_60_addNumRows AS (

  SELECT 
    COUNT(*) AS _NUM_ROWS,
    1 AS _DUMMY_COL
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_60_join_numRows AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in1._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2
  
  FROM MultiFieldBinningv2_60_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_60_addNumRows AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

Summarize_93 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((CITY IS NULL) OR (CAST(CITY AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS "COUNT",
    CITY AS CITY
  
  FROM TextInput_106_cast AS in0
  
  GROUP BY CITY

),

TextInput_270 AS (

  SELECT * 
  
  FROM {{ ref('seed_270')}}

),

TextInput_270_cast AS (

  SELECT 
    CAST(ADDRESS AS STRING) AS ADDRESS,
    CAST(CITY AS STRING) AS CITY,
    CAST(STATE AS STRING) AS STATE,
    ZIP AS ZIP,
    CAST(COMPANY AS STRING) AS COMPANY,
    "AVERAGE SALE" AS "AVERAGE SALE"
  
  FROM TextInput_270 AS in0

),

RandomRecords_269_randOrder AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY (uniform(0, 1, random())) ASC NULLS FIRST) AS _ROW_NUMBER
  
  FROM TextInput_270_cast AS in0

),

RandomRecords_269 AS (

  SELECT * 
  
  FROM RandomRecords_269_randOrder AS in0
  
  WHERE (_ROW_NUMBER <= 1000)

),

RandomRecords_269_cleanup_0 AS (

  SELECT * EXCLUDE ("_ROW_NUMBER")
  
  FROM RandomRecords_269 AS in0

),

TextInput_135 AS (

  SELECT * 
  
  FROM {{ ref('seed_135')}}

),

TextInput_135_cast AS (

  SELECT 
    CAST("NAME" AS STRING) AS "NAME",
    CAST("POSITION TITLE" AS STRING) AS "POSITION TITLE",
    CAST(MANAGER AS STRING) AS MANAGER
  
  FROM TextInput_135 AS in0

),

JoinMultiple_142_in2 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_135_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_2', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_138 AS (

  SELECT * 
  
  FROM {{ ref('seed_138')}}

),

TextInput_138_cast AS (

  SELECT 
    CAST("NAME" AS STRING) AS "NAME",
    "NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    SALARY AS SALARY
  
  FROM TextInput_138 AS in0

),

JoinMultiple_142_in1 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_138_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_1', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_137 AS (

  SELECT * 
  
  FROM {{ ref('seed_137')}}

),

TextInput_137_cast AS (

  SELECT 
    CAST("NAME" AS STRING) AS "NAME",
    AGE AS AGE,
    CAST("NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS INT) AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE"
  
  FROM TextInput_137 AS in0

),

JoinMultiple_142_in0 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_137_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_0', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

JoinMultiple_142 AS (

  SELECT 
    in0."NAME" AS "NAME",
    in1."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE",
    in0.AGE AS AGE,
    in1.SALARY AS SALARY,
    in2.MANAGER AS MANAGER,
    in2."POSITION TITLE" AS "POSITION TITLE",
    in2."NAME" AS INPUT_HASH3_NAME,
    in1."NAME" AS INPUT_HASH2_NAME
  
  FROM JoinMultiple_142_in0 AS in0
  FULL JOIN JoinMultiple_142_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)
  FULL JOIN JoinMultiple_142_in2 AS in2
     ON (coalesce(in0.RECORDPOSITIONFORJOIN_0, in1.RECORDPOSITIONFORJOIN_1) = in2.RECORDPOSITIONFORJOIN_2)

),

MultiFieldBinningv2_54_addNumRows AS (

  SELECT 
    COUNT(*) AS _NUM_ROWS,
    1 AS _DUMMY_COL
  
  FROM TextInput_53_cast AS in0

),

TextInput_24 AS (

  SELECT * 
  
  FROM {{ ref('seed_24')}}

),

TextInput_24_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST(CITY AS STRING) AS CITY,
    VISITS AS VISITS,
    CAST(SPEND AS FLOAT) AS SPEND,
    CAST(LATITUDE AS FLOAT) AS LATITUDE
  
  FROM TextInput_24 AS in0

),

CountRecords_23 AS (

  SELECT COUNT(*) AS "COUNT"
  
  FROM TextInput_24_cast AS in0

),

TextInput_25 AS (

  SELECT * 
  
  FROM {{ ref('seed_25')}}

),

TextInput_25_cast AS (

  SELECT CAST(COL1 AS STRING) AS COL1
  
  FROM TextInput_25 AS in0

),

CountRecords_21 AS (

  SELECT COUNT(*) AS "COUNT"
  
  FROM TextInput_25_cast AS in0

),

Union_182 AS (

  {{
    prophecy_basics.UnionByName(
      ['CountRecords_21', 'CountRecords_23'], 
      ['[{"name": "COUNT", "dataType": "Integer"}]', '[{"name": "COUNT", "dataType": "Integer"}]'], 
      'allowMissingColumns'
    )
  }}

),

MultiFieldBinningv2_56_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_53_cast AS in0

),

TextInput_7 AS (

  SELECT * 
  
  FROM {{ ref('seed_7')}}

),

TextInput_7_cast AS (

  SELECT 
    "STORE NUMBER" AS "STORE NUMBER",
    "UNITS" AS "UNITS",
    CAST(UNITCOST AS FLOAT) AS UNITCOST
  
  FROM TextInput_7 AS in0

),

WeightedAvg_12 AS (

  SELECT 
    (
      CASE
        WHEN (SUM("STORE NUMBER") = 0)
          THEN NULL
        ELSE (SUM(("STORE NUMBER" * "STORE NUMBER")) / SUM("STORE NUMBER"))
      END
    ) AS WEIGHTEDAVERAGE,
    "STORE NUMBER" AS "STORE NUMBER",
    "UNITS" AS "UNITS"
  
  FROM TextInput_7_cast AS in0
  
  GROUP BY 
    "STORE NUMBER", "UNITS"

),

MultiFieldBinningv2_56_addNumRows AS (

  SELECT 
    COUNT(*) AS _NUM_ROWS,
    1 AS _DUMMY_COL
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_56_join_numRows AS (

  SELECT 
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in1._NUM_ROWS AS _NUM_ROWS,
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_56_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_56_addNumRows AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

MultiFieldBinningv256rowNumberEnterpriseSales2015 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY "ENTERPRISE SALES 2015" DESC NULLS FIRST) AS "_ROW_NUMBER_ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_56_join_numRows AS in0

),

MultiFieldBinningv256minRowEnterpriseSales2015 AS (

  SELECT 
    MIN("_ROW_NUMBER_ENTERPRISE SALES 2015") AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    "ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv256rowNumberEnterpriseSales2015 AS in0
  
  GROUP BY "ENTERPRISE SALES 2015"

),

MultiFieldBinningv256joinEnterpriseSales2015 AS (

  SELECT 
    in1."_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0."_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv256rowNumberEnterpriseSales2015 AS in0
  LEFT JOIN MultiFieldBinningv256minRowEnterpriseSales2015 AS in1
     ON (
      ((in0."ENTERPRISE SALES 2015" = in1."ENTERPRISE SALES 2015") OR (in0."ENTERPRISE SALES 2015" IS NULL))
      AND (in1."ENTERPRISE SALES 2015" IS NULL)
    )

),

MultiFieldBinningv2_56_0 AS (

  SELECT 
    (FLOOR(((("_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" - 1) * 3) / _NUM_ROWS)) + 1) AS "ENTERPRISE SALES 2015_TILE_NUM",
    *
  
  FROM MultiFieldBinningv256joinEnterpriseSales2015 AS in0

),

MultiFieldBinningv2_56_cleanup_0 AS (

  SELECT * EXCLUDE ("_NUM_ROWS", "_DUMMY_COL", "_ROW_NUMBER_ENTERPRISE SALES 2015", "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015")
  
  FROM MultiFieldBinningv2_56_0 AS in0

),

MultiFieldBinningv2_55_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_55_addMaxMin AS (

  SELECT 
    MAX("ENTERPRISE SALES 2015") AS "_MAX_ENTERPRISE SALES 2015",
    MIN("ENTERPRISE SALES 2015") AS "_MIN_ENTERPRISE SALES 2015",
    1 AS _DUMMY_COL
  
  FROM MultiFieldBinningv2_55_addDummyColumn_0 AS in0

),

MultiFieldBinningv2_55_join AS (

  SELECT 
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in1."_MIN_ENTERPRISE SALES 2015" AS "_MIN_ENTERPRISE SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015",
    in1."_MAX_ENTERPRISE SALES 2015" AS "_MAX_ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_55_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_55_addMaxMin AS in1
     ON (in0._DUMMY_COL = in1._DUMMY_COL)

),

MultiFieldBinningv2_55_0 AS (

  SELECT 
    (
      CASE
        WHEN ("_MIN_ENTERPRISE SALES 2015" = "_MAX_ENTERPRISE SALES 2015")
          THEN NULL
        ELSE (
          LEAST(
            (WIDTH_BUCKET("ENTERPRISE SALES 2015", "_MIN_ENTERPRISE SALES 2015", "_MAX_ENTERPRISE SALES 2015", 2)), 
            2)
        )
      END
    ) AS "ENTERPRISE SALES 2015_TILE_NUM",
    *
  
  FROM MultiFieldBinningv2_55_join AS in0

),

MultiFieldBinningv2_55_cleanup_0 AS (

  SELECT * EXCLUDE ("_DUMMY_COL", "_MIN_ENTERPRISE SALES 2015", "_MAX_ENTERPRISE SALES 2015")
  
  FROM MultiFieldBinningv2_55_0 AS in0

),

MultiFieldBinningv2_54_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_53_cast AS in0

),

MultiFieldBinningv2_54_join_numRows AS (

  SELECT 
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in1._NUM_ROWS AS _NUM_ROWS,
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_54_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_54_addNumRows AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

MultiFieldBinningv254rowNumberEnterpriseSales2015 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY "ENTERPRISE SALES 2015" DESC NULLS FIRST) AS "_ROW_NUMBER_ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv2_54_join_numRows AS in0

),

MultiFieldBinningv254minRowEnterpriseSales2015 AS (

  SELECT 
    MIN("_ROW_NUMBER_ENTERPRISE SALES 2015") AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    "ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv254rowNumberEnterpriseSales2015 AS in0
  
  GROUP BY "ENTERPRISE SALES 2015"

),

MultiFieldBinningv254joinEnterpriseSales2015 AS (

  SELECT 
    in1."_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0."_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv254rowNumberEnterpriseSales2015 AS in0
  LEFT JOIN MultiFieldBinningv254minRowEnterpriseSales2015 AS in1
     ON (
      ((in0."ENTERPRISE SALES 2015" = in1."ENTERPRISE SALES 2015") OR (in0."ENTERPRISE SALES 2015" IS NULL))
      AND (in1."ENTERPRISE SALES 2015" IS NULL)
    )

),

MultiFieldBinningv2_54_0 AS (

  SELECT 
    (FLOOR(((("_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" - 1) * 2) / _NUM_ROWS)) + 1) AS "ENTERPRISE SALES 2015_TILE_NUM",
    *
  
  FROM MultiFieldBinningv254joinEnterpriseSales2015 AS in0

),

MultiFieldBinningv2_54_cleanup_0 AS (

  SELECT * EXCLUDE ("_NUM_ROWS", "_DUMMY_COL", "_ROW_NUMBER_ENTERPRISE SALES 2015", "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015")
  
  FROM MultiFieldBinningv2_54_0 AS in0

),

MultiFieldBinningv257minRowCommercialSales2015 AS (

  SELECT 
    MIN("_ROW_NUMBER_COMMERCIAL SALES 2015") AS "_MIN_ROW_NUMBER_COMMERCIAL SALES 2015",
    "COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015"
  
  FROM MultiFieldBinningv257rowNumberCommercialSales2015 AS in0
  
  GROUP BY "COMMERCIAL SALES 2015"

),

MultiFieldBinningv257joinCommercialSales2015 AS (

  SELECT 
    in0."_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015",
    in0."OTHER SALES 2015" AS "OTHER SALES 2015",
    in0."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."COMMERCIAL SALES 2015" AS "COMMERCIAL SALES 2015",
    in0.EMPLOYEE AS EMPLOYEE,
    in0."_ROW_NUMBER_COMMERCIAL SALES 2015" AS "_ROW_NUMBER_COMMERCIAL SALES 2015",
    in0._NUM_ROWS AS _NUM_ROWS,
    in0."_ROW_NUMBER_ENTERPRISE SALES 2015" AS "_ROW_NUMBER_ENTERPRISE SALES 2015",
    in1."_MIN_ROW_NUMBER_COMMERCIAL SALES 2015" AS "_MIN_ROW_NUMBER_COMMERCIAL SALES 2015",
    in0."401K CONTRIBUTION FOR 2015" AS "401K CONTRIBUTION FOR 2015",
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."ENTERPRISE SALES 2015" AS "ENTERPRISE SALES 2015"
  
  FROM MultiFieldBinningv257rowNumberCommercialSales2015 AS in0
  LEFT JOIN MultiFieldBinningv257minRowCommercialSales2015 AS in1
     ON (
      ((in0."COMMERCIAL SALES 2015" = in1."COMMERCIAL SALES 2015") OR (in0."COMMERCIAL SALES 2015" IS NULL))
      AND (in1."COMMERCIAL SALES 2015" IS NULL)
    )

),

MultiFieldBinningv2_57_0 AS (

  SELECT 
    (FLOOR(((("_MIN_ROW_NUMBER_ENTERPRISE SALES 2015" - 1) * 3) / _NUM_ROWS)) + 1) AS "ENTERPRISE SALES 2015_TILE_NUM",
    (FLOOR(((("_MIN_ROW_NUMBER_COMMERCIAL SALES 2015" - 1) * 3) / _NUM_ROWS)) + 1) AS "COMMERCIAL SALES 2015_TILE_NUM",
    *
  
  FROM MultiFieldBinningv257joinCommercialSales2015 AS in0

),

MultiFieldBinningv2_57_cleanup_0 AS (

  SELECT * EXCLUDE ("_NUM_ROWS", 
         "_DUMMY_COL", 
         "_ROW_NUMBER_ENTERPRISE SALES 2015", 
         "_MIN_ROW_NUMBER_ENTERPRISE SALES 2015", 
         "_ROW_NUMBER_COMMERCIAL SALES 2015", 
         "_MIN_ROW_NUMBER_COMMERCIAL SALES 2015")
  
  FROM MultiFieldBinningv2_57_0 AS in0

),

Union_214 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'MultiFieldBinningv2_58_cleanup_0', 
        'MultiFieldBinningv2_54_cleanup_0', 
        'MultiFieldBinningv2_57_cleanup_0', 
        'MultiFieldBinningv2_56_cleanup_0', 
        'MultiFieldBinningv2_55_cleanup_0'
      ], 
      [
        '[{"name": "ENTERPRISE SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "OTHER SALES 2015", "dataType": "Number"}, {"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015", "dataType": "Number"}, {"name": "EMPLOYEE", "dataType": "String"}, {"name": "401K CONTRIBUTION FOR 2015", "dataType": "Number"}, {"name": "ENTERPRISE SALES 2015", "dataType": "Number"}]', 
        '[{"name": "ENTERPRISE SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "OTHER SALES 2015", "dataType": "Number"}, {"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015", "dataType": "Number"}, {"name": "EMPLOYEE", "dataType": "String"}, {"name": "401K CONTRIBUTION FOR 2015", "dataType": "Number"}, {"name": "ENTERPRISE SALES 2015", "dataType": "Number"}]', 
        '[{"name": "ENTERPRISE SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "OTHER SALES 2015", "dataType": "Number"}, {"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015", "dataType": "Number"}, {"name": "EMPLOYEE", "dataType": "String"}, {"name": "401K CONTRIBUTION FOR 2015", "dataType": "Number"}, {"name": "ENTERPRISE SALES 2015", "dataType": "Number"}]', 
        '[{"name": "ENTERPRISE SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "OTHER SALES 2015", "dataType": "Number"}, {"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015", "dataType": "Number"}, {"name": "EMPLOYEE", "dataType": "String"}, {"name": "401K CONTRIBUTION FOR 2015", "dataType": "Number"}, {"name": "ENTERPRISE SALES 2015", "dataType": "Number"}]', 
        '[{"name": "ENTERPRISE SALES 2015_TILE_NUM", "dataType": "Number"}, {"name": "OTHER SALES 2015", "dataType": "Number"}, {"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Number"}, {"name": "COMMERCIAL SALES 2015", "dataType": "Number"}, {"name": "EMPLOYEE", "dataType": "String"}, {"name": "401K CONTRIBUTION FOR 2015", "dataType": "Number"}, {"name": "ENTERPRISE SALES 2015", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_216 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_214'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Summarize_89 AS (

  SELECT 
    (MEDIAN(VISITS)) AS MEDIAN_VISITS,
    SUM(SPEND) AS SUM_SPEND,
    STATE AS STATE,
    CITY AS CITY
  
  FROM TextInput_106_cast AS in0
  
  GROUP BY 
    STATE, CITY

),

TextInput_51 AS (

  SELECT * 
  
  FROM {{ ref('seed_51')}}

),

Imputationv3_69_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_66_cast AS in0

),

Imputationv3_69_newValueCols AS (

  SELECT 
    (MEDIAN(ITEM_ID)) AS _ITEM_ID_AGG,
    (MEDIAN("2012_SALES")) AS _2012_SALES_AGG,
    (MEDIAN("2013_SALES")) AS _2013_SALES_AGG,
    (MEDIAN("2014_SALES")) AS _2014_SALES_AGG,
    '1' AS _DUMMY_COL
  
  FROM Imputationv3_69_addDummyColumn_0 AS in0

),

Imputationv3_69_join AS (

  SELECT 
    in1._2012_SALES_AGG AS _2012_SALES_AGG,
    in0.ITEM_ID AS ITEM_ID,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."2013_SALES" AS "2013_SALES",
    in1._ITEM_ID_AGG AS _ITEM_ID_AGG,
    in1._2013_SALES_AGG AS _2013_SALES_AGG,
    in0."2012_SALES" AS "2012_SALES",
    in1._2014_SALES_AGG AS _2014_SALES_AGG,
    in0."2014_SALES" AS "2014_SALES"
  
  FROM Imputationv3_69_addDummyColumn_0 AS in0
  LEFT JOIN Imputationv3_69_newValueCols AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

JoinMultiple_143_in2 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_135_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_2', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Imputationv3_67_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_66_cast AS in0

),

WeightedAvg_8 AS (

  SELECT (
           CASE
             WHEN (SUM("UNITS") = 0)
               THEN NULL
             ELSE (SUM((UNITCOST * "UNITS")) / SUM("UNITS"))
           END
         ) AS WEIGHTEDAVERAGE
  
  FROM TextInput_7_cast AS in0

),

MultiFieldBinningv2_64_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_61_cast AS in0

),

MultiFieldBinningv2_64_addMaxMin AS (

  SELECT 
    MAX(FIELD1) AS _MAX_FIELD1,
    MIN(FIELD1) AS _MIN_FIELD1,
    MAX(FIELD2) AS _MAX_FIELD2,
    MIN(FIELD2) AS _MIN_FIELD2,
    MAX(FIELD3) AS _MAX_FIELD3,
    MIN(FIELD3) AS _MIN_FIELD3,
    1 AS _DUMMY_COL
  
  FROM MultiFieldBinningv2_64_addDummyColumn_0 AS in0

),

MultiFieldBinningv2_64_join AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in1._MIN_FIELD1 AS _MIN_FIELD1,
    in1._MAX_FIELD1 AS _MAX_FIELD1,
    in0._DUMMY_COL AS _DUMMY_COL,
    in1._MAX_FIELD3 AS _MAX_FIELD3,
    in0.FIELD3 AS FIELD3,
    in1._MIN_FIELD2 AS _MIN_FIELD2,
    in0.FIELD2 AS FIELD2,
    in1._MAX_FIELD2 AS _MAX_FIELD2,
    in1._MIN_FIELD3 AS _MIN_FIELD3
  
  FROM MultiFieldBinningv2_64_addDummyColumn_0 AS in0
  LEFT JOIN MultiFieldBinningv2_64_addMaxMin AS in1
     ON (in0._DUMMY_COL = in1._DUMMY_COL)

),

MultiFieldBinningv2_64_0 AS (

  SELECT 
    1 AS FIELD1_TILE_NUM,
    1 AS FIELD2_TILE_NUM,
    1 AS FIELD3_TILE_NUM,
    *
  
  FROM MultiFieldBinningv2_64_join AS in0

),

JoinMultiple_143_in0 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_137_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_0', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_116 AS (

  SELECT * 
  
  FROM {{ ref('seed_116')}}

),

TextInput_116_cast AS (

  SELECT 
    CAST(FIELD1 AS STRING) AS FIELD1,
    CAST(FIELD2 AS STRING) AS FIELD2,
    FIELD3 AS FIELD3,
    FIELD4 AS FIELD4
  
  FROM TextInput_116 AS in0

),

Union_117_1 AS (

  SELECT 
    FIELD3 AS PROPHECY_COLUMN_1,
    FIELD4 AS PROPHECY_COLUMN_2,
    CAST(FIELD1 AS STRING) AS PROPHECY_COLUMN_5,
    CAST(NULL AS STRING) AS PROPHECY_COLUMN_3
  
  FROM TextInput_116_cast AS in0

),

MultiFieldBinningv2_63_cleanup_0 AS (

  SELECT * EXCLUDE ("_DUMMY_COL", "_MIN_FIELD1", "_MAX_FIELD1", "_MIN_FIELD2", "_MAX_FIELD2", "_MIN_FIELD3", "_MAX_FIELD3")
  
  FROM MultiFieldBinningv2_63_0 AS in0

),

PredictiveToolsOversampleField_265_numOversampleValues AS (

  SELECT 
    *,
    COUNT(((("DEFAULT" = 'Yes')))) OVER (ORDER BY '1' ASC NULLS FIRST) AS _NUM_OVERSAMPLE_VALUES
  
  FROM TextInput_262_cast AS in0

),

PredictiveToolsOversampleField_265_randomCol_0 AS (

  SELECT 
    (
      CASE
        WHEN ("DEFAULT" = 'Yes')
          THEN 1
        ELSE (uniform(0, 1, random()))
      END
    ) AS _RAND_COL,
    *
  
  FROM PredictiveToolsOversampleField_265_numOversampleValues AS in0

),

PredictiveToolsOversampleField_265_orderByRandomVals AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY _RAND_COL ASC NULLS FIRST) AS _ROW_NUMBER
  
  FROM PredictiveToolsOversampleField_265_randomCol_0 AS in0

),

PredictiveToolsOversampleField_265 AS (

  SELECT * 
  
  FROM PredictiveToolsOversampleField_265_orderByRandomVals AS in0
  
  WHERE ((_ROW_NUMBER <= FLOOR(((_NUM_OVERSAMPLE_VALUES * 90) / 10))) OR ("DEFAULT" = 'Yes'))

),

PredictiveToolsOversampleField_265_deleteMetadataCols_0 AS (

  SELECT * EXCLUDE ("_RAND_COL", "_NUM_OVERSAMPLE_VALUES", "_ROW_NUMBER")
  
  FROM PredictiveToolsOversampleField_265 AS in0

),

PredictiveToolsOversampleField_263 AS (

  SELECT * 
  
  FROM PredictiveToolsOversampleField_263_orderByRandomVals AS in0
  
  WHERE ((_ROW_NUMBER <= FLOOR(((_NUM_OVERSAMPLE_VALUES * 50) / 50))) OR ("DEFAULT" = 'Yes'))

),

PredictiveToolsOversampleField_263_deleteMetadataCols_0 AS (

  SELECT * EXCLUDE ("_RAND_COL", "_NUM_OVERSAMPLE_VALUES", "_ROW_NUMBER")
  
  FROM PredictiveToolsOversampleField_263 AS in0

),

Union_273 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'PredictiveToolsOversampleField_263_deleteMetadataCols_0', 
        'PredictiveToolsOversampleField_265_deleteMetadataCols_0'
      ], 
      [
        '[{"name": "CHK_BAL", "dataType": "String"}, {"name": "DURATION", "dataType": "Number"}, {"name": "CREDIT_HIST", "dataType": "String"}, {"name": "GENDER_MARITAL", "dataType": "String"}, {"name": "DEBTOR_GUARANTOR", "dataType": "String"}, {"name": "LENGTH_RES", "dataType": "Number"}, {"name": "PROPERTY", "dataType": "String"}, {"name": "AGE", "dataType": "Number"}, {"name": "OTR_INSTALL", "dataType": "String"}, {"name": "TENURE", "dataType": "String"}, {"name": "NUM_LOANS", "dataType": "Number"}, {"name": "JOB_TYPE", "dataType": "String"}, {"name": "DEPENDENTS", "dataType": "Number"}, {"name": "TELEPHONE", "dataType": "String"}, {"name": "FOREIGN_WORKER", "dataType": "String"}, {"name": "DEFAULT", "dataType": "String"}, {"name": "VARIABLESAMPLE", "dataType": "String"}]', 
        '[{"name": "CHK_BAL", "dataType": "String"}, {"name": "DURATION", "dataType": "Number"}, {"name": "CREDIT_HIST", "dataType": "String"}, {"name": "GENDER_MARITAL", "dataType": "String"}, {"name": "DEBTOR_GUARANTOR", "dataType": "String"}, {"name": "LENGTH_RES", "dataType": "Number"}, {"name": "PROPERTY", "dataType": "String"}, {"name": "AGE", "dataType": "Number"}, {"name": "OTR_INSTALL", "dataType": "String"}, {"name": "TENURE", "dataType": "String"}, {"name": "NUM_LOANS", "dataType": "Number"}, {"name": "JOB_TYPE", "dataType": "String"}, {"name": "DEPENDENTS", "dataType": "Number"}, {"name": "TELEPHONE", "dataType": "String"}, {"name": "FOREIGN_WORKER", "dataType": "String"}, {"name": "DEFAULT", "dataType": "String"}, {"name": "VARIABLESAMPLE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_275 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_273'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_276 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_275'], 
      ['RECORDID'], 
      [
        'CHK_BAL', 
        'DURATION', 
        'CREDIT_HIST', 
        'GENDER_MARITAL', 
        'DEBTOR_GUARANTOR', 
        'LENGTH_RES', 
        'PROPERTY', 
        'AGE', 
        'OTR_INSTALL', 
        'TENURE', 
        'NUM_LOANS', 
        'JOB_TYPE', 
        'DEPENDENTS', 
        'TELEPHONE', 
        'FOREIGN_WORKER', 
        'DEFAULT', 
        'VARIABLESAMPLE'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'CHK_BAL', 
        'DURATION', 
        'CREDIT_HIST', 
        'GENDER_MARITAL', 
        'DEBTOR_GUARANTOR', 
        'LENGTH_RES', 
        'PROPERTY', 
        'AGE', 
        'OTR_INSTALL', 
        'TENURE', 
        'NUM_LOANS', 
        'JOB_TYPE', 
        'DEPENDENTS', 
        'TELEPHONE', 
        'FOREIGN_WORKER', 
        'DEFAULT', 
        'VARIABLESAMPLE'
      ], 
      true
    )
  }}

),

Imputationv3_69_0 AS (

  SELECT 
    (
      CASE
        WHEN (ITEM_ID = 300)
          THEN 1
        ELSE 0
      END
    ) AS ITEM_ID_INDICATOR,
    *
  
  FROM Imputationv3_69_join AS in0

),

Imputationv3_69_1 AS (

  SELECT 
    (
      CASE
        WHEN (ITEM_ID_INDICATOR = 1)
          THEN _ITEM_ID_AGG
        ELSE ITEM_ID
      END
    ) AS ITEM_ID,
    (
      CASE
        WHEN ("2012_SALES" = 300)
          THEN 1
        ELSE 0
      END
    ) AS "2012_SALES_INDICATOR",
    * EXCLUDE ("ITEM_ID")
  
  FROM Imputationv3_69_0 AS in0

),

MultiFieldBinningv260rowNumberField1 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD1 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD1
  
  FROM MultiFieldBinningv2_60_join_numRows AS in0

),

MultiFieldBinningv260minRowField1 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD1) AS _MIN_ROW_NUMBER_FIELD1,
    FIELD1 AS FIELD1
  
  FROM MultiFieldBinningv260rowNumberField1 AS in0
  
  GROUP BY FIELD1

),

MultiFieldBinningv260joinField1 AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in1._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1
  
  FROM MultiFieldBinningv260rowNumberField1 AS in0
  LEFT JOIN MultiFieldBinningv260minRowField1 AS in1
     ON (((in0.FIELD1 = in1.FIELD1) OR (in0.FIELD1 IS NULL)) AND (in1.FIELD1 IS NULL))

),

MultiFieldBinningv260rowNumberField2 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD2 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv260joinField1 AS in0

),

MultiFieldBinningv260minRowField2 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD2) AS _MIN_ROW_NUMBER_FIELD2,
    FIELD2 AS FIELD2
  
  FROM MultiFieldBinningv260rowNumberField2 AS in0
  
  GROUP BY FIELD2

),

MultiFieldBinningv260joinField2 AS (

  SELECT 
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in0._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1,
    in1._MIN_ROW_NUMBER_FIELD2 AS _MIN_ROW_NUMBER_FIELD2,
    in0._ROW_NUMBER_FIELD2 AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv260rowNumberField2 AS in0
  LEFT JOIN MultiFieldBinningv260minRowField2 AS in1
     ON (((in0.FIELD2 = in1.FIELD2) OR (in0.FIELD2 IS NULL)) AND (in1.FIELD2 IS NULL))

),

MultiFieldBinningv260rowNumberField3 AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY FIELD3 DESC NULLS FIRST) AS _ROW_NUMBER_FIELD3
  
  FROM MultiFieldBinningv260joinField2 AS in0

),

MultiFieldBinningv260minRowField3 AS (

  SELECT 
    MIN(_ROW_NUMBER_FIELD3) AS _MIN_ROW_NUMBER_FIELD3,
    FIELD3 AS FIELD3
  
  FROM MultiFieldBinningv260rowNumberField3 AS in0
  
  GROUP BY FIELD3

),

MultiFieldBinningv260joinField3 AS (

  SELECT 
    in0._ROW_NUMBER_FIELD3 AS _ROW_NUMBER_FIELD3,
    in1._MIN_ROW_NUMBER_FIELD3 AS _MIN_ROW_NUMBER_FIELD3,
    in0.FIELD1 AS FIELD1,
    in0._NUM_ROWS AS _NUM_ROWS,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0.FIELD3 AS FIELD3,
    in0.FIELD2 AS FIELD2,
    in0._MIN_ROW_NUMBER_FIELD1 AS _MIN_ROW_NUMBER_FIELD1,
    in0._ROW_NUMBER_FIELD1 AS _ROW_NUMBER_FIELD1,
    in0._MIN_ROW_NUMBER_FIELD2 AS _MIN_ROW_NUMBER_FIELD2,
    in0._ROW_NUMBER_FIELD2 AS _ROW_NUMBER_FIELD2
  
  FROM MultiFieldBinningv260rowNumberField3 AS in0
  LEFT JOIN MultiFieldBinningv260minRowField3 AS in1
     ON (((in0.FIELD3 = in1.FIELD3) OR (in0.FIELD3 IS NULL)) AND (in1.FIELD3 IS NULL))

),

MultiFieldBinningv2_60_0 AS (

  SELECT 
    (FLOOR((((_MIN_ROW_NUMBER_FIELD1 - 1) * 5) / _NUM_ROWS)) + 1) AS FIELD1_TILE_NUM,
    (FLOOR((((_MIN_ROW_NUMBER_FIELD2 - 1) * 5) / _NUM_ROWS)) + 1) AS FIELD2_TILE_NUM,
    (FLOOR((((_MIN_ROW_NUMBER_FIELD3 - 1) * 5) / _NUM_ROWS)) + 1) AS FIELD3_TILE_NUM,
    *
  
  FROM MultiFieldBinningv260joinField3 AS in0

),

MultiFieldBinningv2_60_cleanup_0 AS (

  SELECT * EXCLUDE ("_NUM_ROWS", 
         "_DUMMY_COL", 
         "_ROW_NUMBER_FIELD1", 
         "_MIN_ROW_NUMBER_FIELD1", 
         "_ROW_NUMBER_FIELD2", 
         "_MIN_ROW_NUMBER_FIELD2", 
         "_ROW_NUMBER_FIELD3", 
         "_MIN_ROW_NUMBER_FIELD3")
  
  FROM MultiFieldBinningv2_60_0 AS in0

),

Imputationv3_69_2 AS (

  SELECT 
    (
      CASE
        WHEN ("2012_SALES_INDICATOR" = 1)
          THEN _2012_SALES_AGG
        ELSE "2012_SALES"
      END
    ) AS "2012_SALES",
    (
      CASE
        WHEN ("2013_SALES" = 300)
          THEN 1
        ELSE 0
      END
    ) AS "2013_SALES_INDICATOR",
    * EXCLUDE ("2012_SALES")
  
  FROM Imputationv3_69_1 AS in0

),

Imputationv3_69_3 AS (

  SELECT 
    (
      CASE
        WHEN ("2013_SALES_INDICATOR" = 1)
          THEN _2013_SALES_AGG
        ELSE "2013_SALES"
      END
    ) AS "2013_SALES",
    (
      CASE
        WHEN ("2014_SALES" = 300)
          THEN 1
        ELSE 0
      END
    ) AS "2014_SALES_INDICATOR",
    * EXCLUDE ("2013_SALES")
  
  FROM Imputationv3_69_2 AS in0

),

Imputationv3_69_4 AS (

  SELECT 
    (
      CASE
        WHEN ("2014_SALES_INDICATOR" = 1)
          THEN _2014_SALES_AGG
        ELSE "2014_SALES"
      END
    ) AS "2014_SALES",
    * EXCLUDE ("2014_SALES")
  
  FROM Imputationv3_69_3 AS in0

),

Imputationv3_68_addDummyColumn_0 AS (

  SELECT 
    1 AS _DUMMY_COL,
    *
  
  FROM TextInput_66_cast AS in0

),

Imputationv3_68_newValueCols AS (

  SELECT 
    0 AS _ITEM_ID_AGG,
    0 AS _2012_SALES_AGG,
    0 AS _2013_SALES_AGG,
    0 AS _2014_SALES_AGG,
    '1' AS _DUMMY_COL
  
  FROM Imputationv3_68_addDummyColumn_0 AS in0

),

Imputationv3_68_join AS (

  SELECT 
    in1._2012_SALES_AGG AS _2012_SALES_AGG,
    in0.ITEM_ID AS ITEM_ID,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."2013_SALES" AS "2013_SALES",
    in1._ITEM_ID_AGG AS _ITEM_ID_AGG,
    in1._2013_SALES_AGG AS _2013_SALES_AGG,
    in0."2012_SALES" AS "2012_SALES",
    in1._2014_SALES_AGG AS _2014_SALES_AGG,
    in0."2014_SALES" AS "2014_SALES"
  
  FROM Imputationv3_68_addDummyColumn_0 AS in0
  LEFT JOIN Imputationv3_68_newValueCols AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

Imputationv3_68_0 AS (

  SELECT 
    (
      CASE
        WHEN (ITEM_ID IS NULL)
          THEN _ITEM_ID_AGG
        ELSE ITEM_ID
      END
    ) AS ITEM_ID_IMPUTEDVALUE,
    (
      CASE
        WHEN ("2012_SALES" IS NULL)
          THEN _2012_SALES_AGG
        ELSE "2012_SALES"
      END
    ) AS "2012_SALES_IMPUTEDVALUE",
    (
      CASE
        WHEN ("2013_SALES" IS NULL)
          THEN _2013_SALES_AGG
        ELSE "2013_SALES"
      END
    ) AS "2013_SALES_IMPUTEDVALUE",
    (
      CASE
        WHEN ("2014_SALES" IS NULL)
          THEN _2014_SALES_AGG
        ELSE "2014_SALES"
      END
    ) AS "2014_SALES_IMPUTEDVALUE",
    *
  
  FROM Imputationv3_68_join AS in0

),

Imputationv3_68_cleanup_0 AS (

  SELECT * EXCLUDE ("_ITEM_ID_AGG", "_2012_SALES_AGG", "_2013_SALES_AGG", "_2014_SALES_AGG", "_DUMMY_COL")
  
  FROM Imputationv3_68_0 AS in0

),

Summarize_100 AS (

  SELECT 
    (MAX_BY(FIRSTNAME, (LENGTH(FIRSTNAME)))) AS LONGEST_FIRSTNAME,
    (MIN_BY(FIRSTNAME, (LENGTH(FIRSTNAME)))) AS SHORTEST_FIRSTNAME,
    COUNT(
      (
        CASE
          WHEN ((LASTNAME IS NULL) OR (CAST(LASTNAME AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS COUNTNONBLANK_LASTNAME,
    COUNT(
      (
        CASE
          WHEN ((LASTNAME IS NULL) OR (CAST(LASTNAME AS STRING) = ''))
            THEN 1
          ELSE NULL
        END
      )) AS COUNTBLANK_LASTNAME
  
  FROM TextInput_106_cast AS in0

),

Union_117_0 AS (

  SELECT 
    STREAMNUMBER AS PROPHECY_COLUMN_1,
    ID AS PROPHECY_COLUMN_2,
    CAST(FIRSTNAME AS STRING) AS PROPHECY_COLUMN_3,
    CAST(LASTNAME AS STRING) AS PROPHECY_COLUMN_4
  
  FROM TextInput_110_cast AS in0

),

Union_117 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_117_0', 'Union_117_1'], 
      [
        '[{"name": "PROPHECY_COLUMN_1", "dataType": "Boolean"}, {"name": "PROPHECY_COLUMN_2", "dataType": "Number"}, {"name": "PROPHECY_COLUMN_3", "dataType": "String"}, {"name": "PROPHECY_COLUMN_4", "dataType": "String"}]', 
        '[{"name": "PROPHECY_COLUMN_1", "dataType": "Number"}, {"name": "PROPHECY_COLUMN_2", "dataType": "Number"}, {"name": "PROPHECY_COLUMN_5", "dataType": "String"}, {"name": "PROPHECY_COLUMN_3", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_117_postRename AS (

  SELECT 
    PROPHECY_COLUMN_5 AS FIELD1,
    PROPHECY_COLUMN_2 AS ID,
    PROPHECY_COLUMN_4 AS LASTNAME,
    PROPHECY_COLUMN_3 AS FIRSTNAME,
    PROPHECY_COLUMN_1 AS STREAMNUMBER
  
  FROM Union_117 AS in0

),

TextInput_112 AS (

  SELECT * 
  
  FROM {{ ref('seed_112')}}

),

TextInput_112_cast AS (

  SELECT 
    STREAMNUMBER AS STREAMNUMBER,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    ID AS ID
  
  FROM TextInput_112 AS in0

),

Union_111 AS (

  {{
    prophecy_basics.UnionByName(
      ['TextInput_110_cast', 'TextInput_112_cast'], 
      [
        '[{"name": "STREAMNUMBER", "dataType": "Boolean"}, {"name": "ID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}]', 
        '[{"name": "STREAMNUMBER", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "ID", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_119 AS (

  {{
    prophecy_basics.UnionByName(
      ['TextInput_110_cast', 'TextInput_118_cast'], 
      [
        '[{"name": "STREAMNUMBER", "dataType": "Boolean"}, {"name": "ID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}]', 
        '[{"name": "STREAMNUMBER", "dataType": "Number"}, {"name": "ID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_242 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_111', 'Union_117_postRename', 'Union_119', 'Union_120'], 
      [
        '[{"name": "STREAMNUMBER", "dataType": "Boolean"}, {"name": "ID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}]', 
        '[{"name": "FIELD1", "dataType": "String"}, {"name": "ID", "dataType": "Number"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "STREAMNUMBER", "dataType": "Boolean"}]', 
        '[{"name": "STREAMNUMBER", "dataType": "Boolean"}, {"name": "ID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}]', 
        '[{"name": "ID", "dataType": "Number"}, {"name": "STREAMNUMBER", "dataType": "Boolean"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_244 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_242'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_245 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_244'], 
      ['RECORDID'], 
      ['STREAMNUMBER', 'ID', 'FIRSTNAME', 'LASTNAME', 'FIELD1', 'GENDER'], 
      'NAME', 
      'VALUE', 
      ['ID', 'GENDER', 'STREAMNUMBER', 'RECORDID', 'FIRSTNAME', 'LASTNAME', 'FIELD1'], 
      true
    )
  }}

),

Formula_246_0 AS (

  SELECT 
    CAST('union' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_245 AS in0

),

TextInput_51_cast AS (

  SELECT 
    A AS A,
    CAST(B AS STRING) AS B,
    CAST(C AS STRING) AS C
  
  FROM TextInput_51 AS in0

),

Arrange_49_consolidatedDataCol_0 AS (

  SELECT 
    (
      ARRAY_CONSTRUCT(
        (
          OBJECT_CONSTRUCT(
            'shuffledColVals', 
            CAST(B AS STRING), 
            'colAVal', 
            CAST(A AS STRING), 
            'partiallyEmptyVals', 
            NULL, 
            'partiallyEmptyVals2', 
            NULL, 
            'emptyDescText', 
            '', 
            'shuffledColText', 
            'b', 
            'emptyDescText2', 
            '')
        ), 
        (
          OBJECT_CONSTRUCT(
            'shuffledColVals', 
            CAST(A AS STRING), 
            'colAVal', 
            CAST(A AS STRING), 
            'partiallyEmptyVals', 
            NULL, 
            'partiallyEmptyVals2', 
            NULL, 
            'emptyDescText', 
            '', 
            'shuffledColText', 
            'a', 
            'emptyDescText2', 
            '')
        ), 
        (
          OBJECT_CONSTRUCT(
            'shuffledColVals', 
            CAST(C AS STRING), 
            'colAVal', 
            CAST(A AS STRING), 
            'partiallyEmptyVals', 
            CAST(C AS STRING), 
            'partiallyEmptyVals2', 
            CAST(B AS STRING), 
            'emptyDescText', 
            '', 
            'shuffledColText', 
            'c', 
            'emptyDescText2', 
            '')
        ))
    ) AS _CONSOLIDATED_DATA_COL,
    *
  
  FROM TextInput_51_cast AS in0

),

Arrange_49_explode AS (

  SELECT 
    _exploded_data_col_lat.VALUE:"emptyDescText" AS EMPTYDESCTEXT,
    _exploded_data_col_lat.VALUE:"shuffledColVals" AS SHUFFLEDCOLVALS,
    _exploded_data_col_lat.VALUE:"shuffledColText" AS SHUFFLEDCOLTEXT,
    _exploded_data_col_lat.VALUE:"colAVal" AS COLAVAL,
    _exploded_data_col_lat.VALUE:"partiallyEmptyVals" AS PARTIALLYEMPTYVALS,
    _exploded_data_col_lat.VALUE:"emptyDescText2" AS EMPTYDESCTEXT2,
    _exploded_data_col_lat.VALUE:"partiallyEmptyVals2" AS PARTIALLYEMPTYVALS2,
    *
  
  FROM Arrange_49_consolidatedDataCol_0 AS in0, 
  LATERAL flatten(input => _CONSOLIDATED_DATA_COL) AS _EXPLODED_DATA_COL_LAT

),

RecordID_211 AS (

  {{
    prophecy_basics.RecordID(
      ['Arrange_49_explode'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_212 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_211'], 
      ['RECORDID'], 
      [
        'A', 
        'B', 
        'EMPTYDESCTEXT', 
        'SHUFFLEDCOLVALS', 
        'SHUFFLEDCOLTEXT', 
        'COLAVAL', 
        'PARTIALLYEMPTYVALS', 
        'EMPTYDESCTEXT2', 
        'PARTIALLYEMPTYVALS2'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'EMPTYDESCTEXT', 
        'SHUFFLEDCOLVALS', 
        'SHUFFLEDCOLTEXT', 
        'COLAVAL', 
        'PARTIALLYEMPTYVALS', 
        'EMPTYDESCTEXT2', 
        'PARTIALLYEMPTYVALS2', 
        '_CONSOLIDATED_DATA_COL', 
        'A', 
        'B', 
        'C', 
        'SEQ', 
        'KEY', 
        'PATH', 
        'INDEX', 
        'VALUE', 
        'THIS'
      ], 
      true
    )
  }}

),

Formula_213_0 AS (

  SELECT 
    CAST('arrange' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_212 AS in0

),

Union_283_reformat_8 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_213_0 AS in0

),

Summarize_88 AS (

  SELECT (CONCAT((CONCAT('<', (LISTAGG(FIRSTNAME, '>,<')))), '>')) AS CONCAT_FIRSTNAME
  
  FROM TextInput_106_cast AS in0

),

DateTime_298_0 AS (

  SELECT 
    (TO_CHAR((TO_TIMESTAMP("LAST SAVE DATE", 'MM-DD/YYYY')), 'YYYY-MM-DD')) AS "LAST SAVE DATE_NEW",
    *
  
  FROM TextInput_290_cast AS in0

),

DateTime_288_0 AS (

  SELECT 
    (TO_CHAR("JOIN DATE", 'MMMM DD, YYYY')) AS "JOIN DATE_NEW",
    *
  
  FROM TextInput_290_cast AS in0

),

DateTime_292_0 AS (

  SELECT 
    (TO_CHAR("LAST LOGIN DATETIME", 'DY, DD MMMM, YYYY')) AS "LAST LOGIN DATE",
    *
  
  FROM TextInput_290_cast AS in0

),

DateTime_296_0 AS (

  SELECT 
    (TO_CHAR("JOIN DATE", 'DY, MMMM DD, YYYY')) AS "JOIN DATE_NEW",
    *
  
  FROM TextInput_290_cast AS in0

),

AlteryxSelect_299 AS (

  SELECT 
    CAST("LAST LOGIN TIME" AS STRING) AS "LAST LOGIN TIME",
    * EXCLUDE ("LAST LOGIN TIME")
  
  FROM TextInput_290_cast AS in0

),

DateTime_294_0 AS (

  SELECT 
    (TO_CHAR((TO_TIMESTAMP("LAST LOGIN TIME", 'HH24:MI:SS')), 'HH24:MI:SS')) AS "LAST LOGIN TIME_NEW",
    *
  
  FROM AlteryxSelect_299 AS in0

),

Union_301 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'DateTime_289_0', 
        'DateTime_288_0', 
        'DateTime_294_0', 
        'DateTime_296_0', 
        'DateTime_292_0', 
        'DateTime_298_0'
      ], 
      [
        '[{"name": "DOB_NEW", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}, {"name": "LAST LOGIN TIME", "dataType": "String"}]', 
        '[{"name": "JOIN DATE_NEW", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}, {"name": "LAST LOGIN TIME", "dataType": "String"}]', 
        '[{"name": "LAST LOGIN TIME_NEW", "dataType": "String"}, {"name": "LAST LOGIN TIME", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}]', 
        '[{"name": "JOIN DATE_NEW", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}, {"name": "LAST LOGIN TIME", "dataType": "String"}]', 
        '[{"name": "LAST LOGIN DATE", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}, {"name": "LAST LOGIN TIME", "dataType": "String"}]', 
        '[{"name": "LAST SAVE DATE_NEW", "dataType": "String"}, {"name": "CUSTOMER ID", "dataType": "Number"}, {"name": "JOIN DATE", "dataType": "Timestamp"}, {"name": "DOB", "dataType": "String"}, {"name": "LAST SAVE DATE", "dataType": "String"}, {"name": "LAST LOGIN DATETIME", "dataType": "Timestamp"}, {"name": "LAST LOGIN TIME", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_303 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_301'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_304 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_303'], 
      ['RECORDID'], 
      [
        'CUSTOMER ID', 
        'JOIN DATE', 
        'DOB', 
        'LAST SAVE DATE', 
        'LAST LOGIN DATETIME', 
        'LAST LOGIN TIME', 
        'JOIN DATE_NEW', 
        'DOB_NEW', 
        'LAST SAVE DATE_NEW', 
        'LAST LOGIN DATE', 
        'LAST LOGIN TIME_NEW'
      ], 
      'NAME', 
      'VALUE', 
      [
        'DOB', 
        'JOIN DATE', 
        'JOIN DATE_NEW', 
        'LAST LOGIN DATETIME', 
        'RECORDID', 
        'LAST LOGIN DATE', 
        'LAST SAVE DATE_NEW', 
        'LAST SAVE DATE', 
        'CUSTOMER ID', 
        'LAST LOGIN TIME', 
        'LAST LOGIN TIME_NEW', 
        'DOB_NEW'
      ], 
      true
    )
  }}

),

Formula_305_0 AS (

  SELECT 
    CAST('date time' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_304 AS in0

),

TextInput_29 AS (

  SELECT * 
  
  FROM {{ ref('seed_29')}}

),

TextInput_29_cast AS (

  SELECT CAST(FIELD1 AS STRING) AS FIELD1
  
  FROM TextInput_29 AS in0

),

Base64Encoder_28_0 AS (

  SELECT 
    (BASE64_ENCODE(FIELD1)) AS BASE64_ENCODED,
    *
  
  FROM TextInput_29_cast AS in0

),

MakeColumns_37 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_37') }}

),

MakeColumns_37_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_4__TOTAL_RECORDS", 
         "COLUMN_5__TOTAL_RECORDS", 
         "COLUMN_6__TOTAL_RECORDS", 
         "COLUMN_7__TOTAL_RECORDS", 
         "COLUMN_8__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_4__POSITION_ID", 
         "COLUMN_5__POSITION_ID", 
         "COLUMN_6__POSITION_ID", 
         "COLUMN_7__POSITION_ID", 
         "COLUMN_8__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID", 
         "COLUMN_4__SEQUENCE_ID", 
         "COLUMN_5__SEQUENCE_ID", 
         "COLUMN_6__SEQUENCE_ID", 
         "COLUMN_7__SEQUENCE_ID", 
         "COLUMN_8__SEQUENCE_ID")
  
  FROM MakeColumns_37 AS in0

),

Macro_459 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'Macro_459', 'out0') }}

),

TextInput_11 AS (

  SELECT * 
  
  FROM {{ ref('seed_11')}}

),

WeightedAvg_9 AS (

  SELECT 
    (
      CASE
        WHEN (SUM("UNITS") = 0)
          THEN NULL
        ELSE (SUM((UNITCOST * "UNITS")) / SUM("UNITS"))
      END
    ) AS WEIGHTEDAVERAGE,
    "STORE NUMBER" AS "STORE NUMBER"
  
  FROM TextInput_7_cast AS in0
  
  GROUP BY "STORE NUMBER"

),

JoinMultiple_139 AS (

  SELECT 
    in0."NAME" AS "NAME",
    in1."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE",
    in0.AGE AS AGE,
    in1.SALARY AS SALARY,
    in2.MANAGER AS MANAGER,
    in2."POSITION TITLE" AS "POSITION TITLE",
    in2."NAME" AS INPUT_HASH3_NAME,
    in1."NAME" AS INPUT_HASH2_NAME
  
  FROM TextInput_137_cast AS in0
  INNER JOIN TextInput_138_cast AS in1
     ON (in0."NAME" = in1."NAME")
  INNER JOIN TextInput_135_cast AS in2
     ON (coalesce(in0.NAME, in1.NAME) = in2.NAME)

),

TextInput_11_cast AS (

  SELECT 
    "STORE NUMBER" AS "STORE NUMBER",
    "UNITS" AS "UNITS",
    CAST(UNITCOST AS FLOAT) AS UNITCOST
  
  FROM TextInput_11 AS in0

),

WeightedAvg_10 AS (

  SELECT 1 AS WEIGHTEDAVERAGE
  
  FROM TextInput_11_cast AS in0

),

Formula_179_0 AS (

  SELECT 
    100 AS "STORE NUMBER",
    20 AS "UNITS",
    *
  
  FROM WeightedAvg_10 AS in0

),

Formula_178_0 AS (

  SELECT 
    20 AS "UNITS",
    *
  
  FROM WeightedAvg_9 AS in0

),

Formula_177_0 AS (

  SELECT 
    100 AS "STORE NUMBER",
    20 AS "UNITS",
    *
  
  FROM WeightedAvg_8 AS in0

),

TextInput_1300 AS (

  SELECT * 
  
  FROM {{ ref('seed_1300')}}

),

TextInput_1300_cast AS (

  SELECT 
    COL1 AS COL1,
    WT AS WT
  
  FROM TextInput_1300 AS in0

),

WeightedAvg_14 AS (

  SELECT (
           CASE
             WHEN (SUM(WT) = 0)
               THEN NULL
             ELSE (SUM((COL1 * WT)) / SUM(WT))
           END
         ) AS WEIGHTEDAVERAGE
  
  FROM TextInput_1300_cast AS in0

),

Formula_180_0 AS (

  SELECT 
    110 AS "STORE NUMBER",
    21 AS "UNITS",
    *
  
  FROM WeightedAvg_14 AS in0

),

Union_181 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_180_0', 'Formula_179_0', 'Formula_178_0', 'Formula_177_0', 'WeightedAvg_12'], 
      [
        '[{"name": "STORE NUMBER", "dataType": "Number"}, {"name": "UNITS", "dataType": "Number"}, {"name": "WEIGHTEDAVERAGE", "dataType": "Number(38, 6)"}]', 
        '[{"name": "STORE NUMBER", "dataType": "Number"}, {"name": "UNITS", "dataType": "Number"}, {"name": "WEIGHTEDAVERAGE", "dataType": "Number"}]', 
        '[{"name": "UNITS", "dataType": "Number"}, {"name": "WEIGHTEDAVERAGE", "dataType": "Float"}, {"name": "STORE NUMBER", "dataType": "Number"}]', 
        '[{"name": "STORE NUMBER", "dataType": "Number"}, {"name": "UNITS", "dataType": "Number"}, {"name": "WEIGHTEDAVERAGE", "dataType": "Float"}]', 
        '[{"name": "WEIGHTEDAVERAGE", "dataType": "Number(38, 6)"}, {"name": "STORE NUMBER", "dataType": "Number"}, {"name": "UNITS", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_103 AS (

  SELECT DISTINCT CITY AS CITY
  
  FROM TextInput_106_cast AS in0

),

Summarize_96 AS (

  SELECT 
    (FIRST_VALUE(ZIP) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) AS FIRST_ZIP,
    (LAST_VALUE(ZIP) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) AS LAST_ZIP
  
  FROM TextInput_106_cast AS in0

),

Summarize_95 AS (

  SELECT SUM(SPEND) AS SUM_SPEND
  
  FROM TextInput_106_cast AS in0

),

Summarize_105 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((CUSTOMERID IS NULL) OR (CAST(CUSTOMERID AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS "COUNT",
    COUNT((
      CASE
        WHEN (CUSTOMERID IS NULL)
          THEN ''
        ELSE CAST(CUSTOMERID AS string)
      END
    )) AS COUNTNONNULL_CUSTOMERID,
    COUNT(DISTINCT CUSTOMERID) AS COUNTDISTINCT_CUSTOMERID,
    COUNT(DISTINCT CUSTOMERID) AS COUNTDISTINCTNONNULL_CUSTOMERID,
    COUNT((
      CASE
        WHEN (CUSTOMERID IS NULL)
          THEN 1
        ELSE NULL
      END
    )) AS COUNTNULL_CUSTOMERID
  
  FROM TextInput_106_cast AS in0

),

Summarize_91 AS (

  SELECT 
    MIN(JOINDATE) AS EARLIESTJOINDATE,
    MAX(JOINDATE) AS LATESTJOINDATE
  
  FROM TextInput_106_cast AS in0

),

Union_236 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Summarize_93', 
        'Summarize_91', 
        'Summarize_88', 
        'Summarize_100', 
        'Summarize_103', 
        'Summarize_95', 
        'Summarize_105', 
        'Summarize_89', 
        'Summarize_96', 
        'Summarize_98'
      ], 
      [
        '[{"name": "COUNT", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}]', 
        '[{"name": "EARLIESTJOINDATE", "dataType": "Timestamp"}, {"name": "LATESTJOINDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CONCAT_FIRSTNAME", "dataType": "String"}]', 
        '[{"name": "LONGEST_FIRSTNAME", "dataType": "String"}, {"name": "SHORTEST_FIRSTNAME", "dataType": "String"}, {"name": "COUNTNONBLANK_LASTNAME", "dataType": "Number"}, {"name": "COUNTBLANK_LASTNAME", "dataType": "Number"}]', 
        '[{"name": "CITY", "dataType": "String"}]', 
        '[{"name": "SUM_SPEND", "dataType": "Float"}]', 
        '[{"name": "COUNT", "dataType": "Number"}, {"name": "COUNTNONNULL_CUSTOMERID", "dataType": "Number"}, {"name": "COUNTDISTINCT_CUSTOMERID", "dataType": "Number"}, {"name": "COUNTDISTINCTNONNULL_CUSTOMERID", "dataType": "Number"}, {"name": "COUNTNULL_CUSTOMERID", "dataType": "Number"}]', 
        '[{"name": "MEDIAN_VISITS", "dataType": "Number(38, 3)"}, {"name": "SUM_SPEND", "dataType": "Float"}, {"name": "STATE", "dataType": "String"}, {"name": "CITY", "dataType": "String"}]', 
        '[{"name": "FIRST_ZIP", "dataType": "Number"}, {"name": "LAST_ZIP", "dataType": "Number"}]', 
        '[{"name": "AVG_SPEND", "dataType": "Float"}, {"name": "MEDIAN_SPEND", "dataType": "Float"}, {"name": "STDDEV_SPEND", "dataType": "Float"}, {"name": "VARIANCE_SPEND", "dataType": "Float"}, {"name": "PERCENTILE_CUSTOMERID", "dataType": "Float"}, {"name": "AVGNO0_SPEND", "dataType": "Float"}, {"name": "PCTLNO0_SPEND", "dataType": "Float"}, {"name": "MEDIANNO0_SPEND", "dataType": "Float"}, {"name": "STDDEVNO0_SPEND", "dataType": "Float"}, {"name": "VARIANCENO0_SPEND", "dataType": "Float"}, {"name": "STATE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RandomRecords_267_randOrder AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY (uniform(0, 1, random())) ASC NULLS FIRST) AS _ROW_NUMBER
  
  FROM TextInput_270_cast AS in0

),

RandomRecords_267_numRows AS (

  SELECT 
    *,
    COUNT(*) OVER (ORDER BY '1' ASC NULLS FIRST) AS _NUM_ROWS
  
  FROM RandomRecords_267_randOrder AS in0

),

RandomRecords_267 AS (

  SELECT * 
  
  FROM RandomRecords_267_numRows AS in0
  
  WHERE (_ROW_NUMBER <= ROUND((_NUM_ROWS * 0.05)))

),

RandomRecords_267_cleanup_0 AS (

  SELECT * EXCLUDE ("_ROW_NUMBER", "_NUM_ROWS")
  
  FROM RandomRecords_267 AS in0

),

RandomRecords_268_randOrder AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY (uniform(0, 1, random())) ASC NULLS FIRST) AS _ROW_NUMBER
  
  FROM TextInput_270_cast AS in0

),

RandomRecords_268 AS (

  SELECT * 
  
  FROM RandomRecords_268_randOrder AS in0
  
  WHERE (_ROW_NUMBER <= 500)

),

RandomRecords_268_cleanup_0 AS (

  SELECT * EXCLUDE ("_ROW_NUMBER")
  
  FROM RandomRecords_268 AS in0

),

Union_278 AS (

  {{
    prophecy_basics.UnionByName(
      ['RandomRecords_267_cleanup_0', 'RandomRecords_268_cleanup_0', 'RandomRecords_269_cleanup_0'], 
      [
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_280 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_278'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_159 AS (

  SELECT * 
  
  FROM {{ ref('seed_159')}}

),

TextInput_159_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST("NAME" AS STRING) AS "NAME",
    CAST(ADDRESS AS STRING) AS ADDRESS
  
  FROM TextInput_159 AS in0

),

TextToColumns_160 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextInput_159_cast'], 
      'ADDRESS', 
      "\\\\s\-", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'ADDRESS', 
      'ADDRESS', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_160_dropGem_0 AS (

  SELECT 
    GENERATEDCOLUMNNAME AS ADDRESS,
    * EXCLUDE ("GENERATEDCOLUMNNAME", "ADDRESS")
  
  FROM TextToColumns_160 AS in0

),

RecordID_259 AS (

  {{
    prophecy_basics.RecordID(
      ['TextToColumns_160_dropGem_0'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_454 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_259'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'NAME', 'ADDRESS'], 
      'NAME', 
      'VALUE', 
      ['CUSTOMERID', 'NAME', 'ADDRESS', 'RECORDID'], 
      true
    )
  }}

),

Imputationv3_69_cleanup_0 AS (

  SELECT * EXCLUDE ("_ITEM_ID_AGG", "_2012_SALES_AGG", "_2013_SALES_AGG", "_2014_SALES_AGG", "_DUMMY_COL")
  
  FROM Imputationv3_69_4 AS in0

),

Union_224_reformat_2 AS (

  SELECT 
    CAST("2012_SALES" AS FLOAT) AS "2012_SALES",
    "2012_SALES_INDICATOR" AS "2012_SALES_INDICATOR",
    CAST("2013_SALES" AS FLOAT) AS "2013_SALES",
    "2013_SALES_INDICATOR" AS "2013_SALES_INDICATOR",
    CAST("2014_SALES" AS FLOAT) AS "2014_SALES",
    "2014_SALES_INDICATOR" AS "2014_SALES_INDICATOR",
    CAST(ITEM_ID AS FLOAT) AS ITEM_ID,
    ITEM_ID_INDICATOR AS ITEM_ID_INDICATOR
  
  FROM Imputationv3_69_cleanup_0 AS in0

),

Imputationv3_67_newValueCols AS (

  SELECT 
    0 AS _ITEM_ID_AGG,
    0 AS _2012_SALES_AGG,
    0 AS _2013_SALES_AGG,
    0 AS _2014_SALES_AGG,
    '1' AS _DUMMY_COL
  
  FROM Imputationv3_67_addDummyColumn_0 AS in0

),

Imputationv3_67_join AS (

  SELECT 
    in1._2012_SALES_AGG AS _2012_SALES_AGG,
    in0.ITEM_ID AS ITEM_ID,
    in0._DUMMY_COL AS _DUMMY_COL,
    in0."2013_SALES" AS "2013_SALES",
    in1._ITEM_ID_AGG AS _ITEM_ID_AGG,
    in1._2013_SALES_AGG AS _2013_SALES_AGG,
    in0."2012_SALES" AS "2012_SALES",
    in1._2014_SALES_AGG AS _2014_SALES_AGG,
    in0."2014_SALES" AS "2014_SALES"
  
  FROM Imputationv3_67_addDummyColumn_0 AS in0
  LEFT JOIN Imputationv3_67_newValueCols AS in1
     ON (CAST(in0._DUMMY_COL AS INTEGER) = in1._DUMMY_COL)

),

Imputationv3_67_0 AS (

  SELECT 
    (
      CASE
        WHEN (ITEM_ID IS NULL)
          THEN _ITEM_ID_AGG
        ELSE ITEM_ID
      END
    ) AS ITEM_ID,
    (
      CASE
        WHEN ("2012_SALES" IS NULL)
          THEN _2012_SALES_AGG
        ELSE "2012_SALES"
      END
    ) AS "2012_SALES",
    (
      CASE
        WHEN ("2013_SALES" IS NULL)
          THEN _2013_SALES_AGG
        ELSE "2013_SALES"
      END
    ) AS "2013_SALES",
    (
      CASE
        WHEN ("2014_SALES" IS NULL)
          THEN _2014_SALES_AGG
        ELSE "2014_SALES"
      END
    ) AS "2014_SALES",
    * EXCLUDE ("ITEM_ID", "2013_SALES", "2012_SALES", "2014_SALES")
  
  FROM Imputationv3_67_join AS in0

),

Imputationv3_67_cleanup_0 AS (

  SELECT * EXCLUDE ("_ITEM_ID_AGG", "_2012_SALES_AGG", "_2013_SALES_AGG", "_2014_SALES_AGG", "_DUMMY_COL")
  
  FROM Imputationv3_67_0 AS in0

),

Union_224_reformat_0 AS (

  SELECT 
    CAST("2012_SALES" AS FLOAT) AS "2012_SALES",
    CAST("2013_SALES" AS FLOAT) AS "2013_SALES",
    CAST("2014_SALES" AS FLOAT) AS "2014_SALES",
    CAST(ITEM_ID AS FLOAT) AS ITEM_ID
  
  FROM Imputationv3_67_cleanup_0 AS in0

),

Union_224_reformat_1 AS (

  SELECT 
    CAST("2012_SALES" AS FLOAT) AS "2012_SALES",
    CAST("2012_SALES_IMPUTEDVALUE" AS FLOAT) AS "2012_SALES_IMPUTEDVALUE",
    CAST("2013_SALES" AS FLOAT) AS "2013_SALES",
    CAST("2013_SALES_IMPUTEDVALUE" AS FLOAT) AS "2013_SALES_IMPUTEDVALUE",
    CAST("2014_SALES" AS FLOAT) AS "2014_SALES",
    CAST("2014_SALES_IMPUTEDVALUE" AS FLOAT) AS "2014_SALES_IMPUTEDVALUE",
    CAST(ITEM_ID AS FLOAT) AS ITEM_ID,
    CAST(ITEM_ID_IMPUTEDVALUE AS FLOAT) AS ITEM_ID_IMPUTEDVALUE
  
  FROM Imputationv3_68_cleanup_0 AS in0

),

Union_224 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_224_reformat_0', 'Union_224_reformat_1', 'Union_224_reformat_2', 'Union_224_reformat_3'], 
      [
        '[{"name": "2012_SALES", "dataType": "Float"}, {"name": "2013_SALES", "dataType": "Float"}, {"name": "2014_SALES", "dataType": "Float"}, {"name": "ITEM_ID", "dataType": "Float"}]', 
        '[{"name": "2012_SALES", "dataType": "Float"}, {"name": "2012_SALES_IMPUTEDVALUE", "dataType": "Float"}, {"name": "2013_SALES", "dataType": "Float"}, {"name": "2013_SALES_IMPUTEDVALUE", "dataType": "Float"}, {"name": "2014_SALES", "dataType": "Float"}, {"name": "2014_SALES_IMPUTEDVALUE", "dataType": "Float"}, {"name": "ITEM_ID", "dataType": "Float"}, {"name": "ITEM_ID_IMPUTEDVALUE", "dataType": "Float"}]', 
        '[{"name": "2012_SALES", "dataType": "Float"}, {"name": "2012_SALES_INDICATOR", "dataType": "Number"}, {"name": "2013_SALES", "dataType": "Float"}, {"name": "2013_SALES_INDICATOR", "dataType": "Number"}, {"name": "2014_SALES", "dataType": "Float"}, {"name": "2014_SALES_INDICATOR", "dataType": "Number"}, {"name": "ITEM_ID", "dataType": "Float"}, {"name": "ITEM_ID_INDICATOR", "dataType": "Number"}]', 
        '[{"name": "2012_SALES", "dataType": "Float"}, {"name": "2013_SALES", "dataType": "Float"}, {"name": "2014_SALES", "dataType": "Float"}, {"name": "ITEM_ID", "dataType": "Float"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_226 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_224'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_227 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_226'], 
      ['RECORDID'], 
      [
        'ITEM_ID', 
        '2012_SALES', 
        '2013_SALES', 
        '2014_SALES', 
        'ITEM_ID_IMPUTEDVALUE', 
        '2012_SALES_IMPUTEDVALUE', 
        '2013_SALES_IMPUTEDVALUE', 
        '2014_SALES_IMPUTEDVALUE', 
        'ITEM_ID_INDICATOR', 
        '2012_SALES_INDICATOR', 
        '2013_SALES_INDICATOR', 
        '2014_SALES_INDICATOR'
      ], 
      'NAME', 
      'VALUE', 
      [
        '"2013_SALES_INDICATOR"', 
        'ITEM_ID_INDICATOR', 
        'ITEM_ID', 
        '"2013_SALES_IMPUTEDVALUE"', 
        '"2012_SALES_IMPUTEDVALUE"', 
        'RECORDID', 
        '"2014_SALES"', 
        '"2012_SALES_INDICATOR"', 
        '"2014_SALES_INDICATOR"', 
        '"2014_SALES_IMPUTEDVALUE"', 
        '"2013_SALES"', 
        'ITEM_ID_IMPUTEDVALUE', 
        '"2012_SALES"'
      ], 
      true
    )
  }}

),

Formula_228_0 AS (

  SELECT 
    CAST('imputation' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_227 AS in0

),

JoinMultiple_134 AS (

  SELECT 
    in0."NAME" AS "NAME",
    in1."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE",
    in0.AGE AS AGE,
    in1.SALARY AS SALARY,
    in2.MANAGER AS MANAGER,
    in2."POSITION TITLE" AS "POSITION TITLE",
    in2."NAME" AS INPUT_HASH3_NAME,
    in1."NAME" AS INPUT_HASH2_NAME
  
  FROM TextInput_137_cast AS in0
  FULL JOIN TextInput_138_cast AS in1
     ON (in0."NAME" = in1."NAME")
  FULL JOIN TextInput_135_cast AS in2
     ON (coalesce(in0.NAME, in1.NAME) = in2.NAME)

),

JoinMultiple_143_in1 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_138_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_1', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

JoinMultiple_143 AS (

  SELECT 
    in0."NAME" AS "NAME",
    in1."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE",
    in0.AGE AS AGE,
    in1.SALARY AS SALARY,
    in2.MANAGER AS MANAGER,
    in2."POSITION TITLE" AS "POSITION TITLE",
    in2."NAME" AS INPUT_HASH3_NAME,
    in1."NAME" AS INPUT_HASH2_NAME
  
  FROM JoinMultiple_143_in0 AS in0
  INNER JOIN JoinMultiple_143_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)
  INNER JOIN JoinMultiple_143_in2 AS in2
     ON (coalesce(in0.RECORDPOSITIONFORJOIN_0, in1.RECORDPOSITIONFORJOIN_1) = in2.RECORDPOSITIONFORJOIN_2)

),

JoinMultiple_136 AS (

  SELECT 
    in0."NAME" AS "NAME",
    in1."NUMBER OF YEARS IN CURRENT POSITION" AS "NUMBER OF YEARS IN CURRENT POSITION",
    in0."NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE" AS "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE",
    in0.AGE AS AGE,
    in1.SALARY AS SALARY,
    in1."NAME" AS INPUT_HASH2_NAME
  
  FROM TextInput_137_cast AS in0
  FULL JOIN TextInput_138_cast AS in1
     ON (in0."NAME" = in1."NAME")

),

Union_247 AS (

  {{
    prophecy_basics.UnionByName(
      ['JoinMultiple_139', 'JoinMultiple_136', 'JoinMultiple_142', 'JoinMultiple_143', 'JoinMultiple_134'], 
      [
        '[{"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Integer"}, {"name": "NAME", "dataType": "String"}, {"name": "POSITION TITLE", "dataType": "String"}, {"name": "INPUT_HASH3_NAME", "dataType": "String"}, {"name": "AGE", "dataType": "Integer"}, {"name": "INPUT_HASH2_NAME", "dataType": "String"}, {"name": "SALARY", "dataType": "Integer"}, {"name": "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE", "dataType": "Integer"}, {"name": "MANAGER", "dataType": "String"}]', 
        '[{"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Integer"}, {"name": "NAME", "dataType": "String"}, {"name": "AGE", "dataType": "Integer"}, {"name": "INPUT_HASH2_NAME", "dataType": "String"}, {"name": "SALARY", "dataType": "Integer"}, {"name": "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE", "dataType": "Integer"}]', 
        '[{"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Integer"}, {"name": "NAME", "dataType": "String"}, {"name": "POSITION TITLE", "dataType": "String"}, {"name": "INPUT_HASH3_NAME", "dataType": "String"}, {"name": "AGE", "dataType": "Integer"}, {"name": "INPUT_HASH2_NAME", "dataType": "String"}, {"name": "SALARY", "dataType": "Integer"}, {"name": "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE", "dataType": "Integer"}, {"name": "MANAGER", "dataType": "String"}]', 
        '[{"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Integer"}, {"name": "NAME", "dataType": "String"}, {"name": "POSITION TITLE", "dataType": "String"}, {"name": "INPUT_HASH3_NAME", "dataType": "String"}, {"name": "AGE", "dataType": "Integer"}, {"name": "INPUT_HASH2_NAME", "dataType": "String"}, {"name": "SALARY", "dataType": "Integer"}, {"name": "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE", "dataType": "Integer"}, {"name": "MANAGER", "dataType": "String"}]', 
        '[{"name": "NUMBER OF YEARS IN CURRENT POSITION", "dataType": "Integer"}, {"name": "NAME", "dataType": "String"}, {"name": "POSITION TITLE", "dataType": "String"}, {"name": "INPUT_HASH3_NAME", "dataType": "String"}, {"name": "AGE", "dataType": "Integer"}, {"name": "INPUT_HASH2_NAME", "dataType": "String"}, {"name": "SALARY", "dataType": "Integer"}, {"name": "NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE", "dataType": "Integer"}, {"name": "MANAGER", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_249 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_247'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_250 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_249'], 
      ['RECORDID'], 
      [
        'NAME', 
        'AGE', 
        'NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE', 
        'INPUT_HASH2_NAME', 
        'NUMBER OF YEARS IN CURRENT POSITION', 
        'SALARY', 
        'INPUT_HASH3_NAME', 
        'POSITION TITLE', 
        'MANAGER'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'NUMBER OF YEARS IN CURRENT POSITION', 
        'NAME', 
        'POSITION TITLE', 
        'INPUT_HASH3_NAME', 
        'AGE', 
        'INPUT_HASH2_NAME', 
        'SALARY', 
        'NUMBER OF YEARS OF EDUCATION PARANTHESESOPENSTARTING AT 1ST GRADEPARANTHESESCLOSE', 
        'MANAGER'
      ], 
      true
    )
  }}

),

MakeColumns_33 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_33') }}

),

MakeColumns_33_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_4__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_4__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID", 
         "COLUMN_4__SEQUENCE_ID")
  
  FROM MakeColumns_33 AS in0

),

MakeColumns_36_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_4__TOTAL_RECORDS", 
         "COLUMN_5__TOTAL_RECORDS", 
         "COLUMN_6__TOTAL_RECORDS", 
         "COLUMN_7__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_4__POSITION_ID", 
         "COLUMN_5__POSITION_ID", 
         "COLUMN_6__POSITION_ID", 
         "COLUMN_7__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID", 
         "COLUMN_4__SEQUENCE_ID", 
         "COLUMN_5__SEQUENCE_ID", 
         "COLUMN_6__SEQUENCE_ID", 
         "COLUMN_7__SEQUENCE_ID")
  
  FROM MakeColumns_36 AS in0

),

MakeColumns_34 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_34') }}

),

MakeColumns_34_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_4__TOTAL_RECORDS", 
         "COLUMN_5__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_4__POSITION_ID", 
         "COLUMN_5__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID", 
         "COLUMN_4__SEQUENCE_ID", 
         "COLUMN_5__SEQUENCE_ID")
  
  FROM MakeColumns_34 AS in0

),

MakeColumns_32 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_32') }}

),

MakeColumns_32_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID")
  
  FROM MakeColumns_32 AS in0

),

MakeColumns_35 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'MakeColumns_35') }}

),

MakeColumns_35_dropCols_0 AS (

  SELECT * EXCLUDE ("_GROUP_ID", 
         "COLUMN_1__TOTAL_RECORDS", 
         "COLUMN_2__TOTAL_RECORDS", 
         "COLUMN_3__TOTAL_RECORDS", 
         "COLUMN_4__TOTAL_RECORDS", 
         "COLUMN_5__TOTAL_RECORDS", 
         "COLUMN_6__TOTAL_RECORDS", 
         "COLUMN_1__POSITION_ID", 
         "COLUMN_2__POSITION_ID", 
         "COLUMN_3__POSITION_ID", 
         "COLUMN_4__POSITION_ID", 
         "COLUMN_5__POSITION_ID", 
         "COLUMN_6__POSITION_ID", 
         "COLUMN_1__SEQUENCE_ID", 
         "COLUMN_2__SEQUENCE_ID", 
         "COLUMN_3__SEQUENCE_ID", 
         "COLUMN_4__SEQUENCE_ID", 
         "COLUMN_5__SEQUENCE_ID", 
         "COLUMN_6__SEQUENCE_ID")
  
  FROM MakeColumns_35 AS in0

),

Union_196 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'MakeColumns_36_dropCols_0', 
        'MakeColumns_33_dropCols_0', 
        'MakeColumns_35_dropCols_0', 
        'MakeColumns_34_dropCols_0', 
        'MakeColumns_32_dropCols_0', 
        'MakeColumns_37_dropCols_0'
      ], 
      [
        '[{"name": "COLUMN_7_UNITCOST", "dataType": "Float"}, {"name": "UNITS", "dataType": "Number"}, {"name": "COLUMN_5_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITCOST", "dataType": "Float"}, {"name": "STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_6_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_4_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_3_UNITCOST", "dataType": "Float"}]', 
        '[{"name": "COLUMN_4_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_1_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_2_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_1_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_4_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITCOST", "dataType": "Float"}]', 
        '[{"name": "COLUMN_5_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITCOST", "dataType": "Float"}, {"name": "STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_6_UNITS", "dataType": "Number"}, {"name": "COLUMN_5_UNITS", "dataType": "Number"}, {"name": "COLUMN_6_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITCOST", "dataType": "Float"}]', 
        '[{"name": "COLUMN_5_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITCOST", "dataType": "Float"}, {"name": "STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_5_UNITS", "dataType": "Number"}, {"name": "COLUMN_2_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_1_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITCOST", "dataType": "Float"}]', 
        '[{"name": "COLUMN_1_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_2_UNITCOST", "dataType": "Float"}, {"name": "COLUMN_2_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITS", "dataType": "Number"}, {"name": "COLUMN_1_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_1_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_3_UNITCOST", "dataType": "Float"}]', 
        '[{"name": "COLUMN_7_UNITS", "dataType": "Number"}, {"name": "UNITCOST", "dataType": "Float"}, {"name": "STORE NUMBER", "dataType": "Number"}, {"name": "COLUMN_6_UNITS", "dataType": "Number"}, {"name": "COLUMN_8_UNITS", "dataType": "Number"}, {"name": "COLUMN_5_UNITS", "dataType": "Number"}, {"name": "COLUMN_2_UNITS", "dataType": "Number"}, {"name": "COLUMN_3_UNITS", "dataType": "Number"}, {"name": "COLUMN_1_UNITS", "dataType": "Number"}, {"name": "COLUMN_4_UNITS", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_198 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_196'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_199 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_198'], 
      ['RECORDID'], 
      [
        'COLUMN_1_STORE NUMBER', 
        'COLUMN_1_UNITS', 
        'COLUMN_1_UNITCOST', 
        'COLUMN_2_STORE NUMBER', 
        'COLUMN_2_UNITS', 
        'COLUMN_2_UNITCOST', 
        'COLUMN_3_STORE NUMBER', 
        'COLUMN_3_UNITS', 
        'COLUMN_3_UNITCOST', 
        'COLUMN_4_STORE NUMBER', 
        'COLUMN_4_UNITS', 
        'COLUMN_4_UNITCOST', 
        'STORE NUMBER', 
        'COLUMN_5_UNITS', 
        'COLUMN_5_UNITCOST', 
        'COLUMN_6_UNITS', 
        'COLUMN_6_UNITCOST', 
        'UNITS', 
        'COLUMN_7_UNITCOST', 
        'UNITCOST', 
        'COLUMN_7_UNITS', 
        'COLUMN_8_UNITS'
      ], 
      'NAME', 
      'VALUE', 
      [
        'COLUMN_4_STORE NUMBER', 
        'COLUMN_7_UNITCOST', 
        'UNITS', 
        'COLUMN_7_UNITS', 
        'COLUMN_5_UNITCOST', 
        'COLUMN_1_UNITCOST', 
        'COLUMN_2_STORE NUMBER', 
        'RECORDID', 
        'COLUMN_2_UNITCOST', 
        'UNITCOST', 
        'STORE NUMBER', 
        'COLUMN_6_UNITS', 
        'COLUMN_8_UNITS', 
        'COLUMN_5_UNITS', 
        'COLUMN_6_UNITCOST', 
        'COLUMN_2_UNITS', 
        'COLUMN_3_UNITS', 
        'COLUMN_4_UNITCOST', 
        'COLUMN_1_STORE NUMBER', 
        'COLUMN_1_UNITS', 
        'COLUMN_3_STORE NUMBER', 
        'COLUMN_4_UNITS', 
        'COLUMN_3_UNITCOST'
      ], 
      true
    )
  }}

),

Formula_209_0 AS (

  SELECT 
    CAST('makeCols' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_199 AS in0

),

Union_283_reformat_7 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_209_0 AS in0

),

RecordID_191 AS (

  {{
    prophecy_basics.RecordID(
      ['Base64Encoder_28_0'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_192 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_191'], 
      ['RECORDID'], 
      ['FIELD1', 'BASE64_ENCODED'], 
      'NAME', 
      'VALUE', 
      ['FIELD1', 'BASE64_ENCODED', 'RECORDID'], 
      true
    )
  }}

),

Formula_207_0 AS (

  SELECT 
    CAST('base64' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_192 AS in0

),

Formula_251_0 AS (

  SELECT 
    CAST('join multiple' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_250 AS in0

),

Union_283_reformat_13 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_251_0 AS in0

),

RecordID_194 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_182'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_195 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_194'], 
      ['RECORDID'], 
      ['COUNT'], 
      'NAME', 
      'VALUE', 
      ['COUNT', 'RECORDID'], 
      true
    )
  }}

),

Formula_205_0 AS (

  SELECT 
    CAST('count records' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_195 AS in0

),

Union_283_reformat_3 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_205_0 AS in0

),

MultiFieldBinningv2_64_cleanup_0 AS (

  SELECT * EXCLUDE ("_DUMMY_COL", "_MIN_FIELD1", "_MAX_FIELD1", "_MIN_FIELD2", "_MAX_FIELD2", "_MIN_FIELD3", "_MAX_FIELD3")
  
  FROM MultiFieldBinningv2_64_0 AS in0

),

Union_219 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'MultiFieldBinningv2_60_cleanup_0', 
        'MultiFieldBinningv2_62_cleanup_0', 
        'MultiFieldBinningv2_63_cleanup_0', 
        'MultiFieldBinningv2_64_cleanup_0'
      ], 
      [
        '[{"name": "FIELD1_TILE_NUM", "dataType": "Number"}, {"name": "FIELD2_TILE_NUM", "dataType": "Number"}, {"name": "FIELD3_TILE_NUM", "dataType": "Number"}, {"name": "FIELD1", "dataType": "Boolean"}, {"name": "FIELD3", "dataType": "Number"}, {"name": "FIELD2", "dataType": "Float"}]', 
        '[{"name": "FIELD1_TILE_NUM", "dataType": "Number"}, {"name": "FIELD2_TILE_NUM", "dataType": "Number"}, {"name": "FIELD3_TILE_NUM", "dataType": "Number"}, {"name": "FIELD1", "dataType": "Boolean"}, {"name": "FIELD3", "dataType": "Number"}, {"name": "FIELD2", "dataType": "Float"}]', 
        '[{"name": "FIELD1_TILE_NUM", "dataType": "Number"}, {"name": "FIELD2_TILE_NUM", "dataType": "Number"}, {"name": "FIELD3_TILE_NUM", "dataType": "Number"}, {"name": "FIELD1", "dataType": "Boolean"}, {"name": "FIELD3", "dataType": "Number"}, {"name": "FIELD2", "dataType": "Float"}]', 
        '[{"name": "FIELD1_TILE_NUM", "dataType": "Number"}, {"name": "FIELD2_TILE_NUM", "dataType": "Number"}, {"name": "FIELD3_TILE_NUM", "dataType": "Number"}, {"name": "FIELD1", "dataType": "Boolean"}, {"name": "FIELD3", "dataType": "Number"}, {"name": "FIELD2", "dataType": "Float"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_221 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_219'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_222 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_221'], 
      ['RECORDID'], 
      ['FIELD1', 'FIELD2', 'FIELD3', 'FIELD1_TILE_NUM', 'FIELD2_TILE_NUM', 'FIELD3_TILE_NUM'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'FIELD1_TILE_NUM', 'FIELD2_TILE_NUM', 'FIELD3_TILE_NUM', 'FIELD1', 'FIELD3', 'FIELD2'], 
      true
    )
  }}

),

Formula_223_0 AS (

  SELECT 
    CAST('multi field binning weird' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_222 AS in0

),

Union_283_reformat_4 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_223_0 AS in0

),

Macro_459 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('alteryx_all_test_sql', 'Macro_459', 'out1') }}

),

Union_283_reformat_2 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Macro_459 AS in0

),

Union_283_reformat_6 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_207_0 AS in0

),

Formula_277_0 AS (

  SELECT 
    CAST('oversample' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_276 AS in0

),

Union_283_reformat_15 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_277_0 AS in0

),

Union_283_reformat_12 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_246_0 AS in0

),

RecordID_237 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_236'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_238 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_237'], 
      ['RECORDID'], 
      [
        'SUM_SPEND', 
        'LONGEST_FIRSTNAME', 
        'SHORTEST_FIRSTNAME', 
        'COUNTNONBLANK_LASTNAME', 
        'COUNTBLANK_LASTNAME', 
        'EARLIESTJOINDATE', 
        'LATESTJOINDATE', 
        'COUNT', 
        'COUNTNONNULL_CUSTOMERID', 
        'COUNTDISTINCT_CUSTOMERID', 
        'COUNTDISTINCTNONNULL_CUSTOMERID', 
        'COUNTNULL_CUSTOMERID', 
        'CITY', 
        'CONCAT_FIRSTNAME', 
        'STATE', 
        'MEDIAN_VISITS', 
        'FIRST_ZIP', 
        'LAST_ZIP', 
        'AVG_SPEND', 
        'MEDIAN_SPEND', 
        'STDDEV_SPEND', 
        'VARIANCE_SPEND', 
        'PERCENTILE_CUSTOMERID', 
        'AVGNO0_SPEND', 
        'PCTLNO0_SPEND', 
        'MEDIANNO0_SPEND', 
        'STDDEVNO0_SPEND', 
        'VARIANCENO0_SPEND'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'COUNT', 
        'CITY', 
        'EARLIESTJOINDATE', 
        'LATESTJOINDATE', 
        'CONCAT_FIRSTNAME', 
        'LONGEST_FIRSTNAME', 
        'SHORTEST_FIRSTNAME', 
        'COUNTNONBLANK_LASTNAME', 
        'COUNTBLANK_LASTNAME', 
        'SUM_SPEND', 
        'COUNTNONNULL_CUSTOMERID', 
        'COUNTDISTINCT_CUSTOMERID', 
        'COUNTDISTINCTNONNULL_CUSTOMERID', 
        'COUNTNULL_CUSTOMERID', 
        'MEDIAN_VISITS', 
        'STATE', 
        'FIRST_ZIP', 
        'LAST_ZIP', 
        'AVG_SPEND', 
        'MEDIAN_SPEND', 
        'STDDEV_SPEND', 
        'VARIANCE_SPEND', 
        'PERCENTILE_CUSTOMERID', 
        'AVGNO0_SPEND', 
        'PCTLNO0_SPEND', 
        'MEDIANNO0_SPEND', 
        'STDDEVNO0_SPEND', 
        'VARIANCENO0_SPEND'
      ], 
      true
    )
  }}

),

Formula_239_0 AS (

  SELECT 
    CAST('summarize' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_238 AS in0

),

Union_283_reformat_11 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_239_0 AS in0

),

Union_283_reformat_17 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_305_0 AS in0

),

Transpose_217 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_216'], 
      ['RECORDID'], 
      [
        'EMPLOYEE', 
        'ENTERPRISE SALES 2015', 
        'COMMERCIAL SALES 2015', 
        'OTHER SALES 2015', 
        'NUMBER OF YEARS IN CURRENT POSITION', 
        '401K CONTRIBUTION FOR 2015', 
        'ENTERPRISE SALES 2015_TILE_NUM', 
        'COMMERCIAL SALES 2015_TILE_NUM'
      ], 
      'NAME', 
      'VALUE', 
      [
        'ENTERPRISE SALES 2015_TILE_NUM', 
        'NUMBER OF YEARS IN CURRENT POSITION', 
        'OTHER SALES 2015', 
        'EMPLOYEE', 
        'COMMERCIAL SALES 2015_TILE_NUM', 
        'RECORDID', 
        '"401K CONTRIBUTION FOR 2015"', 
        'COMMERCIAL SALES 2015', 
        'ENTERPRISE SALES 2015'
      ], 
      true
    )
  }}

),

Formula_218_0 AS (

  SELECT 
    CAST('multi field binning' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_217 AS in0

),

Union_283_reformat_9 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_218_0 AS in0

),

RecordID_184 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_181'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_189 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_184'], 
      ['RECORDID'], 
      ['WEIGHTEDAVERAGE', 'STORE NUMBER', 'UNITS'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'STORE NUMBER', 'UNITS', 'WEIGHTEDAVERAGE'], 
      true
    )
  }}

),

Formula_203_0 AS (

  SELECT 
    CAST('weighted avg' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_189 AS in0

),

Union_283_reformat_5 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_203_0 AS in0

),

Union_283_reformat_1 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Macro_459 AS in0

),

Formula_261_0 AS (

  SELECT 
    CAST('text to columns' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_454 AS in0

),

Union_283_reformat_14 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_261_0 AS in0

),

Transpose_281 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_280'], 
      ['RECORDID'], 
      ['ADDRESS', 'CITY', 'STATE', 'ZIP', 'COMPANY', 'AVERAGE SALE'], 
      'NAME', 
      'VALUE', 
      ['ADDRESS', 'ZIP', 'CITY', 'RECORDID', 'COMPANY', 'AVERAGE SALE', 'STATE'], 
      true
    )
  }}

),

Formula_282_0 AS (

  SELECT 
    CAST('random sample' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_281 AS in0

),

Union_283_reformat_16 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_282_0 AS in0

),

Union_283_reformat_10 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Formula_228_0 AS in0

),

Union_283_reformat_0 AS (

  SELECT 
    "NAME" AS "NAME",
    CAST(RECORDID AS STRING) AS RECORDID,
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Macro_455 AS in0

),

Union_283 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Union_283_reformat_13', 
        'Union_283_reformat_3', 
        'Union_283_reformat_7', 
        'Union_283_reformat_10', 
        'Union_283_reformat_14', 
        'Union_283_reformat_8', 
        'Union_283_reformat_6', 
        'Union_283_reformat_2', 
        'Union_283_reformat_15', 
        'Union_283_reformat_11', 
        'Union_283_reformat_1', 
        'Union_283_reformat_16', 
        'Union_283_reformat_9', 
        'Union_283_reformat_0', 
        'Union_283_reformat_12', 
        'Union_283_reformat_17', 
        'Union_283_reformat_4', 
        'Union_283_reformat_5'
      ], 
      [
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
        '[{"name": "NAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_283
