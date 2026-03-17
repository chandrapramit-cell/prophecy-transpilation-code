{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH C1071032_Centra_66 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'C1071032_Centra_66') }}

),

Filter_84 AS (

  SELECT * 
  
  FROM C1071032_Centra_66 AS in0
  
  WHERE (NOT(`Inquiry hashslashChange Request hash` IS NULL))

),

Cleanse_73 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Filter_84'], 
      [
        { "name": "Change From", "dataType": "Double" }, 
        { "name": "InquiryslashRequest Description", "dataType": "String" }, 
        { "name": "Expected Vendor Action", "dataType": "Double" }, 
        { "name": "Change To", "dataType": "Double" }, 
        { "name": "Response Needed By Date", "dataType": "Date" }, 
        { "name": "Subject ID hash", "dataType": "Double" }, 
        { "name": "Collection Date in CRF paranthesesOpenIf AvailableparanthesesClose", "dataType": "Double" }, 
        {
          "name": "Expectedslash Response Date  paranthesesOpendd-Mmm-yyyyparanthesesClose", 
          "dataType": "Double"
        }, 
        { "name": "Priority paranthesesOpenHighcomma Mediumcomma LowparanthesesClose", "dataType": "String" }, 
        { "name": "Requested By", "dataType": "String" }, 
        { "name": "Data Type", "dataType": "Double" }, 
        {
          "name": "Type of Request paranthesesOpenInquiryslashChange RequestparanthesesClose", 
          "dataType": "String"
        }, 
        { "name": "Status 2", "dataType": "String" }, 
        { "name": "Visit", "dataType": "String" }, 
        { "name": "Request Date  paranthesesOpendd-Mmm-yyyyparanthesesClose", "dataType": "Date" }, 
        { "name": "Response or Resolution Details", "dataType": "String" }, 
        { "name": "DM Comments", "dataType": "String" }, 
        { "name": "Site ID", "dataType": "String" }, 
        { "name": "FileName", "dataType": "String" }, 
        { "name": "Inquiry hashslashChange Request hash", "dataType": "Double" }, 
        { "name": "Accession hash  paranthesesOpenIf ApplicableparanthesesClose", "dataType": "String" }, 
        { "name": "Response Provided By", "dataType": "Double" }, 
        { "name": "Additional_Identifier_e__g___Lab_ID_Test_Name", "dataType": "Double" }, 
        { "name": "Collection Date in eData paranthesesOpenOptionalparanthesesClose", "dataType": "String" }, 
        { "name": "Status", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [], 
      false, 
      '', 
      false, 
      0, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      true, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_68_to_Formula_76_0 AS (

  SELECT 
    CAST('C1071032' AS string) AS study_id,
    CAST((REVERSE(FileName)) AS string) AS report_run_date,
    *
  
  FROM Cleanse_73 AS in0

),

Formula_68_to_Formula_76_1 AS (

  SELECT 
    CAST((
      SUBSTRING(
        report_run_date, 
        1, 
        (
          (
            CASE
              WHEN ((INSTR(report_run_date, '_')) = 0)
                THEN NULL
              ELSE (INSTR(report_run_date, '_'))
            END
          )
          - 1
        ))
    ) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_68_to_Formula_76_0 AS in0

),

Formula_68_to_Formula_76_2 AS (

  SELECT 
    CAST((REVERSE(report_run_date)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_68_to_Formula_76_1 AS in0

),

DateTime_77_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(report_run_date, 'ddMMMyyyy')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM Formula_68_to_Formula_76_2 AS in0

),

AlteryxSelect_80 AS (

  SELECT 
    study_id AS study_id,
    FileName AS FileName,
    DateTime_Out AS report_run_date,
    `Inquiry hashslashChange Request hash` AS `Inquiry hashslashChange Request hash`,
    `Requested By` AS `Requested By`,
    `Site ID` AS `Site ID`,
    `Subject ID hash` AS `Subject ID hash`,
    Visit AS Visit,
    `Data Type` AS `Data Type`,
    `Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS `Collection Date in eData paranthesesOpenOptionalparanthesesClose`,
    `InquiryslashRequest Description` AS `InquiryslashRequest Description`,
    `Expected Vendor Action` AS `Expected Vendor Action`,
    `Response or Resolution Details` AS `Response or Resolution Details`,
    Status AS Status,
    `Response Provided By` AS `Response Provided By`,
    `DM Comments` AS `DM Comments`,
    `Status 2` AS `Status 2`,
    CAST(NULL AS string) AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    CAST(NULL AS string) AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    CAST(NULL AS string) AS `Response Needed By
    Date`,
    CAST(NULL AS string) AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    CAST(NULL AS string) AS `Accession hash 
    paranthesesOpenIf ApplicableparanthesesClose`,
    CAST(NULL AS string) AS Additional_Identifiere__g___Lab_ID_Test_Name,
    CAST(NULL AS string) AS `Collection Date in CRF
    paranthesesOpenIf AvailableparanthesesClose`,
    CAST(NULL AS string) AS `Change
    From`,
    CAST(NULL AS string) AS `Change
    To`,
    CAST(NULL AS string) AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`
  
  FROM DateTime_77_0 AS in0

),

Union_69_1 AS (

  SELECT 
    CAST(`Requested By` AS string) AS prophecy_column_5,
    CAST(`Site ID` AS string) AS prophecy_column_10,
    CAST(`Expectedslash Response Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS DOUBLE) AS prophecy_column_24,
    CAST(`Response Provided By` AS DOUBLE) AS prophecy_column_25,
    CAST(`Data Type` AS string) AS prophecy_column_14,
    CAST(`Change From` AS STRING) AS prophecy_column_20,
    CAST(study_id AS string) AS prophecy_column_1,
    CAST(`Request Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS STRING) AS prophecy_column_6,
    CAST(`Change To` AS STRING) AS prophecy_column_21,
    CAST(`Type of Request paranthesesOpenInquiryslashChange RequestparanthesesClose` AS STRING) AS prophecy_column_9,
    CAST(`Accession hash paranthesesOpenIf ApplicableparanthesesClose` AS STRING) AS prophecy_column_13,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS string) AS prophecy_column_17,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_22,
    CAST(`Status 2` AS string) AS prophecy_column_27,
    CAST(Visit AS string) AS prophecy_column_12,
    CAST(`Priority paranthesesOpenHighcomma Mediumcomma LowparanthesesClose` AS STRING) AS prophecy_column_7,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(`InquiryslashRequest Description` AS string) AS prophecy_column_18,
    CAST(`Collection Date in CRF paranthesesOpenIf AvailableparanthesesClose` AS STRING) AS prophecy_column_16,
    CAST(`Subject ID hash` AS string) AS prophecy_column_11,
    CAST(`DM Comments` AS string) AS prophecy_column_26,
    CAST(Status AS string) AS prophecy_column_23,
    CAST(`Response Needed By Date` AS STRING) AS prophecy_column_8,
    CAST(`Expected Vendor Action` AS string) AS prophecy_column_19,
    CAST(`Inquiry hashslashChange Request hash` AS DOUBLE) AS prophecy_column_4,
    CAST(Additional_Identifier_e__g___Lab_ID_Test_Name AS STRING) AS prophecy_column_15
  
  FROM AlteryxSelect_80 AS in0

),

AlteryxSelect_34 AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__AlteryxSelect_34')}}

),

Union_69_0 AS (

  SELECT 
    CAST(`Requested By` AS string) AS prophecy_column_5,
    CAST(`Site ID` AS string) AS prophecy_column_10,
    CAST(`Expectedslash Response Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS DOUBLE) AS prophecy_column_24,
    CAST(`Response Provided By` AS DOUBLE) AS prophecy_column_25,
    CAST(`Data Type` AS string) AS prophecy_column_14,
    CAST(`Change From` AS STRING) AS prophecy_column_20,
    CAST(study_id AS string) AS prophecy_column_1,
    CAST(`Request Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS STRING) AS prophecy_column_6,
    CAST(`Change To` AS STRING) AS prophecy_column_21,
    CAST(`Type of Request paranthesesOpenInquiryslashChange RequestparanthesesClose` AS STRING) AS prophecy_column_9,
    CAST(`Accession hash paranthesesOpenIf ApplicableparanthesesClose` AS STRING) AS prophecy_column_13,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS string) AS prophecy_column_17,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_22,
    CAST(`Status 2` AS string) AS prophecy_column_27,
    CAST(Visit AS string) AS prophecy_column_12,
    CAST(`Priority paranthesesOpenHighcomma Mediumcomma LowparanthesesClose` AS STRING) AS prophecy_column_7,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(`InquiryslashRequest Description` AS string) AS prophecy_column_18,
    CAST(`Collection Date in CRF paranthesesOpenIf AvailableparanthesesClose` AS STRING) AS prophecy_column_16,
    CAST(`Subject ID hash` AS string) AS prophecy_column_11,
    CAST(`DM Comments` AS string) AS prophecy_column_26,
    CAST(Status AS string) AS prophecy_column_23,
    CAST(`Response Needed By Date` AS STRING) AS prophecy_column_8,
    CAST(`Expected Vendor Action` AS string) AS prophecy_column_19,
    CAST(`Inquiry hashslashChange Request hash` AS DOUBLE) AS prophecy_column_4,
    CAST(Additional_Identifier_e__g___Lab_ID_Test_Name AS STRING) AS prophecy_column_15
  
  FROM AlteryxSelect_34 AS in0

),

Union_69 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_69_0', 'Union_69_1'], 
      [
        '[{"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "Double"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]', 
        '[{"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "Double"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_69_postRename AS (

  SELECT 
    prophecy_column_13 AS `Accession hash 
    paranthesesOpenIf ApplicableparanthesesClose`,
    prophecy_column_20 AS `Change
    From`,
    prophecy_column_19 AS `Expected Vendor Action`,
    prophecy_column_17 AS `Collection Date in eData paranthesesOpenOptionalparanthesesClose`,
    prophecy_column_1 AS study_id,
    prophecy_column_3 AS report_run_date,
    prophecy_column_16 AS `Collection Date in CRF
    paranthesesOpenIf AvailableparanthesesClose`,
    prophecy_column_5 AS `Requested By`,
    prophecy_column_14 AS `Data Type`,
    prophecy_column_7 AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    prophecy_column_27 AS `Status 2`,
    prophecy_column_12 AS Visit,
    prophecy_column_18 AS `InquiryslashRequest Description`,
    prophecy_column_22 AS `Response or Resolution Details`,
    prophecy_column_4 AS `Inquiry hashslashChange Request hash`,
    prophecy_column_11 AS `Subject ID hash`,
    prophecy_column_26 AS `DM Comments`,
    prophecy_column_15 AS Additional_Identifiere__g___Lab_ID_Test_Name,
    prophecy_column_21 AS `Change
    To`,
    prophecy_column_8 AS `Response Needed By
    Date`,
    prophecy_column_10 AS `Site ID`,
    prophecy_column_2 AS FileName,
    prophecy_column_6 AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    prophecy_column_25 AS `Response Provided By`,
    prophecy_column_24 AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    prophecy_column_9 AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    prophecy_column_23 AS Status
  
  FROM Union_69 AS in0

)

SELECT *

FROM Union_69_postRename
