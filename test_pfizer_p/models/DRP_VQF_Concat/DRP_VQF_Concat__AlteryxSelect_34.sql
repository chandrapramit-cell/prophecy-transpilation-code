{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH C1071007_LB003C_2 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'C1071007_LB003C_2') }}

),

Union_58_0 AS (

  SELECT 
    CAST(`Status 2` AS string) AS prophecy_column_24,
    CAST(Assignment AS string) AS prophecy_column_25,
    CAST(`Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS string) AS prophecy_column_14,
    CAST(Status AS string) AS prophecy_column_20,
    CAST(`Inquiry hashslashChange Request hash` AS DOUBLE) AS prophecy_column_1,
    CAST(FileName AS string) AS prophecy_column_28,
    CAST(Visit AS string) AS prophecy_column_9,
    CAST(`Requested By` AS string) AS prophecy_column_2,
    CAST(`Response Provided By` AS DOUBLE) AS prophecy_column_22,
    CAST(F27 AS DOUBLE) AS prophecy_column_27,
    CAST(`Site ID` AS string) AS prophecy_column_7,
    CAST(`Expected Vendor Action` AS string) AS prophecy_column_16,
    CAST(`Data Type` AS string) AS prophecy_column_11,
    CAST(F26 AS DOUBLE) AS prophecy_column_26,
    CAST(`DM Comments` AS string) AS prophecy_column_23,
    CAST(`Subject ID hash` AS string) AS prophecy_column_8,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_19,
    CAST(`InquiryslashRequest Description` AS string) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_5,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_21,
    CAST(NULL AS string) AS prophecy_column_13,
    CAST(NULL AS string) AS prophecy_column_17,
    CAST(NULL AS string) AS prophecy_column_12,
    CAST(NULL AS string) AS prophecy_column_3,
    CAST(NULL AS string) AS prophecy_column_18,
    CAST(NULL AS string) AS prophecy_column_4
  
  FROM C1071007_LB003C_2 AS in0

),

C1071007_LB003C_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'C1071007_LB003C_57') }}

),

Union_58_1 AS (

  SELECT 
    CAST(`Status 2` AS string) AS prophecy_column_24,
    CAST(Assignment AS string) AS prophecy_column_25,
    CAST(`Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS string) AS prophecy_column_14,
    CAST(Status AS string) AS prophecy_column_20,
    CAST(`Inquiry hashslashChange Request hash` AS DOUBLE) AS prophecy_column_1,
    CAST(FileName AS string) AS prophecy_column_28,
    CAST(Visit AS string) AS prophecy_column_9,
    CAST(`Requested By` AS string) AS prophecy_column_2,
    CAST(`Response Provided By` AS DOUBLE) AS prophecy_column_22,
    CAST(F27 AS DOUBLE) AS prophecy_column_27,
    CAST(`Site ID` AS string) AS prophecy_column_7,
    CAST(`Expected Vendor Action` AS string) AS prophecy_column_16,
    CAST(`Data Type` AS string) AS prophecy_column_11,
    CAST(F26 AS DOUBLE) AS prophecy_column_26,
    CAST(`DM Comments` AS string) AS prophecy_column_23,
    CAST(`Subject ID hash` AS string) AS prophecy_column_8,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_19,
    CAST(`InquiryslashRequest Description` AS string) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_5,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_21,
    CAST(NULL AS string) AS prophecy_column_13,
    CAST(NULL AS string) AS prophecy_column_17,
    CAST(NULL AS string) AS prophecy_column_12,
    CAST(NULL AS string) AS prophecy_column_3,
    CAST(NULL AS string) AS prophecy_column_18,
    CAST(NULL AS string) AS prophecy_column_4
  
  FROM C1071007_LB003C_57 AS in0

),

Union_58 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_58_0', 'Union_58_1'], 
      [
        '[{"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "Double"}, {"name": "prophecy_column_28", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "Double"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "Double"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "Double"}, {"name": "prophecy_column_28", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "Double"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "Double"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_58_postRename AS (

  SELECT 
    prophecy_column_10 AS `Accession hash 
    paranthesesOpenIf ApplicableparanthesesClose`,
    prophecy_column_17 AS `Change
    From`,
    prophecy_column_16 AS `Expected Vendor Action`,
    prophecy_column_27 AS F27,
    prophecy_column_14 AS `Collection Date in eData paranthesesOpenOptionalparanthesesClose`,
    prophecy_column_13 AS `Collection Date in CRF
    paranthesesOpenIf AvailableparanthesesClose`,
    prophecy_column_2 AS `Requested By`,
    prophecy_column_11 AS `Data Type`,
    prophecy_column_4 AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    prophecy_column_24 AS `Status 2`,
    prophecy_column_9 AS Visit,
    prophecy_column_15 AS `InquiryslashRequest Description`,
    prophecy_column_19 AS `Response or Resolution Details`,
    prophecy_column_1 AS `Inquiry hashslashChange Request hash`,
    prophecy_column_8 AS `Subject ID hash`,
    prophecy_column_23 AS `DM Comments`,
    prophecy_column_26 AS F26,
    prophecy_column_12 AS Additional_Identifiere__g___Lab_ID_Test_Name,
    prophecy_column_18 AS `Change
    To`,
    prophecy_column_5 AS `Response Needed By
    Date`,
    prophecy_column_7 AS `Site ID`,
    prophecy_column_28 AS FileName,
    prophecy_column_3 AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    prophecy_column_22 AS `Response Provided By`,
    prophecy_column_25 AS Assignment,
    prophecy_column_21 AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    prophecy_column_6 AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    prophecy_column_20 AS Status
  
  FROM Union_58 AS in0

),

AlteryxSelect_60 AS (

  SELECT 
    `Inquiry hashslashChange Request hash` AS `Inquiry hashslashChange Request hash`,
    `Requested By` AS `Requested By`,
    `Request Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    `Priority paranthesesOpenHighcomma Mediumcomma LowparanthesesClose` AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    `Response Needed By Date` AS `Response Needed By
    Date`,
    `Type of Request paranthesesOpenInquiryslashChange RequestparanthesesClose` AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    `Site ID` AS `Site ID`,
    `Subject ID hash` AS `Subject ID hash`,
    Visit AS Visit,
    `Accession hash paranthesesOpenIf ApplicableparanthesesClose` AS `Accession hash 
    paranthesesOpenIf ApplicableparanthesesClose`,
    `Data Type` AS `Data Type`,
    Additional_Identifier_e__g___Lab_ID_Test_Name AS Additional_Identifiere__g___Lab_ID_Test_Name,
    `Collection Date in CRF paranthesesOpenIf AvailableparanthesesClose` AS `Collection Date in CRF
    paranthesesOpenIf AvailableparanthesesClose`,
    `Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS `Collection Date in eData paranthesesOpenOptionalparanthesesClose`,
    `InquiryslashRequest Description` AS `InquiryslashRequest Description`,
    `Expected Vendor Action` AS `Expected Vendor Action`,
    `Change From` AS `Change
    From`,
    `Change To` AS `Change
    To`,
    `Response or Resolution Details` AS `Response or Resolution Details`,
    Status AS Status,
    `Expectedslash Response Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    `Response Provided By` AS `Response Provided By`,
    `DM Comments` AS `DM Comments`,
    `Status 2` AS `Status 2`,
    Assignment AS Assignment,
    FileName AS FileName
  
  FROM Union_58_postRename AS in0

),

Formula_74_0 AS (

  SELECT 
    CAST((REVERSE(FileName)) AS string) AS report_run_date,
    *
  
  FROM AlteryxSelect_60 AS in0

),

Formula_74_1 AS (

  SELECT 
    CAST((
      SUBSTRING(
        report_run_date, 
        1, 
        (
          (
            CASE
              WHEN ((INSTR(report_run_date, ' ')) = 0)
                THEN NULL
              ELSE (INSTR(report_run_date, ' '))
            END
          )
          - 1
        ))
    ) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_74_0 AS in0

),

Formula_74_2 AS (

  SELECT 
    CAST((REVERSE(report_run_date)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_74_1 AS in0

),

DateTime_75_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(report_run_date, 'ddMMMyyyy')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM Formula_74_2 AS in0

),

Formula_37_0 AS (

  SELECT 
    CAST('C1071007' AS string) AS study_id,
    *
  
  FROM DateTime_75_0 AS in0

),

AlteryxSelect_34 AS (

  SELECT 
    study_id AS study_id,
    FileName AS FileName,
    DateTime_Out AS report_run_date,
    `Inquiry hashslashChange Request hash` AS `Inquiry hashslashChange Request hash`,
    `Requested By` AS `Requested By`,
    `Request Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    `Priority paranthesesOpenHighcomma Mediumcomma LowparanthesesClose` AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    `Response Needed By Date` AS `Response Needed By
    Date`,
    `Type of Request paranthesesOpenInquiryslashChange RequestparanthesesClose` AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    `Site ID` AS `Site ID`,
    `Subject ID hash` AS `Subject ID hash`,
    Visit AS Visit,
    `Accession hash paranthesesOpenIf ApplicableparanthesesClose` AS `Accession hash 
    paranthesesOpenIf ApplicableparanthesesClose`,
    `Data Type` AS `Data Type`,
    Additional_Identifier_e__g___Lab_ID_Test_Name AS Additional_Identifiere__g___Lab_ID_Test_Name,
    `Collection Date in CRF paranthesesOpenIf AvailableparanthesesClose` AS `Collection Date in CRF
    paranthesesOpenIf AvailableparanthesesClose`,
    `Collection Date in eData paranthesesOpenOptionalparanthesesClose` AS `Collection Date in eData paranthesesOpenOptionalparanthesesClose`,
    `InquiryslashRequest Description` AS `InquiryslashRequest Description`,
    `Expected Vendor Action` AS `Expected Vendor Action`,
    `Change From` AS `Change
    From`,
    `Change To` AS `Change
    To`,
    `Response or Resolution Details` AS `Response or Resolution Details`,
    Status AS Status,
    `Expectedslash Response Date paranthesesOpendd-Mmm-yyyyparanthesesClose` AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    `Response Provided By` AS `Response Provided By`,
    `DM Comments` AS `DM Comments`,
    `Status 2` AS `Status 2`
  
  FROM Formula_37_0 AS in0

)

SELECT *

FROM AlteryxSelect_34
