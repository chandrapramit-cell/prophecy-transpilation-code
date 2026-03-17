{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH C1071032_Centra_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'C1071032_Centra_1') }}

),

Formula_38_to_Formula_78_0 AS (

  SELECT 
    CAST('C1071032' AS string) AS study_id,
    CAST((REVERSE(FileName)) AS string) AS report_run_date,
    *
  
  FROM C1071032_Centra_1 AS in0

),

Formula_38_to_Formula_78_1 AS (

  SELECT 
    CAST((
      SUBSTRING(
        report_run_date, 
        1, 
        (
          (
            LEAST(
              (
                CASE
                  WHEN ((INSTR(report_run_date, '_')) = 0)
                    THEN NULL
                  ELSE (INSTR(report_run_date, '_'))
                END
              ), 
              (
                CASE
                  WHEN ((INSTR(report_run_date, ' ')) = 0)
                    THEN NULL
                  ELSE (INSTR(report_run_date, ' '))
                END
              ))
          )
          - 1
        ))
    ) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_38_to_Formula_78_0 AS in0

),

Formula_38_to_Formula_78_2 AS (

  SELECT 
    CAST((REVERSE(report_run_date)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_38_to_Formula_78_1 AS in0

),

DateTime_79_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(report_run_date, 'ddMMMyyyy')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM Formula_38_to_Formula_78_2 AS in0

),

AlteryxSelect_33 AS (

  SELECT 
    study_id AS study_id,
    FileName AS FileName,
    DateTime_Out AS report_run_date,
    `Inquiry hashslashChange Request hash` AS `Inquiry hashslashChange Request hash`,
    `Requested By` AS `Requested By`,
    `Response Needed By Date` AS `Response Needed By Date`,
    `Subject ID hash` AS `Subject ID hash`,
    Visit AS Visit,
    `Lab Collection Date in Vendor File` AS `Lab Collection Date in Vendor File`,
    `Lab type` AS `Lab type`,
    `Accession hash` AS `Accession hash`,
    `Missing Samples paranthesesOpenPx CodesslashLAB IDparanthesesClose` AS `Missing Samples paranthesesOpenPx CodesslashLAB IDparanthesesClose`,
    `InquiryslashRequest Description` AS `InquiryslashRequest Description`,
    `Response or Resolution Details` AS `Response or Resolution Details`,
    `Response Provided By` AS `Response Provided By`,
    `FINAL Status` AS `FINAL Status`,
    CAST(NULL AS string) AS `Request Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`,
    CAST(NULL AS string) AS `Priority
    paranthesesOpenHighcomma Mediumcomma LowparanthesesClose`,
    CAST(NULL AS string) AS `Type of Request
    paranthesesOpenInquiryslashChange RequestparanthesesClose`,
    CAST(NULL AS string) AS `Expectedslash Response Date 
    paranthesesOpendd-Mmm-yyyyparanthesesClose`
  
  FROM DateTime_79_0 AS in0

)

SELECT *

FROM AlteryxSelect_33
