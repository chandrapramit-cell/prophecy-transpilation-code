{{
  config({    
    "materialized": "table",
    "alias": "lab_reconciliat_19",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH C1071007_LB003__35 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_LAB_RECON_Concat', 'C1071007_LB003__35') }}

),

RecordID_70 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM C1071007_LB003__35

),

Union_38_1 AS (

  SELECT 
    CAST(`Subject ID` AS string) AS prophecy_column_5,
    CAST(Discrepancy AS string) AS prophecy_column_10,
    CAST(FileName AS string) AS prophecy_column_14,
    CAST(RecordID AS INTEGER) AS prophecy_column_1,
    CAST(`Subject Status` AS string) AS prophecy_column_6,
    CAST(`Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS string) AS prophecy_column_9,
    CAST(Ageing AS DOUBLE) AS prophecy_column_13,
    CAST(`Study ID` AS string) AS prophecy_column_2,
    CAST(`Reference LAB Table` AS string) AS prophecy_column_12,
    CAST(`Visit Refname` AS string) AS prophecy_column_7,
    CAST(`Site ID` AS string) AS prophecy_column_3,
    CAST(`Discrepancy Category` AS string) AS prophecy_column_11,
    CAST(`Date Of Visit paranthesesOpenInFormparanthesesClose` AS string) AS prophecy_column_8,
    CAST(COUNTRY AS string) AS prophecy_column_4
  
  FROM RecordID_70 AS in0

),

C1071007_LB003__1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_LAB_RECON_Concat', 'C1071007_LB003__1') }}

),

RecordID_69 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM C1071007_LB003__1

),

Union_38_0 AS (

  SELECT 
    CAST(`Subject ID` AS string) AS prophecy_column_5,
    CAST(Discrepancy AS string) AS prophecy_column_10,
    CAST(FileName AS string) AS prophecy_column_14,
    CAST(RecordID AS INTEGER) AS prophecy_column_1,
    CAST(`Subject Status` AS string) AS prophecy_column_6,
    CAST(`Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS string) AS prophecy_column_9,
    CAST(Ageing AS DOUBLE) AS prophecy_column_13,
    CAST(`Study ID` AS string) AS prophecy_column_2,
    CAST(`Reference LAB Table` AS string) AS prophecy_column_12,
    CAST(`Visit Refname` AS string) AS prophecy_column_7,
    CAST(`Site ID` AS string) AS prophecy_column_3,
    CAST(`Discrepancy Category` AS string) AS prophecy_column_11,
    CAST(`Date Of Visit paranthesesOpenInFormparanthesesClose` AS string) AS prophecy_column_8,
    CAST(COUNTRY AS string) AS prophecy_column_4
  
  FROM RecordID_69 AS in0

),

Union_38 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_38_0', 'Union_38_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "Integer"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "Integer"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_38_postRename AS (

  SELECT 
    prophecy_column_12 AS `Reference LAB Table`,
    prophecy_column_2 AS `Study ID`,
    prophecy_column_8 AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    prophecy_column_6 AS `Subject Status`,
    prophecy_column_7 AS `Visit Refname`,
    prophecy_column_5 AS `Subject ID`,
    prophecy_column_11 AS `Discrepancy Category`,
    prophecy_column_13 AS Ageing,
    prophecy_column_3 AS `Site ID`,
    prophecy_column_14 AS FileName,
    prophecy_column_4 AS COUNTRY,
    prophecy_column_1 AS RecordID,
    prophecy_column_9 AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    prophecy_column_10 AS Discrepancy
  
  FROM Union_38 AS in0

),

Formula_42_0 AS (

  SELECT 
    CAST((REVERSE(FileName)) AS string) AS report_run_date,
    *
  
  FROM Union_38_postRename AS in0

),

Formula_42_1 AS (

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
  
  FROM Formula_42_0 AS in0

),

Formula_42_2 AS (

  SELECT 
    CAST((REVERSE(report_run_date)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_42_1 AS in0

),

Formula_42_3 AS (

  SELECT 
    CAST((SUBSTRING(report_run_date, 1, 8)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_42_2 AS in0

),

DateTime_43_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(report_run_date, 'yyyyMMdd')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM Formula_42_3 AS in0

),

AlteryxSelect_44 AS (

  SELECT 
    `Study ID` AS `Study ID`,
    FileName AS FileName,
    DateTime_Out AS report_run_date,
    `Site ID` AS `Site ID`,
    COUNTRY AS COUNTRY,
    `Subject ID` AS `Subject ID`,
    `Subject Status` AS `Subject Status`,
    `Visit Refname` AS `Visit Refname`,
    `Date Of Visit paranthesesOpenInFormparanthesesClose` AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    `Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    Discrepancy AS Discrepancy,
    `Discrepancy Category` AS `Discrepancy Category`,
    `Reference LAB Table` AS `Reference LAB Table`,
    Ageing AS Ageing,
    RecordID AS RecordID
  
  FROM DateTime_43_0 AS in0

),

Union_10_0 AS (

  SELECT 
    CAST(COUNTRY AS string) AS prophecy_column_5,
    CAST(`Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS string) AS prophecy_column_10,
    CAST(Ageing AS DOUBLE) AS prophecy_column_14,
    CAST(`Study ID` AS string) AS prophecy_column_1,
    CAST(`Subject ID` AS string) AS prophecy_column_6,
    CAST(`Date Of Visit paranthesesOpenInFormparanthesesClose` AS string) AS prophecy_column_9,
    CAST(`Reference LAB Table` AS string) AS prophecy_column_13,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Discrepancy Category` AS string) AS prophecy_column_12,
    CAST(`Subject Status` AS string) AS prophecy_column_7,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(Discrepancy AS string) AS prophecy_column_11,
    CAST(`Visit Refname` AS string) AS prophecy_column_8,
    CAST(`Site ID` AS string) AS prophecy_column_4,
    CAST(RecordID AS INTEGER) AS prophecy_column_15
  
  FROM AlteryxSelect_44 AS in0

),

DynamicRename_7 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_LAB_RECON_Concat', 'DynamicRename_7') }}

),

AlteryxSelect_9 AS (

  SELECT 
    RecordID AS RecordID,
    `Study ID` AS `Study ID`,
    COUNTRY AS COUNTRY,
    `Site ID` AS `Site ID`,
    `Subject ID` AS `Subject ID`,
    `Visit Refname` AS `Visit Refname`,
    `Date Of Visit paranthesesOpenInFormparanthesesClose` AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    `Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    `Lab Time of Collection` AS `Lab Time of Collection`,
    `Discrepancy Category` AS `Discrepancy Category`,
    Ageing AS Ageing,
    FileName AS FileName
  
  FROM DynamicRename_7 AS in0

),

Formula_45_0 AS (

  SELECT 
    CAST((REVERSE(FileName)) AS string) AS report_run_date,
    *
  
  FROM AlteryxSelect_9 AS in0

),

Formula_45_1 AS (

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
  
  FROM Formula_45_0 AS in0

),

Formula_45_2 AS (

  SELECT 
    CAST((REVERSE(report_run_date)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_45_1 AS in0

),

Formula_45_3 AS (

  SELECT 
    CAST((SUBSTRING(report_run_date, 1, 8)) AS string) AS report_run_date,
    * EXCEPT (`report_run_date`)
  
  FROM Formula_45_2 AS in0

),

DateTime_46_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(report_run_date, 'yyyyMMdd')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM Formula_45_3 AS in0

),

AlteryxSelect_47 AS (

  SELECT 
    `Study ID` AS `Study ID`,
    FileName AS FileName,
    DateTime_Out AS report_run_date,
    `Site ID` AS `Site ID`,
    COUNTRY AS COUNTRY,
    `Subject ID` AS `Subject ID`,
    `Visit Refname` AS `Visit Refname`,
    `Date Of Visit paranthesesOpenInFormparanthesesClose` AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    `Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    `Discrepancy Category` AS `Discrepancy Category`,
    Ageing AS Ageing,
    RecordID AS RecordID,
    CAST(NULL AS string) AS `Subject Status`,
    CAST(NULL AS string) AS Discrepancy,
    CAST(NULL AS string) AS `Reference LAB Table`
  
  FROM DateTime_46_0 AS in0

),

Union_10_1 AS (

  SELECT 
    CAST(COUNTRY AS string) AS prophecy_column_5,
    CAST(`Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS string) AS prophecy_column_10,
    CAST(Ageing AS DOUBLE) AS prophecy_column_14,
    CAST(`Study ID` AS string) AS prophecy_column_1,
    CAST(`Subject ID` AS string) AS prophecy_column_6,
    CAST(`Date Of Visit paranthesesOpenInFormparanthesesClose` AS string) AS prophecy_column_9,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Discrepancy Category` AS string) AS prophecy_column_12,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(report_run_date AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(`Visit Refname` AS string) AS prophecy_column_8,
    CAST(`Site ID` AS string) AS prophecy_column_4,
    CAST(RecordID AS INTEGER) AS prophecy_column_15
  
  FROM AlteryxSelect_47 AS in0

),

Union_10 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_10_0', 'Union_10_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_10_postRename AS (

  SELECT 
    prophecy_column_13 AS `Reference LAB Table`,
    prophecy_column_1 AS `Study ID`,
    prophecy_column_9 AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    prophecy_column_7 AS `Subject Status`,
    prophecy_column_8 AS `Visit Refname`,
    prophecy_column_6 AS `Subject ID`,
    prophecy_column_3 AS report_run_date,
    prophecy_column_12 AS `Discrepancy Category`,
    prophecy_column_14 AS Ageing,
    prophecy_column_4 AS `Site ID`,
    prophecy_column_2 AS FileName,
    prophecy_column_5 AS COUNTRY,
    prophecy_column_15 AS RecordID,
    prophecy_column_10 AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    prophecy_column_11 AS Discrepancy
  
  FROM Union_10 AS in0

),

AlteryxSelect_11 AS (

  SELECT 
    `Study ID` AS `Study ID`,
    FileName AS FileName,
    report_run_date AS report_run_date,
    `Site ID` AS `Site ID`,
    COUNTRY AS COUNTRY,
    `Subject ID` AS `Subject ID`,
    `Subject Status` AS `Subject Status`,
    `Visit Refname` AS `Visit Refname`,
    `Date Of Visit paranthesesOpenInFormparanthesesClose` AS `Date Of Visit paranthesesOpenInFormparanthesesClose`,
    `Date Of Assessment paranthesesOpenVendor DataparanthesesClose` AS `Date Of Assessment paranthesesOpenVendor DataparanthesesClose`,
    Discrepancy AS Discrepancy,
    `Discrepancy Category` AS `Discrepancy Category`,
    `Reference LAB Table` AS `Reference LAB Table`,
    Ageing AS Aging,
    RecordID AS RecordID
  
  FROM Union_10_postRename AS in0

),

DynamicRename_12 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_11'], 
      [
        'Reference LAB Table', 
        'Aging', 
        'Study ID', 
        'Date Of Visit paranthesesOpenInFormparanthesesClose', 
        'Subject Status', 
        'Visit Refname', 
        'Subject ID', 
        'report_run_date', 
        'Discrepancy Category', 
        'Site ID', 
        'FileName', 
        'COUNTRY', 
        'RecordID', 
        'Date Of Assessment paranthesesOpenVendor DataparanthesesClose', 
        'Discrepancy'
      ], 
      'advancedRename', 
      [
        'Study ID', 
        'FileName', 
        'report_run_date', 
        'Site ID', 
        'COUNTRY', 
        'Subject ID', 
        'Subject Status', 
        'Visit Refname', 
        'Date Of Visit paranthesesOpenInFormparanthesesClose', 
        'Date Of Assessment paranthesesOpenVendor DataparanthesesClose', 
        'Discrepancy', 
        'Discrepancy Category', 
        'Reference LAB Table', 
        'Aging', 
        'RecordID'
      ], 
      'Suffix', 
      '', 
      "lower(column_name)"
    )
  }}

),

DynamicRename_13 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_12'], 
      [
        'date of assessment paranthesesOpenvendor dataparanthesesClose', 
        'subject id', 
        'date of visit paranthesesOpeninformparanthesesClose', 
        'report_run_date', 
        'country', 
        'discrepancy', 
        'site id', 
        'filename', 
        'recordid', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'visit refname'
      ], 
      'advancedRename', 
      [
        'subject id', 
        'report_run_date', 
        'country', 
        'discrepancy', 
        'site id', 
        'filename', 
        'recordid', 
        'date of assessment paranthesesOpenvendor dataparanthesesClose', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'date of visit paranthesesOpeninformparanthesesClose', 
        'visit refname'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '[(]', substring('', 1, 1))"
    )
  }}

),

DynamicRename_14 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_13'], 
      [
        'subject id', 
        'date of assessment vendor dataparanthesesClose', 
        'report_run_date', 
        'country', 
        'discrepancy', 
        'date of visit informparanthesesClose', 
        'site id', 
        'filename', 
        'recordid', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'visit refname'
      ], 
      'advancedRename', 
      [
        'subject id', 
        'date of assessment vendor dataparanthesesClose', 
        'report_run_date', 
        'country', 
        'discrepancy', 
        'site id', 
        'filename', 
        'recordid', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'visit refname', 
        'date of visit informparanthesesClose'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '[)]', substring('', 1, 1))"
    )
  }}

),

DynamicRename_15 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_14'], 
      [
        'subject id', 
        'report_run_date', 
        'date of assessment vendor data', 
        'country', 
        'discrepancy', 
        'date of visit inform', 
        'site id', 
        'filename', 
        'recordid', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'visit refname'
      ], 
      'advancedRename', 
      [
        'subject id', 
        'report_run_date', 
        'date of assessment vendor data', 
        'country', 
        'discrepancy', 
        'date of visit inform', 
        'site id', 
        'filename', 
        'recordid', 
        'study id', 
        'discrepancy category', 
        'reference lab table', 
        'aging', 
        'subject status', 
        'visit refname'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '[ ]', substring('_', 1, 1))"
    )
  }}

),

TextToColumns_16 AS (

  {{
    prophecy_basics.TextToColumns(
      ['DynamicRename_15'], 
      'date_of_assessment_vendor_data', 
      ",", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'date_of_assessment_vendor_data', 
      'date_of_assessment_vendor_data', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_16_dropGem_0 AS (

  SELECT 
    date_of_assessment_vendor_data_1_date_of_assessment_vendor_data AS date_of_assessment_vendor_data1,
    date_of_assessment_vendor_data_2_date_of_assessment_vendor_data AS date_of_assessment_vendor_data2,
    * EXCEPT (`date_of_assessment_vendor_data_1_date_of_assessment_vendor_data`, 
    `date_of_assessment_vendor_data_2_date_of_assessment_vendor_data`)
  
  FROM TextToColumns_16 AS in0

),

AlteryxSelect_18 AS (

  SELECT 
    study_id AS study_id,
    filename AS filename,
    report_run_date AS report_run_date,
    site_id AS site_id,
    country AS country,
    subject_id AS subject_id,
    subject_status AS subject_status,
    visit_refname AS visit_refname,
    date_of_visit_inform AS date_of_visit_inform,
    discrepancy AS discrepancy,
    discrepancy_category AS discrepancy_category,
    reference_lab_table AS reference_lab_table,
    aging AS aging,
    recordid AS recordid,
    date_of_assessment_vendor_data1 AS date_of_assessment_vendor_data
  
  FROM TextToColumns_16_dropGem_0 AS in0

),

AlteryxSelect_32 AS (

  SELECT 
    study_id AS study_id,
    filename AS filename,
    report_run_date AS report_run_date,
    site_id AS site_id,
    country AS country,
    subject_id AS subject_id,
    subject_status AS subject_status,
    visit_refname AS visit_refname,
    date_of_visit_inform AS date_of_visit_inform,
    discrepancy AS discrepancy,
    discrepancy_category AS discrepancy_category,
    reference_lab_table AS reference_lab_table,
    aging AS aging,
    recordid AS recordid,
    date_of_assessment_vendor_data AS date_of_assessment_vendor_data
  
  FROM AlteryxSelect_18 AS in0

),

Formula_48_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (date_of_visit_inform = 'Not Done')
        OR contains(coalesce(lower(date_of_visit_inform), ''), lower('           '))
      )
        THEN NULL
      ELSE date_of_visit_inform
    END AS STRING) AS date_of_visit_inform,
    * EXCEPT (`date_of_visit_inform`)
  
  FROM AlteryxSelect_32 AS in0

),

Filter_25_reject AS (

  SELECT * 
  
  FROM Formula_48_0 AS in0
  
  WHERE (NOT (not(contains(date_of_visit_inform, '-'))) OR isnull(not(contains(date_of_visit_inform, '-'))))

),

DateTime_24_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(date_of_visit_inform, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS date_of_visit_inform2,
    *
  
  FROM Filter_25_reject AS in0

),

Filter_25 AS (

  SELECT * 
  
  FROM Formula_48_0 AS in0
  
  WHERE not(contains(date_of_visit_inform, '-'))

),

DateTime_20_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(date_of_visit_inform, 'ddMMMyyy')), 'yyyy-MM-dd')) AS date_of_visit_inform2,
    *
  
  FROM Filter_25 AS in0

),

Union_29 AS (

  {{
    prophecy_basics.UnionByName(
      ['DateTime_24_0', 'DateTime_20_0'], 
      [
        '[{"name": "date_of_visit_inform2", "dataType": "String"}, {"name": "date_of_visit_inform", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "filename", "dataType": "String"}, {"name": "report_run_date", "dataType": "String"}, {"name": "site_id", "dataType": "String"}, {"name": "country", "dataType": "String"}, {"name": "subject_id", "dataType": "String"}, {"name": "subject_status", "dataType": "String"}, {"name": "visit_refname", "dataType": "String"}, {"name": "discrepancy", "dataType": "String"}, {"name": "discrepancy_category", "dataType": "String"}, {"name": "reference_lab_table", "dataType": "String"}, {"name": "aging", "dataType": "String"}, {"name": "recordid", "dataType": "String"}, {"name": "date_of_assessment_vendor_data", "dataType": "String"}]', 
        '[{"name": "date_of_visit_inform2", "dataType": "String"}, {"name": "date_of_visit_inform", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "filename", "dataType": "String"}, {"name": "report_run_date", "dataType": "String"}, {"name": "site_id", "dataType": "String"}, {"name": "country", "dataType": "String"}, {"name": "subject_id", "dataType": "String"}, {"name": "subject_status", "dataType": "String"}, {"name": "visit_refname", "dataType": "String"}, {"name": "discrepancy", "dataType": "String"}, {"name": "discrepancy_category", "dataType": "String"}, {"name": "reference_lab_table", "dataType": "String"}, {"name": "aging", "dataType": "String"}, {"name": "recordid", "dataType": "String"}, {"name": "date_of_assessment_vendor_data", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_26_reject AS (

  SELECT * 
  
  FROM Union_29 AS in0
  
  WHERE (
          NOT (not(contains(date_of_assessment_vendor_data, '-')))
          OR isnull(not(contains(date_of_assessment_vendor_data, '-')))
        )

),

DateTime_28_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(date_of_assessment_vendor_data, 'dd-MMM-yyy')), 'yyyy-MM-dd')) AS date_of_assessment_vendor_data1,
    *
  
  FROM Filter_26_reject AS in0

),

Filter_26 AS (

  SELECT * 
  
  FROM Union_29 AS in0
  
  WHERE not(contains(date_of_assessment_vendor_data, '-'))

),

DateTime_21_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(date_of_assessment_vendor_data, 'ddMMMyyy')), 'yyyy-MM-dd')) AS date_of_assessment_vendor_data1,
    *
  
  FROM Filter_26 AS in0

),

Union_30 AS (

  {{
    prophecy_basics.UnionByName(
      ['DateTime_21_0', 'DateTime_28_0'], 
      [
        '[{"name": "date_of_assessment_vendor_data1", "dataType": "String"}, {"name": "date_of_visit_inform2", "dataType": "String"}, {"name": "date_of_visit_inform", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "filename", "dataType": "String"}, {"name": "report_run_date", "dataType": "String"}, {"name": "site_id", "dataType": "String"}, {"name": "country", "dataType": "String"}, {"name": "subject_id", "dataType": "String"}, {"name": "subject_status", "dataType": "String"}, {"name": "visit_refname", "dataType": "String"}, {"name": "discrepancy", "dataType": "String"}, {"name": "discrepancy_category", "dataType": "String"}, {"name": "reference_lab_table", "dataType": "String"}, {"name": "aging", "dataType": "String"}, {"name": "recordid", "dataType": "String"}, {"name": "date_of_assessment_vendor_data", "dataType": "String"}]', 
        '[{"name": "date_of_assessment_vendor_data1", "dataType": "String"}, {"name": "date_of_visit_inform2", "dataType": "String"}, {"name": "date_of_visit_inform", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "filename", "dataType": "String"}, {"name": "report_run_date", "dataType": "String"}, {"name": "site_id", "dataType": "String"}, {"name": "country", "dataType": "String"}, {"name": "subject_id", "dataType": "String"}, {"name": "subject_status", "dataType": "String"}, {"name": "visit_refname", "dataType": "String"}, {"name": "discrepancy", "dataType": "String"}, {"name": "discrepancy_category", "dataType": "String"}, {"name": "reference_lab_table", "dataType": "String"}, {"name": "aging", "dataType": "String"}, {"name": "recordid", "dataType": "String"}, {"name": "date_of_assessment_vendor_data", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_23 AS (

  SELECT 
    recordid AS recordid,
    study_id AS study_id,
    filename AS filename,
    report_run_date AS report_run_date,
    site_id AS site_id,
    country AS country,
    subject_id AS subject_id,
    subject_status AS subject_status,
    visit_refname AS visit_refname,
    date_of_visit_inform2 AS date_of_visit_inform,
    date_of_assessment_vendor_data1 AS date_of_assessment_vendor_data,
    discrepancy AS discrepancy,
    discrepancy_category AS discrepancy_category,
    reference_lab_table AS reference_lab_table,
    aging AS aging
  
  FROM Union_30 AS in0

)

SELECT *

FROM AlteryxSelect_23
