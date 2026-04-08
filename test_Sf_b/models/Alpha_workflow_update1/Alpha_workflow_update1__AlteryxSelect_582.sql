{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_515 AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__AlteryxSelect_515')}}

),

AlteryxSelect_397 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    CAST("COMBOSLASHACCT CODE" AS STRING) AS "COMBOSLASHACCT CODE",
    * EXCLUDE ("COMBOSLASHACCT CODE")
  
  FROM AlteryxSelect_515 AS in0

),

Formula_536_0 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    CAST(CO AS STRING) AS "COMPANY CODE",
    CAST((SUBSTRING("BUSINESS UNIT", 1, ((CHARINDEX(':', "BUSINESS UNIT")) - 1))) AS STRING) AS "BUSINESS UNIT EXT",
    *
  
  FROM AlteryxSelect_397 AS in0

),

DynamicRename_537 AS (

  {#VisualGroup: BAcompanymismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Formula_536_0'], 
      [
        'DOB', 
        'SAL PLAN', 
        'ENTRY DATE', 
        'COUNTRY2', 
        'MAIL DROP', 
        'REASON', 
        'COMP RATE', 
        'LOCATION', 
        'SUPV ID', 
        'DEPARTMENT', 
        'STND HRSMT', 
        'FSLASHP', 
        'INCR DT', 
        'ALIGHT BENCODE', 
        'FIN SECTOR', 
        'LOCATION SET ID', 
        'COMBOSLASHACCT CODE', 
        'NAME', 
        'ADDRESS 2', 
        'FIRST NAME', 
        'DEPTIDCOMMA4COMMA1', 
        'LAST NAME', 
        'SHFT RATE', 
        'TITLE', 
        'MAR STATUS', 
        'FRINGE CD', 
        'ES BA FUNCTION HREP', 
        'SUPERVISOR NAME', 
        'HOME PHONE O', 
        'COUNTRY', 
        'EMPLOYEE ID', 
        'GLC CODE', 
        'FLSA STAT', 
        'REHIRE DT', 
        'SUBFUNCTION', 
        'ELIG FLD 1', 
        'ADDRESS 3', 
        'WORK SCHEDULE', 
        'ELIG FID 2', 
        'FTE', 
        'LAST FOUR OF SSH', 
        'SEQUERCE', 
        'HREPID', 
        'BARG UNIT', 
        'STATE2', 
        'ELIA FLD 9', 
        'DEPT FUNCTION', 
        'EMP ID', 
        'WORK PHONE', 
        'TAX LOC', 
        'WIIRS COMP', 
        'ELIG FID 5', 
        'DEPT', 
        'BUSINESS UNIT EXT', 
        'CHNG AMT', 
        'YEAR', 
        'ELIG FID', 
        'GRADE', 
        'EMPL CLASS', 
        'COMPANY CODE', 
        'DEPT DATE', 
        'HRBP NAME', 
        'VIT TCG', 
        'ELIG FID A', 
        'BUSINESS UNIT2', 
        'DEPTIDS2', 
        'SSA', 
        'ELIG FLD 7', 
        'CO', 
        'UNION CODE', 
        'ADDRESS 4', 
        'LOC DESCR', 
        'BUSINESS UNIT', 
        'JOB TITLE', 
        'UNION SENIORITY DATE', 
        'CITIZENSHIP STATUS', 
        'AGE', 
        'SALARY SET ID', 
        'ORIG MIRE DATE', 
        'BA', 
        'DIRSLASHIND', 
        'JOB CODE', 
        'SENICE MONT', 
        'COMPANY SENIORITY D', 
        'EMAL', 
        'BID CODE', 
        'CITY', 
        'BIRTHPLACE', 
        'LABOR CODE', 
        'RSLASHT', 
        'WORK PERIOD', 
        'ELIG FID 4', 
        'JOB FUND', 
        'TERM DATE', 
        'SHIFT', 
        'POSTAL', 
        'SHIFT FCTR', 
        'MIRE DATE', 
        'ACTION DATE', 
        'STATE', 
        'BIRTH MM-DD', 
        'ALIGHT KCODE', 
        'SEX', 
        'EEO-1 CAT', 
        'PAY GROUP', 
        'ACTION', 
        'JOB GROUP', 
        'ADDRESS 1', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'MIDDLE', 
        'HR SECTOR', 
        'SERVICE DT', 
        'ANN BEN RT', 
        'TIME IN HR', 
        'PAY STATUS', 
        'GRADE DATE', 
        'SUPV EMAIL'
      ], 
      'advancedRename', 
      [
        'COMPANY CODE', 
        'BUSINESS UNIT EXT', 
        'COMBOSLASHACCT CODE', 
        'JOB CODE', 
        'EMPLOYEE ID', 
        'DOB', 
        'GRADE DATE', 
        'INCR DT', 
        'SUPV ID', 
        'WORK PERIOD', 
        'BID CODE', 
        'ELIG FID', 
        'ELIG FID A', 
        'COMP RATE', 
        'ES BA FUNCTION HREP', 
        'BUSINESS UNIT', 
        'STATE2', 
        'ELIG FID 5', 
        'CHNG AMT', 
        'REHIRE DT', 
        'GLC CODE', 
        'SAL PLAN', 
        'EMP ID', 
        'MIDDLE', 
        'ALIGHT BENCODE', 
        'SHIFT FCTR', 
        'FRINGE CD', 
        'HR SECTOR', 
        'NAME', 
        'UNION CODE', 
        'ELIG FLD 1', 
        'TAX LOC', 
        'ELIA FLD 9', 
        'LAST NAME', 
        'FLSA STAT', 
        'DEPT FUNCTION', 
        'COMPANY SENIORITY D', 
        'CITY', 
        'ELIG FID 4', 
        'HOME PHONE O', 
        'TIME IN HR', 
        'SUBFUNCTION', 
        'ADDRESS 1', 
        'PAY GROUP', 
        'STND HRSMT', 
        'DEPT DATE', 
        'FTE', 
        'EEO-1 CAT', 
        'SUPERVISOR NAME', 
        'GRADE', 
        'HREPID', 
        'ACTION', 
        'HRBP NAME', 
        'EMPL CLASS', 
        'TITLE', 
        'SEQUERCE', 
        'BIRTHPLACE', 
        'JOB FUND', 
        'WORK PHONE', 
        'AGE', 
        'SEX', 
        'VIT TCG', 
        'YEAR', 
        'DEPTIDS2', 
        'SSA', 
        'JOB GROUP', 
        'ADDRESS 4', 
        'EMAL', 
        'DEPTIDCOMMA4COMMA1', 
        'JOB TITLE', 
        'SERVICE DT', 
        'FSLASHP', 
        'MAIL DROP', 
        'LOC DESCR', 
        'SALARY SET ID', 
        'FIRST NAME', 
        'WIIRS COMP', 
        'BA', 
        'PAY STATUS', 
        'DEPT', 
        'BUSINESS UNIT2', 
        'ADDRESS 3', 
        'SUPV EMAIL', 
        'SENICE MONT', 
        'CO', 
        'LOCATION', 
        'UNION SENIORITY DATE', 
        'SHIFT', 
        'ENTRY DATE', 
        'LOCATION SET ID', 
        'BIRTH MM-DD', 
        'ELIG FID 2', 
        'LAST FOUR OF SSH', 
        'MIRE DATE', 
        'ALIGHT KCODE', 
        'FIN SECTOR', 
        'CITIZENSHIP STATUS', 
        'ACTION DATE', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'COUNTRY', 
        'ANN BEN RT', 
        'REASON', 
        'SHFT RATE', 
        'WORK SCHEDULE', 
        'ORIG MIRE DATE', 
        'COUNTRY2', 
        'DEPARTMENT', 
        'ELIG FLD 7', 
        'DIRSLASHIND', 
        'STATE', 
        'RSLASHT', 
        'BARG UNIT', 
        'MAR STATUS', 
        'ADDRESS 2', 
        'LABOR CODE', 
        'TERM DATE', 
        'POSTAL'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_RA')"
    )
  }}

),

Gen_00_datakey__28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Gen_00_datakey__28') }}

),

DynamicRename_532 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    F1 AS "DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE",
    F2 AS "SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE",
    F3 AS "JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE",
    F4 AS "BU VALUE",
    F5 AS "BU DESCRIPTION",
    F6 AS "COMPANY CRITERIA",
    F7 AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM Gen_00_datakey__28 AS in0

),

DynamicRename_532_row_number AS (

  {#VisualGroup: BAcompanymismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_532'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_532_filter AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT * 
  
  FROM DynamicRename_532_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_532_drop_0 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_532_filter AS in0

),

AlteryxSelect_534 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    "BU VALUE" AS "BU VALUE",
    "COMPANY CRITERIA" AS "COMPANY CRITERIA"
  
  FROM DynamicRename_532_drop_0 AS in0

),

DynamicRename_538 AS (

  {#VisualGroup: BAcompanymismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_534'], 
      ['BU VALUE', 'COMPANY CRITERIA'], 
      'advancedRename', 
      ['BU VALUE', 'COMPANY CRITERIA'], 
      'Suffix', 
      '', 
      "concat(column_name, '_datakey')"
    )
  }}

),

Join_535_inner AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM DynamicRename_537 AS in0
  INNER JOIN DynamicRename_538 AS in1
     ON (in0."COMPANY CODE_RA" = in1."COMPANY CRITERIA_DATAKEY")

),

Gen_dept_id_all_199 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Gen_dept_id_all_199') }}

),

DynamicRename_202 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    F1 AS "LOB OR BUSINESS AREA",
    F2 AS "EFFECTIVE DATE",
    F1_2 AS DESCRIPTION,
    F9_2 AS "SET ID",
    F3 AS "DEPARTMENT FUNCTION",
    F4 AS "HR SECTOR",
    F5 AS "STATUS AS OF EFFECTIVE DATE",
    F6 AS "LMS COMPANY BUSINESS UNIT",
    F7 AS "ORGANIZATIONAL GROUP",
    F8 AS "SHORT DESC",
    F9 AS "FUNCTION LOB",
    F10 AS COMPANY,
    F11 AS "DEPT ID"
  
  FROM Gen_dept_id_all_199 AS in0

),

DynamicRename_202_row_number AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_202'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_202_filter AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT * 
  
  FROM DynamicRename_202_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_202_drop_0 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_202_filter AS in0

),

AlteryxSelect_203 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    COMPANY AS COMPANY,
    "DEPT ID" AS "DEPT ID"
  
  FROM DynamicRename_202_drop_0 AS in0

),

DynamicRename_205 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_203'], 
      ['COMPANY', 'DEPT ID'], 
      'advancedRename', 
      ['COMPANY', 'DEPT ID'], 
      'Suffix', 
      '', 
      "concat(column_name, '_dept')"
    )
  }}

),

AlteryxSelect_453 AS (

  SELECT 
    "BUSINESS UNIT" AS "BUSINESS UNIT",
    "FIRST NAME" AS "FIRST NAME",
    "LAST NAME" AS "LAST NAME",
    "JOB TITLE" AS "JOB TITLE",
    "JOB CODE" AS "JOB CODE",
    "WORK SCHEDULE" AS "WORK SCHEDULE",
    "EMP ID" AS "EMP ID",
    GRADE AS GRADE,
    DEPARTMENT AS DEPARTMENT,
    SHIFT AS SHIFT,
    "EMPLOYEE ID" AS "EMPLOYEE ID",
    CAST(NULL AS STRING) AS "RECRUITING ANALYST",
    CAST(NULL AS STRING) AS "BUSINESS AREA DIVISION",
    CAST(NULL AS STRING) AS "REQ HASH",
    CAST(NULL AS STRING) AS "FULL TIME SLASH PART TIME",
    CAST(NULL AS STRING) AS "CURRENT HR STATUS",
    CAST(NULL AS STRING) AS "MIDDLE INITIAL",
    CAST(NULL AS STRING) AS "DIRECT SUPERVISOR",
    CAST(NULL AS STRING) AS RECRUITER,
    CAST(NULL AS STRING) AS "PEOPLESOFT STATUS",
    CAST(NULL AS STRING) AS "CANDIDATE HIRE TYPE DETAILS",
    CAST(NULL AS STRING) AS DIRECTSLASHINDIRECT,
    CAST(NULL AS STRING) AS "FLSA STATUS",
    CAST(NULL AS STRING) AS "EMPLOYEE CLASS",
    CAST(NULL AS STRING) AS "BENEFITS SUMMARY",
    CAST(NULL AS STRING) AS "REGULAR SLASH TEMPORARY",
    CAST(NULL AS STRING) AS "HIRING MANAGER",
    CAST(NULL AS STRING) AS "HIRE TYPE",
    CAST(NULL AS STRING) AS "REQ STATUS",
    CAST(NULL AS STRING) AS "LOCATION CODE",
    CAST(NULL AS STRING) AS COMPANY,
    CAST(NULL AS STRING) AS "FINANCIAL COST CENTER",
    CAST(NULL AS STRING) AS "CANDIDATE HIRE TYPE",
    CAST(NULL AS STRING) AS "START DATE",
    CAST(NULL AS STRING) AS "UNION JOB"
  
  FROM AlteryxSelect_515 AS in0

),

AlteryxSelect_471 AS (

  SELECT 
    "RECRUITING ANALYST" AS "RECRUITING ANALYST",
    "FIRST NAME" AS "FIRST NAME",
    "LAST NAME" AS "LAST NAME",
    "MIDDLE INITIAL" AS "MIDDLE INITIAL",
    "EMPLOYEE ID" AS "EMPLOYEE ID",
    "START DATE" AS "START DATE",
    "CURRENT HR STATUS" AS "CURRENT HR STATUS",
    "PEOPLESOFT STATUS" AS "PEOPLESOFT STATUS",
    "REQ HASH" AS "REQ HASH",
    "DIRECT SUPERVISOR" AS "DIRECT SUPERVISOR",
    "HIRING MANAGER" AS "HIRING MANAGER",
    RECRUITER AS RECRUITER,
    "JOB CODE" AS "JOB CODE",
    "JOB TITLE" AS "JOB TITLE",
    COMPANY AS COMPANY,
    "BUSINESS UNIT" AS "BUSINESS UNIT",
    "BUSINESS AREA DIVISION" AS "BUSINESS AREA DIVISION",
    DEPARTMENT AS DEPARTMENT,
    "FINANCIAL COST CENTER" AS "FINANCIAL COST CENTER",
    "LOCATION CODE" AS "LOCATION CODE",
    GRADE AS GRADE,
    SHIFT AS SHIFT,
    "FULL TIME SLASH PART TIME" AS "FULL TIME SLASH PART TIME",
    "REGULAR SLASH TEMPORARY" AS "REGULAR SLASH TEMPORARY",
    "FLSA STATUS" AS "FLSA STATUS",
    DIRECTSLASHINDIRECT AS DIRECTSLASHINDIRECT,
    "UNION JOB" AS "UNION JOB",
    "HIRE TYPE" AS "HIRE TYPE",
    "CANDIDATE HIRE TYPE" AS "CANDIDATE HIRE TYPE",
    "CANDIDATE HIRE TYPE DETAILS" AS "CANDIDATE HIRE TYPE DETAILS",
    "WORK SCHEDULE" AS "WORK SCHEDULE",
    "EMPLOYEE CLASS" AS "EMPLOYEE CLASS",
    "BENEFITS SUMMARY" AS "BENEFITS SUMMARY",
    "REQ STATUS" AS "REQ STATUS",
    * EXCLUDE ("RECRUITING ANALYST", 
    "FIRST NAME", 
    "LAST NAME", 
    "MIDDLE INITIAL", 
    "EMPLOYEE ID", 
    "START DATE", 
    "CURRENT HR STATUS", 
    "PEOPLESOFT STATUS", 
    "REQ HASH", 
    "DIRECT SUPERVISOR", 
    "HIRING MANAGER", 
    "RECRUITER", 
    "JOB CODE", 
    "JOB TITLE", 
    "COMPANY", 
    "BUSINESS UNIT", 
    "BUSINESS AREA DIVISION", 
    "DEPARTMENT", 
    "FINANCIAL COST CENTER", 
    "LOCATION CODE", 
    "GRADE", 
    "SHIFT", 
    "FULL TIME SLASH PART TIME", 
    "REGULAR SLASH TEMPORARY", 
    "FLSA STATUS", 
    "DIRECTSLASHINDIRECT", 
    "UNION JOB", 
    "HIRE TYPE", 
    "CANDIDATE HIRE TYPE", 
    "CANDIDATE HIRE TYPE DETAILS", 
    "WORK SCHEDULE", 
    "EMPLOYEE CLASS", 
    "BENEFITS SUMMARY", 
    "REQ STATUS")
  
  FROM AlteryxSelect_453 AS in0

),

AlteryxSelect_509 AS (

  SELECT 
    CAST("EMP ID" AS STRING) AS "EMP ID",
    * EXCLUDE ("EMP ID")
  
  FROM AlteryxSelect_471 AS in0

),

Accountsdata_xl_311 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Accountsdata_xl_311') }}

),

DynamicRename_312 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    F1 AS "EFF DATE",
    F2 AS STATUS,
    F3 AS DESCR,
    F4 AS "SHORT DESC",
    F5 AS "SET ID",
    F6 AS ACCT
  
  FROM Accountsdata_xl_311 AS in0

),

DynamicRename_312_row_number AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_312'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_312_filter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM DynamicRename_312_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_312_drop_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_312_filter AS in0

),

AlteryxSelect_313 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "SET ID" AS "SET ID",
    ACCT AS ACCT
  
  FROM DynamicRename_312_drop_0 AS in0

),

DynamicRename_314 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_313'], 
      ['SET ID', 'ACCT'], 
      'advancedRename', 
      ['SET ID', 'ACCT'], 
      'Suffix', 
      '', 
      "concat(column_name, '_Acct')"
    )
  }}

),

Formula_304_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    CAST((SUBSTRING("BUSINESS UNIT", 1, ((CHARINDEX(':', "BUSINESS UNIT")) - 1))) AS STRING) AS "BUSINESS UNIT EXT",
    CAST((GET((SPLIT((REGEXP_REPLACE("COMBOSLASHACCT CODE", '_', ' ')), '\\s+')), CAST(2 AS INTEGER))) AS STRING) AS "ACCT CODE",
    *
  
  FROM AlteryxSelect_515 AS in0

),

DynamicRename_309 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Formula_304_0'], 
      [
        'DOB', 
        'SAL PLAN', 
        'ENTRY DATE', 
        'COUNTRY2', 
        'MAIL DROP', 
        'REASON', 
        'COMP RATE', 
        'LOCATION', 
        'SUPV ID', 
        'DEPARTMENT', 
        'STND HRSMT', 
        'FSLASHP', 
        'INCR DT', 
        'ALIGHT BENCODE', 
        'FIN SECTOR', 
        'LOCATION SET ID', 
        'COMBOSLASHACCT CODE', 
        'NAME', 
        'ADDRESS 2', 
        'FIRST NAME', 
        'DEPTIDCOMMA4COMMA1', 
        'LAST NAME', 
        'SHFT RATE', 
        'TITLE', 
        'MAR STATUS', 
        'FRINGE CD', 
        'ES BA FUNCTION HREP', 
        'SUPERVISOR NAME', 
        'HOME PHONE O', 
        'COUNTRY', 
        'EMPLOYEE ID', 
        'GLC CODE', 
        'FLSA STAT', 
        'REHIRE DT', 
        'SUBFUNCTION', 
        'ELIG FLD 1', 
        'ADDRESS 3', 
        'WORK SCHEDULE', 
        'ELIG FID 2', 
        'FTE', 
        'LAST FOUR OF SSH', 
        'SEQUERCE', 
        'HREPID', 
        'BARG UNIT', 
        'STATE2', 
        'ELIA FLD 9', 
        'BUSINESS UNIT EXT', 
        'DEPT FUNCTION', 
        'EMP ID', 
        'WORK PHONE', 
        'TAX LOC', 
        'WIIRS COMP', 
        'ELIG FID 5', 
        'DEPT', 
        'CHNG AMT', 
        'YEAR', 
        'ELIG FID', 
        'GRADE', 
        'EMPL CLASS', 
        'DEPT DATE', 
        'HRBP NAME', 
        'VIT TCG', 
        'ELIG FID A', 
        'BUSINESS UNIT2', 
        'DEPTIDS2', 
        'SSA', 
        'ELIG FLD 7', 
        'CO', 
        'UNION CODE', 
        'ADDRESS 4', 
        'LOC DESCR', 
        'BUSINESS UNIT', 
        'JOB TITLE', 
        'UNION SENIORITY DATE', 
        'CITIZENSHIP STATUS', 
        'AGE', 
        'SALARY SET ID', 
        'ORIG MIRE DATE', 
        'BA', 
        'DIRSLASHIND', 
        'JOB CODE', 
        'SENICE MONT', 
        'COMPANY SENIORITY D', 
        'EMAL', 
        'BID CODE', 
        'CITY', 
        'BIRTHPLACE', 
        'LABOR CODE', 
        'RSLASHT', 
        'WORK PERIOD', 
        'ELIG FID 4', 
        'JOB FUND', 
        'TERM DATE', 
        'SHIFT', 
        'POSTAL', 
        'SHIFT FCTR', 
        'MIRE DATE', 
        'ACTION DATE', 
        'STATE', 
        'BIRTH MM-DD', 
        'ACCT CODE', 
        'ALIGHT KCODE', 
        'SEX', 
        'EEO-1 CAT', 
        'PAY GROUP', 
        'ACTION', 
        'JOB GROUP', 
        'ADDRESS 1', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'MIDDLE', 
        'HR SECTOR', 
        'SERVICE DT', 
        'ANN BEN RT', 
        'TIME IN HR', 
        'PAY STATUS', 
        'GRADE DATE', 
        'SUPV EMAIL'
      ], 
      'advancedRename', 
      [
        'BUSINESS UNIT EXT', 
        'ACCT CODE', 
        'JOB CODE', 
        'EMPLOYEE ID', 
        'DOB', 
        'GRADE DATE', 
        'INCR DT', 
        'SUPV ID', 
        'WORK PERIOD', 
        'BID CODE', 
        'ELIG FID', 
        'ELIG FID A', 
        'COMP RATE', 
        'ES BA FUNCTION HREP', 
        'BUSINESS UNIT', 
        'STATE2', 
        'ELIG FID 5', 
        'COMBOSLASHACCT CODE', 
        'CHNG AMT', 
        'REHIRE DT', 
        'GLC CODE', 
        'SAL PLAN', 
        'EMP ID', 
        'MIDDLE', 
        'ALIGHT BENCODE', 
        'SHIFT FCTR', 
        'FRINGE CD', 
        'HR SECTOR', 
        'NAME', 
        'UNION CODE', 
        'ELIG FLD 1', 
        'TAX LOC', 
        'ELIA FLD 9', 
        'LAST NAME', 
        'FLSA STAT', 
        'DEPT FUNCTION', 
        'COMPANY SENIORITY D', 
        'CITY', 
        'ELIG FID 4', 
        'HOME PHONE O', 
        'TIME IN HR', 
        'SUBFUNCTION', 
        'ADDRESS 1', 
        'PAY GROUP', 
        'STND HRSMT', 
        'DEPT DATE', 
        'FTE', 
        'EEO-1 CAT', 
        'SUPERVISOR NAME', 
        'GRADE', 
        'HREPID', 
        'ACTION', 
        'HRBP NAME', 
        'EMPL CLASS', 
        'TITLE', 
        'SEQUERCE', 
        'BIRTHPLACE', 
        'JOB FUND', 
        'WORK PHONE', 
        'AGE', 
        'SEX', 
        'VIT TCG', 
        'YEAR', 
        'DEPTIDS2', 
        'SSA', 
        'JOB GROUP', 
        'ADDRESS 4', 
        'EMAL', 
        'DEPTIDCOMMA4COMMA1', 
        'JOB TITLE', 
        'SERVICE DT', 
        'FSLASHP', 
        'MAIL DROP', 
        'LOC DESCR', 
        'SALARY SET ID', 
        'FIRST NAME', 
        'WIIRS COMP', 
        'BA', 
        'PAY STATUS', 
        'DEPT', 
        'BUSINESS UNIT2', 
        'ADDRESS 3', 
        'SUPV EMAIL', 
        'SENICE MONT', 
        'CO', 
        'LOCATION', 
        'UNION SENIORITY DATE', 
        'SHIFT', 
        'ENTRY DATE', 
        'LOCATION SET ID', 
        'BIRTH MM-DD', 
        'ELIG FID 2', 
        'LAST FOUR OF SSH', 
        'MIRE DATE', 
        'ALIGHT KCODE', 
        'FIN SECTOR', 
        'CITIZENSHIP STATUS', 
        'ACTION DATE', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'COUNTRY', 
        'ANN BEN RT', 
        'REASON', 
        'SHFT RATE', 
        'WORK SCHEDULE', 
        'ORIG MIRE DATE', 
        'COUNTRY2', 
        'DEPARTMENT', 
        'ELIG FLD 7', 
        'DIRSLASHIND', 
        'STATE', 
        'RSLASHT', 
        'BARG UNIT', 
        'MAR STATUS', 
        'ADDRESS 2', 
        'LABOR CODE', 
        'TERM DATE', 
        'POSTAL'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_RA')"
    )
  }}

),

DynamicRename_308 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    F1 AS "DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE",
    F2 AS "SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE",
    F3 AS "JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE",
    F4 AS "BU VALUE",
    F5 AS "BU DESCRIPTION",
    F6 AS "COMPANY CRITERIA",
    F7 AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM Gen_00_datakey__28 AS in0

),

DynamicRename_308_row_number AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_308'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_308_filter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM DynamicRename_308_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_308_drop_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_308_filter AS in0

),

AlteryxSelect_307 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "BU VALUE" AS "BU VALUE",
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM DynamicRename_308_drop_0 AS in0

),

DynamicRename_310_before AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE",
    * EXCLUDE ("GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE")
  
  FROM AlteryxSelect_307 AS in0

),

DynamicRename_310 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_310_before'], 
      ['BU VALUE', 'GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE'], 
      'advancedRename', 
      ['GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE', 'BU VALUE'], 
      'Suffix', 
      '', 
      "concat(column_name, '_datakey')"
    )
  }}

),

DynamicRename_310_after AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE",
    *
  
  FROM DynamicRename_310 AS in0

),

Join_315_inner AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM DynamicRename_309 AS in0
  INNER JOIN DynamicRename_310_after AS in1
     ON (in0."BUSINESS UNIT EXT_RA" = in1."BU VALUE_DATAKEY")

),

AlteryxSelect_316 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA",
    "ACCT CODE_RA" AS "ACCT CODE_RA",
    "BU VALUE_DATAKEY" AS "BU VALUE_DATAKEY",
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY"
  
  FROM Join_315_inner AS in0

),

Join_317_left_UnionLeftOuter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_316 AS in0
  LEFT JOIN DynamicRename_314 AS in1
     ON (
      (in0."GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY" = in1."SET ID_ACCT")
      AND (in0."ACCT CODE_RA" = in1.ACCT_ACCT)
    )

),

Formula_319_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    (
      CASE
        WHEN (("SET ID_ACCT" IS NULL))
          THEN 'BU_Acct Code Mismatch'
        ELSE '                     '
      END
    ) AS PROBLEM,
    *
  
  FROM Join_317_left_UnionLeftOuter AS in0

),

Filter_320 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM Formula_319_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

AlteryxSelect_321 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "ACCT CODE_RA" AS "ACCT CODE_RA",
    "BU VALUE_DATAKEY" AS "BU VALUE_DATAKEY",
    PROBLEM AS PROBLEM,
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA"
  
  FROM Filter_320 AS in0

),

DynamicRename_455 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    F1 AS "DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE",
    F2 AS "SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE",
    F3 AS "JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE",
    F4 AS "BU VALUE",
    F5 AS "BU DESCRIPTION",
    F6 AS "COMPANY CRITERIA",
    F7 AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM Gen_00_datakey__28 AS in0

),

DynamicRename_455_row_number AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_455'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_455_filter AS (

  {#VisualGroup: AccountCode#}
  SELECT * 
  
  FROM DynamicRename_455_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_455_drop_0 AS (

  {#VisualGroup: AccountCode#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_455_filter AS in0

),

AlteryxSelect_454 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "BU VALUE" AS "BU VALUE",
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM DynamicRename_455_drop_0 AS in0

),

DynamicRename_456_before AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE",
    * EXCLUDE ("GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE")
  
  FROM AlteryxSelect_454 AS in0

),

DynamicRename_456 AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_456_before'], 
      ['BU VALUE', 'GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE'], 
      'advancedRename', 
      ['GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE', 'BU VALUE'], 
      'Suffix', 
      '', 
      "concat(column_name, '_datakey')"
    )
  }}

),

DynamicRename_456_after AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE",
    *
  
  FROM DynamicRename_456 AS in0

),

AlteryxSelect_398 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "COMBOSLASHACCT CODE" AS "COMBOSLASHACCT CODE",
    "BUSINESS UNIT" AS "BUSINESS UNIT",
    "EMP ID" AS "EMP ID",
    "EMPLOYEE ID" AS "EMPLOYEE ID",
    CAST(NULL AS STRING) AS "FINANCIAL COST CENTER"
  
  FROM AlteryxSelect_397 AS in0

),

DynamicRename_404 AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_398'], 
      ['COMBOSLASHACCT CODE', 'EMPLOYEE ID', 'EMP ID', 'FINANCIAL COST CENTER', 'BUSINESS UNIT'], 
      'advancedRename', 
      ['COMBOSLASHACCT CODE', 'BUSINESS UNIT', 'EMP ID', 'EMPLOYEE ID', 'FINANCIAL COST CENTER'], 
      'Suffix', 
      '', 
      "concat(column_name, '_RA')"
    )
  }}

),

RegEx_405 AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.Regex(
      ['DynamicRename_404'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '([a-zA-Z0-9]+)' }], 
      '[{"name": "FINANCIAL COST CENTER_RA", "dataType": "String"}, {"name": "EMP ID_RA", "dataType": "String"}, {"name": "EMPLOYEE ID_RA", "dataType": "String"}, {"name": "BUSINESS UNIT_RA", "dataType": "String"}, {"name": "COMBOSLASHACCT CODE_RA", "dataType": "String"}]', 
      'COMBOSLASHACCT CODE_RA', 
      '.*?_([a-zA-Z0-9]+)_.*', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      'EX_ACCT_RA', 
      '', 
      false
    )
  }}

),

RegEx_406 AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.Regex(
      ['RegEx_405'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.*?)' }], 
      '[{"name": "FINANCIAL COST CENTER_RA", "dataType": "String"}, {"name": "EMP ID_RA", "dataType": "String"}, {"name": "EMPLOYEE ID_RA", "dataType": "String"}, {"name": "BUSINESS UNIT_RA", "dataType": "String"}, {"name": "COMBOSLASHACCT CODE_RA", "dataType": "String"}, {"name": "EX_ACCT_RA1", "dataType": "String"}]', 
      'BUSINESS UNIT_RA', 
      '^(.*?):', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      'BUS_UNITEXT_RA', 
      '', 
      false
    )
  }}

),

Join_460_inner AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    in0.*,
    in1.*
  
  FROM RegEx_406 AS in0
  INNER JOIN DynamicRename_456_after AS in1
     ON (in0.BUS_UNITEXT_RA1 = in1."BU VALUE_DATAKEY")

),

AlteryxSelect_461 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    EX_ACCT_RA1 AS EX_ACCT_RA1,
    "BU VALUE_DATAKEY" AS "BU VALUE_DATAKEY",
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA",
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY"
  
  FROM Join_460_inner AS in0

),

DynamicRename_457 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    F1 AS "EFF DATE",
    F2 AS STATUS,
    F3 AS DESCR,
    F4 AS "SHORT DESC",
    F5 AS "SET ID",
    F6 AS ACCT
  
  FROM Accountsdata_xl_311 AS in0

),

DynamicRename_457_row_number AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_457'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_457_filter AS (

  {#VisualGroup: AccountCode#}
  SELECT * 
  
  FROM DynamicRename_457_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_457_drop_0 AS (

  {#VisualGroup: AccountCode#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_457_filter AS in0

),

AlteryxSelect_458 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "SET ID" AS "SET ID",
    ACCT AS ACCT
  
  FROM DynamicRename_457_drop_0 AS in0

),

DynamicRename_459 AS (

  {#VisualGroup: AccountCode#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_458'], 
      ['SET ID', 'ACCT'], 
      'advancedRename', 
      ['SET ID', 'ACCT'], 
      'Suffix', 
      '', 
      "concat(column_name, '_Acct')"
    )
  }}

),

Join_462_inner AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_461 AS in0
  INNER JOIN DynamicRename_459 AS in1
     ON (in0."GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY" = in1."SET ID_ACCT")

),

Formula_463_0 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    (
      CASE
        WHEN (EX_ACCT_RA1 = ACCT_ACCT)
          THEN 't'
        ELSE 'f'
      END
    ) AS PROBLEMS,
    *
  
  FROM Join_462_inner AS in0

),

Summarize_465 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    MAX(PROBLEMS) AS MAX_PROBLEMS,
    "EMP ID_RA" AS "EMP ID_RA"
  
  FROM Formula_463_0 AS in0
  
  GROUP BY "EMP ID_RA"

),

Formula_466_0 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    (
      CASE
        WHEN (MAX_PROBLEMS = 'f')
          THEN 'Account code'
        ELSE '            '
      END
    ) AS PROBLEM,
    *
  
  FROM Summarize_465 AS in0

),

Filter_467 AS (

  {#VisualGroup: AccountCode#}
  SELECT * 
  
  FROM Formula_466_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

Summarize_468 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    DISTINCT "EMP ID_RA" AS "EMP ID_RA",
    EX_ACCT_RA1 AS EX_ACCT_RA1,
    "BU VALUE_DATAKEY" AS "BU VALUE_DATAKEY"
  
  FROM Formula_463_0 AS in0

),

Join_469_inner AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    in0.*,
    in1.* EXCLUDE ("EMP ID_RA")
  
  FROM Summarize_468 AS in0
  INNER JOIN Filter_467 AS in1
     ON (in0."EMP ID_RA" = in1."EMP ID_RA")

),

AlteryxSelect_562 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM
  
  FROM Join_469_inner AS in0

),

Summarize_580 AS (

  {#VisualGroup: AccountCode#}
  SELECT 
    DISTINCT "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM
  
  FROM AlteryxSelect_562 AS in0

),

Formula_2_0 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    (
      CASE
        WHEN (
          (
            (
              (
                (("BUSINESS UNIT" = 'Ship Repair') AND ("EMPL CLASS" = 'Bargaining Unit employee'))
                AND ("UNION CODE" = 'None')
              )
              OR ("EMPL CLASS" <> 'Bargaining Unit employee')
            )
            OR (("EMPL CLASS" IS NULL))
          )
          AND (("UNION CODE" <> 'None') OR (("UNION CODE" IS NULL)))
        )
          THEN 'Empl Class Mismatch'
        ELSE '                   '
      END
    ) AS "EMPL CLASS MISMATCH FLAG",
    (
      CASE
        WHEN (CONTAINS((coalesce(LOWER("JOB CODE"), '')), LOWER('intern')))
          THEN 'Intern/Regular Flag'
        ELSE '                   '
      END
    ) AS "INTERNSLASHREGULAR FLAG",
    (
      CASE
        WHEN (CONTAINS((coalesce(LOWER("FLSA STAT"), '')), LOWER('Any')))
          THEN 'Shift Flag'
        ELSE '          '
      END
    ) AS "SHIFT FLAG",
    (
      CASE
        WHEN (("FLSA STAT" = 'Exempt FLSA') AND ("UNION CODE" = 'None'))
          THEN 'Union/FLSA Mismatch'
        ELSE '                   '
      END
    ) AS "UNION FLSA MISMATCH  FLAG ",
    (
      CASE
        WHEN (
          ((CONTAINS(UPPER("FIRST NAME"), "FIRST NAME")) AND (CONTAINS(UPPER("LAST NAME"), "LAST NAME")))
          AND (CONTAINS(UPPER(MIDDLE), MIDDLE))
        )
          THEN 'ALL Caps Name Flag'
        ELSE '                  '
      END
    ) AS "ALL CAPS NAME FLAG",
    (
      CASE
        WHEN (
          ((CONTAINS(LOWER("FIRST NAME"), "FIRST NAME")) AND (CONTAINS(LOWER("LAST NAME"), "LAST NAME")))
          AND (CONTAINS(LOWER(MIDDLE), MIDDLE))
        )
          THEN 'ALL Lower case Name Flag'
        ELSE '                        '
      END
    ) AS "ALL LOWER CASE NAME FLAG ",
    (
      CASE
        WHEN (CONTAINS((coalesce(LOWER(MIDDLE), '')), LOWER('NMN')))
          THEN 'Middle Initial NMN'
        ELSE '                  '
      END
    ) AS "MIDDLE INITIAL NMN FLAG",
    CAST(TRIM((SUBSTRING("FIRST NAME", (((CHARINDEX(' ', "FIRST NAME")) - 1) + 1), (LENGTH("FIRST NAME"))))) AS STRING) AS "GET SUFFIX",
    *
  
  FROM AlteryxSelect_515 AS in0

),

Formula_2_1 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    CAST((REGEXP_REPLACE((REGEXP_REPLACE("GET SUFFIX", ',', '')), '.', '')) AS STRING) AS "GET SUFFIX",
    * EXCLUDE ("GET SUFFIX")
  
  FROM Formula_2_0 AS in0

),

Formula_2_2 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    (
      CASE
        WHEN (CAST("GET SUFFIX" AS STRING) IN ('JR', 'SR', 'II', 'III', 'IV'))
          THEN 'Suffix in First Name'
        ELSE ''
      END
    ) AS "SUFFIX IN FIRST NAME FLAG",
    CAST(TRIM((SUBSTRING("LAST NAME", (((CHARINDEX(' ', "LAST NAME")) - 1) + 1), (LENGTH("LAST NAME"))))) AS STRING) AS "GET SUFFIX LAST NAME",
    *
  
  FROM Formula_2_1 AS in0

),

Formula_2_3 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    CAST((REGEXP_REPLACE((REGEXP_REPLACE("GET SUFFIX LAST NAME", ',', '')), '.', '')) AS STRING) AS "GET SUFFIX LAST NAME",
    * EXCLUDE ("GET SUFFIX LAST NAME")
  
  FROM Formula_2_2 AS in0

),

Formula_2_4 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    (
      CASE
        WHEN (CAST("GET SUFFIX LAST NAME" AS STRING) IN ('JR', 'SR', 'II', 'III', 'IV'))
          THEN 'Suffix in Last Name'
        ELSE ''
      END
    ) AS "SUFFIX IN LAST NAME FLAG",
    (
      CASE
        WHEN ((("BUSINESS UNIT" = 'Ship Repair') AND ("FLSA STAT" = 'Non Exempt')) AND ("EMPL CLASS" = 'Salaried'))
          THEN 'Empl Class/FLSA Mismatch Flag'
        ELSE '                             '
      END
    ) AS "EMPL CLASSSLASHFLSA MISMATCH FLAG",
    (
      CASE
        WHEN ((CO = '130') AND (("WORK SCHEDULE" <> '5/40') OR (("WORK SCHEDULE" IS NULL))))
          THEN 'PT Not in 5/40 Flag'
        ELSE '                   '
      END
    ) AS "PT NOT IN 5SLASH40 FLAG",
    CAST(CO AS STRING) AS "COMPANY CODE",
    *
  
  FROM Formula_2_3 AS in0

),

AlteryxSelect_15 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    "EMP ID" AS "EMP ID",
    "EMPL CLASS MISMATCH FLAG" AS "EMPL CLASS MISMATCH FLAG",
    "INTERNSLASHREGULAR FLAG" AS "INTERNSLASHREGULAR FLAG",
    "SHIFT FLAG" AS "SHIFT FLAG",
    "UNION FLSA MISMATCH  FLAG " AS "UNION FLSA MISMATCH  FLAG ",
    "ALL CAPS NAME FLAG" AS "ALL CAPS NAME FLAG",
    "ALL LOWER CASE NAME FLAG " AS "ALL LOWER CASE NAME FLAG ",
    "MIDDLE INITIAL NMN FLAG" AS "MIDDLE INITIAL NMN FLAG",
    "SUFFIX IN FIRST NAME FLAG" AS "SUFFIX IN FIRST NAME FLAG",
    "SUFFIX IN LAST NAME FLAG" AS "SUFFIX IN LAST NAME FLAG",
    "EMPL CLASSSLASHFLSA MISMATCH FLAG" AS "EMPL CLASSSLASHFLSA MISMATCH FLAG",
    "PT NOT IN 5SLASH40 FLAG" AS "PT NOT IN 5SLASH40 FLAG",
    "EMPLOYEE ID" AS "EMPLOYEE ID",
    CAST(NULL AS STRING) AS "DUPLICATE EID",
    CAST(NULL AS STRING) AS "L9 OR C IN EID FLAG ",
    CAST(NULL AS STRING) AS "REMOTE WORKER FLAG   ",
    CAST(NULL AS STRING) AS "NON HRLDP IN HR  FLAG",
    CAST(NULL AS STRING) AS "TWO FIRST NAMES FLAG",
    CAST(NULL AS STRING) AS "TWO LAST NAME FLAG",
    CAST(NULL AS STRING) AS "VALIDATE START DATE FLAG",
    CAST(NULL AS STRING) AS "LOWERCASESLASHUPPERCASE MIDDLE NAME FLAG",
    CAST(NULL AS STRING) AS "UPCOMING CNT CONVERSION FLAG"
  
  FROM Formula_2_4 AS in0

),

Transpose_4 AS (

  {#VisualGroup: RAProblems#}
  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_15'], 
      ['EMP ID'], 
      ['EMPL CLASS MISMATCH FLAG', 'EMP ID', 'INTERNSLASHREGULAR FLAG', 'SHIFT FLAG'], 
      'NAME', 
      '"VALUE"', 
      [
        'EMP ID', 
        'EMPL CLASS MISMATCH FLAG', 
        'INTERNSLASHREGULAR FLAG', 
        'SHIFT FLAG', 
        'UNION FLSA MISMATCH  FLAG ', 
        'ALL CAPS NAME FLAG', 
        'ALL LOWER CASE NAME FLAG ', 
        'MIDDLE INITIAL NMN FLAG', 
        'SUFFIX IN FIRST NAME FLAG', 
        'SUFFIX IN LAST NAME FLAG', 
        'EMPL CLASSSLASHFLSA MISMATCH FLAG', 
        'PT NOT IN 5SLASH40 FLAG', 
        'EMPLOYEE ID', 
        'DUPLICATE EID', 
        'L9 OR C IN EID FLAG ', 
        'REMOTE WORKER FLAG   ', 
        'NON HRLDP IN HR  FLAG', 
        'TWO FIRST NAMES FLAG', 
        'TWO LAST NAME FLAG', 
        'VALIDATE START DATE FLAG', 
        'LOWERCASESLASHUPPERCASE MIDDLE NAME FLAG', 
        'UPCOMING CNT CONVERSION FLAG'
      ], 
      true
    )
  }}

),

Filter_5 AS (

  {#VisualGroup: RAProblems#}
  SELECT * 
  
  FROM Transpose_4 AS in0
  
  WHERE (
          NOT(
            (LENGTH(VALUE)) = 0)
        )

),

AlteryxSelect_24 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    VALUE AS PROBLEM,
    * EXCLUDE ("NAME", "VALUE")
  
  FROM Filter_5 AS in0

),

AlteryxSelect_480 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    PROBLEM AS PROBLEM,
    "EMP ID" AS "EMP ID"
  
  FROM AlteryxSelect_24 AS in0

),

Summarize_481 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    LISTAGG(PROBLEM, ' ') AS PROBLEM,
    "EMP ID" AS "EMP ID"
  
  FROM AlteryxSelect_480 AS in0
  
  GROUP BY "EMP ID"

),

DynamicRename_521 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT * 
  
  FROM Gen_00_datakey__28 AS in0

),

DynamicRename_521_row_number AS (

  {#VisualGroup: BUBAMismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_521'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_521_filter AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT * 
  
  FROM DynamicRename_521_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_521_drop_0 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_521_filter AS in0

),

DynamicRename_522 AS (

  {#VisualGroup: BUBAMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_521_drop_0'], 
      [
        'SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE', 
        'JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE', 
        'BU DESCRIPTION', 
        'GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE', 
        'BU VALUE', 
        'DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE', 
        'COMPANY CRITERIA'
      ], 
      'advancedRename', 
      [
        'BU VALUE', 
        'BU DESCRIPTION', 
        'COMPANY CRITERIA', 
        'SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE', 
        'DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE', 
        'JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE', 
        'GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_datakey')"
    )
  }}

),

Formula_519_0 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT 
    CAST((SUBSTRING("BUSINESS UNIT", 1, ((CHARINDEX(':', "BUSINESS UNIT")) - 1))) AS STRING) AS "BUSINESS UNIT EXT",
    *
  
  FROM AlteryxSelect_397 AS in0

),

DynamicRename_520 AS (

  {#VisualGroup: BUBAMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Formula_519_0'], 
      [
        'DOB', 
        'SAL PLAN', 
        'ENTRY DATE', 
        'COUNTRY2', 
        'MAIL DROP', 
        'REASON', 
        'COMP RATE', 
        'LOCATION', 
        'SUPV ID', 
        'DEPARTMENT', 
        'STND HRSMT', 
        'FSLASHP', 
        'INCR DT', 
        'ALIGHT BENCODE', 
        'FIN SECTOR', 
        'LOCATION SET ID', 
        'COMBOSLASHACCT CODE', 
        'NAME', 
        'ADDRESS 2', 
        'FIRST NAME', 
        'DEPTIDCOMMA4COMMA1', 
        'LAST NAME', 
        'SHFT RATE', 
        'TITLE', 
        'MAR STATUS', 
        'FRINGE CD', 
        'ES BA FUNCTION HREP', 
        'SUPERVISOR NAME', 
        'HOME PHONE O', 
        'COUNTRY', 
        'EMPLOYEE ID', 
        'GLC CODE', 
        'FLSA STAT', 
        'REHIRE DT', 
        'SUBFUNCTION', 
        'ELIG FLD 1', 
        'ADDRESS 3', 
        'WORK SCHEDULE', 
        'ELIG FID 2', 
        'FTE', 
        'LAST FOUR OF SSH', 
        'SEQUERCE', 
        'HREPID', 
        'BARG UNIT', 
        'STATE2', 
        'ELIA FLD 9', 
        'BUSINESS UNIT EXT', 
        'DEPT FUNCTION', 
        'EMP ID', 
        'WORK PHONE', 
        'TAX LOC', 
        'WIIRS COMP', 
        'ELIG FID 5', 
        'DEPT', 
        'CHNG AMT', 
        'YEAR', 
        'ELIG FID', 
        'GRADE', 
        'EMPL CLASS', 
        'DEPT DATE', 
        'HRBP NAME', 
        'VIT TCG', 
        'ELIG FID A', 
        'BUSINESS UNIT2', 
        'DEPTIDS2', 
        'SSA', 
        'ELIG FLD 7', 
        'CO', 
        'UNION CODE', 
        'ADDRESS 4', 
        'LOC DESCR', 
        'BUSINESS UNIT', 
        'JOB TITLE', 
        'UNION SENIORITY DATE', 
        'CITIZENSHIP STATUS', 
        'AGE', 
        'SALARY SET ID', 
        'ORIG MIRE DATE', 
        'BA', 
        'DIRSLASHIND', 
        'JOB CODE', 
        'SENICE MONT', 
        'COMPANY SENIORITY D', 
        'EMAL', 
        'BID CODE', 
        'CITY', 
        'BIRTHPLACE', 
        'LABOR CODE', 
        'RSLASHT', 
        'WORK PERIOD', 
        'ELIG FID 4', 
        'JOB FUND', 
        'TERM DATE', 
        'SHIFT', 
        'POSTAL', 
        'SHIFT FCTR', 
        'MIRE DATE', 
        'ACTION DATE', 
        'STATE', 
        'BIRTH MM-DD', 
        'ALIGHT KCODE', 
        'SEX', 
        'EEO-1 CAT', 
        'PAY GROUP', 
        'ACTION', 
        'JOB GROUP', 
        'ADDRESS 1', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'MIDDLE', 
        'HR SECTOR', 
        'SERVICE DT', 
        'ANN BEN RT', 
        'TIME IN HR', 
        'PAY STATUS', 
        'GRADE DATE', 
        'SUPV EMAIL'
      ], 
      'advancedRename', 
      [
        'BUSINESS UNIT EXT', 
        'COMBOSLASHACCT CODE', 
        'JOB CODE', 
        'EMPLOYEE ID', 
        'DOB', 
        'GRADE DATE', 
        'INCR DT', 
        'SUPV ID', 
        'WORK PERIOD', 
        'BID CODE', 
        'ELIG FID', 
        'ELIG FID A', 
        'COMP RATE', 
        'ES BA FUNCTION HREP', 
        'BUSINESS UNIT', 
        'STATE2', 
        'ELIG FID 5', 
        'CHNG AMT', 
        'REHIRE DT', 
        'GLC CODE', 
        'SAL PLAN', 
        'EMP ID', 
        'MIDDLE', 
        'ALIGHT BENCODE', 
        'SHIFT FCTR', 
        'FRINGE CD', 
        'HR SECTOR', 
        'NAME', 
        'UNION CODE', 
        'ELIG FLD 1', 
        'TAX LOC', 
        'ELIA FLD 9', 
        'LAST NAME', 
        'FLSA STAT', 
        'DEPT FUNCTION', 
        'COMPANY SENIORITY D', 
        'CITY', 
        'ELIG FID 4', 
        'HOME PHONE O', 
        'TIME IN HR', 
        'SUBFUNCTION', 
        'ADDRESS 1', 
        'PAY GROUP', 
        'STND HRSMT', 
        'DEPT DATE', 
        'FTE', 
        'EEO-1 CAT', 
        'SUPERVISOR NAME', 
        'GRADE', 
        'HREPID', 
        'ACTION', 
        'HRBP NAME', 
        'EMPL CLASS', 
        'TITLE', 
        'SEQUERCE', 
        'BIRTHPLACE', 
        'JOB FUND', 
        'WORK PHONE', 
        'AGE', 
        'SEX', 
        'VIT TCG', 
        'YEAR', 
        'DEPTIDS2', 
        'SSA', 
        'JOB GROUP', 
        'ADDRESS 4', 
        'EMAL', 
        'DEPTIDCOMMA4COMMA1', 
        'JOB TITLE', 
        'SERVICE DT', 
        'FSLASHP', 
        'MAIL DROP', 
        'LOC DESCR', 
        'SALARY SET ID', 
        'FIRST NAME', 
        'WIIRS COMP', 
        'BA', 
        'PAY STATUS', 
        'DEPT', 
        'BUSINESS UNIT2', 
        'ADDRESS 3', 
        'SUPV EMAIL', 
        'SENICE MONT', 
        'CO', 
        'LOCATION', 
        'UNION SENIORITY DATE', 
        'SHIFT', 
        'ENTRY DATE', 
        'LOCATION SET ID', 
        'BIRTH MM-DD', 
        'ELIG FID 2', 
        'LAST FOUR OF SSH', 
        'MIRE DATE', 
        'ALIGHT KCODE', 
        'FIN SECTOR', 
        'CITIZENSHIP STATUS', 
        'ACTION DATE', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'COUNTRY', 
        'ANN BEN RT', 
        'REASON', 
        'SHFT RATE', 
        'WORK SCHEDULE', 
        'ORIG MIRE DATE', 
        'COUNTRY2', 
        'DEPARTMENT', 
        'ELIG FLD 7', 
        'DIRSLASHIND', 
        'STATE', 
        'RSLASHT', 
        'BARG UNIT', 
        'MAR STATUS', 
        'ADDRESS 2', 
        'LABOR CODE', 
        'TERM DATE', 
        'POSTAL'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_RA')"
    )
  }}

),

Join_523_left_UnionLeftOuter AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM DynamicRename_520 AS in0
  LEFT JOIN DynamicRename_522 AS in1
     ON (
      (in0."BUSINESS UNIT EXT_RA" = in1."BU VALUE_DATAKEY")
      AND (in0."BUSINESS UNIT_RA" = in1."BU DESCRIPTION_DATAKEY")
    )

),

Formula_525_0 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT 
    (
      CASE
        WHEN (("BU DESCRIPTION_DATAKEY" IS NULL))
          THEN 'BU/BA Mismatch'
        ELSE '              '
      END
    ) AS PROBLEM,
    *
  
  FROM Join_523_left_UnionLeftOuter AS in0

),

Filter_526 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT * 
  
  FROM Formula_525_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

AlteryxSelect_527 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM
  
  FROM Filter_526 AS in0

),

Summarize_528 AS (

  {#VisualGroup: BUBAMismatch#}
  SELECT 
    DISTINCT "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM
  
  FROM AlteryxSelect_527 AS in0

),

Gen_Union_jobs__485 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Gen_Union_jobs__485') }}

),

DynamicRename_493 AS (

  {#VisualGroup: UnionJobError#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Gen_Union_jobs__485'], 
      ['COMPANY', 'JOB TITLE BEGINS WITHSLASHCONTAINS', 'EMPL CLASS', 'UNION CODE', 'GRADE BEGINS WITH'], 
      'advancedRename', 
      ['JOB TITLE BEGINS WITHSLASHCONTAINS', 'UNION CODE', 'EMPL CLASS', 'COMPANY', 'GRADE BEGINS WITH'], 
      'Suffix', 
      '', 
      "concat(column_name, '_', 'union job')"
    )
  }}

),

Filter_494_reject AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM DynamicRename_493 AS in0
  
  WHERE (
          (
            NOT(
              "COMPANY_UNION JOB" = '130')
          ) OR ((("COMPANY_UNION JOB" = '130') IS NULL))
        )

),

DynamicRename_486_before AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    "BIRTH MM-DD" AS "BIRTH MM__DD",
    "EEO-1 CAT" AS "EEO__1 CAT",
    * EXCLUDE ("BIRTH MM-DD", "EEO-1 CAT")
  
  FROM AlteryxSelect_397 AS in0

),

DynamicRename_486 AS (

  {#VisualGroup: UnionJobError#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_486_before'], 
      [
        'DOB', 
        'EEO__1 CAT', 
        'SAL PLAN', 
        'ENTRY DATE', 
        'COUNTRY2', 
        'MAIL DROP', 
        'REASON', 
        'COMP RATE', 
        'LOCATION', 
        'SUPV ID', 
        'DEPARTMENT', 
        'STND HRSMT', 
        'FSLASHP', 
        'INCR DT', 
        'ALIGHT BENCODE', 
        'FIN SECTOR', 
        'LOCATION SET ID', 
        'COMBOSLASHACCT CODE', 
        'NAME', 
        'ADDRESS 2', 
        'FIRST NAME', 
        'DEPTIDCOMMA4COMMA1', 
        'LAST NAME', 
        'SHFT RATE', 
        'TITLE', 
        'MAR STATUS', 
        'FRINGE CD', 
        'ES BA FUNCTION HREP', 
        'SUPERVISOR NAME', 
        'HOME PHONE O', 
        'COUNTRY', 
        'EMPLOYEE ID', 
        'GLC CODE', 
        'FLSA STAT', 
        'REHIRE DT', 
        'SUBFUNCTION', 
        'ELIG FLD 1', 
        'ADDRESS 3', 
        'WORK SCHEDULE', 
        'ELIG FID 2', 
        'FTE', 
        'LAST FOUR OF SSH', 
        'SEQUERCE', 
        'HREPID', 
        'BARG UNIT', 
        'STATE2', 
        'ELIA FLD 9', 
        'DEPT FUNCTION', 
        'EMP ID', 
        'WORK PHONE', 
        'TAX LOC', 
        'WIIRS COMP', 
        'ELIG FID 5', 
        'DEPT', 
        'CHNG AMT', 
        'YEAR', 
        'ELIG FID', 
        'GRADE', 
        'EMPL CLASS', 
        'DEPT DATE', 
        'HRBP NAME', 
        'VIT TCG', 
        'ELIG FID A', 
        'BUSINESS UNIT2', 
        'DEPTIDS2', 
        'SSA', 
        'ELIG FLD 7', 
        'CO', 
        'UNION CODE', 
        'ADDRESS 4', 
        'LOC DESCR', 
        'BUSINESS UNIT', 
        'JOB TITLE', 
        'UNION SENIORITY DATE', 
        'CITIZENSHIP STATUS', 
        'AGE', 
        'SALARY SET ID', 
        'ORIG MIRE DATE', 
        'BA', 
        'DIRSLASHIND', 
        'JOB CODE', 
        'SENICE MONT', 
        'COMPANY SENIORITY D', 
        'EMAL', 
        'BID CODE', 
        'CITY', 
        'BIRTHPLACE', 
        'LABOR CODE', 
        'RSLASHT', 
        'WORK PERIOD', 
        'ELIG FID 4', 
        'JOB FUND', 
        'BIRTH MM__DD', 
        'TERM DATE', 
        'SHIFT', 
        'POSTAL', 
        'SHIFT FCTR', 
        'MIRE DATE', 
        'ACTION DATE', 
        'STATE', 
        'ALIGHT KCODE', 
        'SEX', 
        'PAY GROUP', 
        'ACTION', 
        'JOB GROUP', 
        'ADDRESS 1', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'MIDDLE', 
        'HR SECTOR', 
        'SERVICE DT', 
        'ANN BEN RT', 
        'TIME IN HR', 
        'PAY STATUS', 
        'GRADE DATE', 
        'SUPV EMAIL'
      ], 
      'advancedRename', 
      [
        'BIRTH MM__DD', 
        'EEO__1 CAT', 
        'COMBOSLASHACCT CODE', 
        'JOB CODE', 
        'EMPLOYEE ID', 
        'DOB', 
        'GRADE DATE', 
        'INCR DT', 
        'SUPV ID', 
        'WORK PERIOD', 
        'BID CODE', 
        'ELIG FID', 
        'ELIG FID A', 
        'COMP RATE', 
        'ES BA FUNCTION HREP', 
        'BUSINESS UNIT', 
        'STATE2', 
        'ELIG FID 5', 
        'CHNG AMT', 
        'REHIRE DT', 
        'GLC CODE', 
        'SAL PLAN', 
        'EMP ID', 
        'MIDDLE', 
        'ALIGHT BENCODE', 
        'SHIFT FCTR', 
        'FRINGE CD', 
        'HR SECTOR', 
        'NAME', 
        'UNION CODE', 
        'ELIG FLD 1', 
        'TAX LOC', 
        'ELIA FLD 9', 
        'LAST NAME', 
        'FLSA STAT', 
        'DEPT FUNCTION', 
        'COMPANY SENIORITY D', 
        'CITY', 
        'ELIG FID 4', 
        'HOME PHONE O', 
        'TIME IN HR', 
        'SUBFUNCTION', 
        'ADDRESS 1', 
        'PAY GROUP', 
        'STND HRSMT', 
        'DEPT DATE', 
        'FTE', 
        'SUPERVISOR NAME', 
        'GRADE', 
        'HREPID', 
        'ACTION', 
        'HRBP NAME', 
        'EMPL CLASS', 
        'TITLE', 
        'SEQUERCE', 
        'BIRTHPLACE', 
        'JOB FUND', 
        'WORK PHONE', 
        'AGE', 
        'SEX', 
        'VIT TCG', 
        'YEAR', 
        'DEPTIDS2', 
        'SSA', 
        'JOB GROUP', 
        'ADDRESS 4', 
        'EMAL', 
        'DEPTIDCOMMA4COMMA1', 
        'JOB TITLE', 
        'SERVICE DT', 
        'FSLASHP', 
        'MAIL DROP', 
        'LOC DESCR', 
        'SALARY SET ID', 
        'FIRST NAME', 
        'WIIRS COMP', 
        'BA', 
        'PAY STATUS', 
        'DEPT', 
        'BUSINESS UNIT2', 
        'ADDRESS 3', 
        'SUPV EMAIL', 
        'SENICE MONT', 
        'CO', 
        'LOCATION', 
        'UNION SENIORITY DATE', 
        'SHIFT', 
        'ENTRY DATE', 
        'LOCATION SET ID', 
        'ELIG FID 2', 
        'LAST FOUR OF SSH', 
        'MIRE DATE', 
        'ALIGHT KCODE', 
        'FIN SECTOR', 
        'CITIZENSHIP STATUS', 
        'ACTION DATE', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'COUNTRY', 
        'ANN BEN RT', 
        'REASON', 
        'SHFT RATE', 
        'WORK SCHEDULE', 
        'ORIG MIRE DATE', 
        'COUNTRY2', 
        'DEPARTMENT', 
        'ELIG FLD 7', 
        'DIRSLASHIND', 
        'STATE', 
        'RSLASHT', 
        'BARG UNIT', 
        'MAR STATUS', 
        'ADDRESS 2', 
        'LABOR CODE', 
        'TERM DATE', 
        'POSTAL'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_', 'RA')"
    )
  }}

),

DynamicRename_486_after AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    CAST(NULL AS STRING) AS "BIRTH MM-DD",
    CAST(NULL AS STRING) AS "EEO-1 CAT",
    *
  
  FROM DynamicRename_486 AS in0

),

AlteryxSelect_489 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    "JOB TITLE_RA" AS "JOB TITLE_RA",
    CO_RA AS CO_RA,
    "JOB CODE_RA" AS "JOB CODE_RA",
    "EMP ID_RA" AS "EMP ID_RA",
    "UNION CODE_RA" AS "UNION CODE_RA",
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA",
    CAST(NULL AS STRING) AS COMPANY_RA,
    CAST(NULL AS STRING) AS "UNION JOB_RA"
  
  FROM DynamicRename_486_after AS in0

),

Formula_490_0 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    CAST(CO_RA AS STRING) AS "COMPANY CODE RA",
    *
  
  FROM AlteryxSelect_489 AS in0

),

Formula_490_1 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    (
      CASE
        WHEN ("COMPANY CODE RA" = '130')
          THEN (SUBSTRING("JOB TITLE_RA", 1, ((CHARINDEX(' ', "JOB TITLE_RA")) - 1)))
        ELSE (SUBSTRING("JOB CODE_RA", 1, ((CHARINDEX('_', "JOB CODE_RA")) - 1)))
      END
    ) AS "JOB TITLESLASHJOB CODE RA",
    *
  
  FROM Formula_490_0 AS in0

),

Filter_491_reject AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM Formula_490_1 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  "COMPANY CODE RA" = '130')
              ) OR (("COMPANY CODE RA" IS NULL))
            )
            OR ((("COMPANY CODE RA" = '130') IS NULL))
          )
          AND (CAST("COMPANY CODE RA" AS STRING) IN ('130', '155', '251'))
        )

),

Join_499_inner AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_491_reject AS in0
  INNER JOIN Filter_494_reject AS in1
     ON (
      (in0."COMPANY CODE RA" = in1."COMPANY_UNION JOB")
      AND (in0."UNION CODE_RA" = in1."UNION CODE_UNION JOB")
    )

),

Formula_500_0 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    (
      CASE
        WHEN ("COMPANY CODE RA" = '251')
          THEN (
            CASE
              WHEN (((LENGTH("JOB TITLESLASHJOB CODE RA")) = 5) AND ((SUBSTRING("JOB TITLESLASHJOB CODE RA", 1, 1)) = 2))
                THEN '               '
              ELSE 'Union job Error'
            END
          )
        WHEN ("COMPANY CODE RA" = '155')
          THEN (
            CASE
              WHEN (
                ((LENGTH("JOB TITLESLASHJOB CODE RA")) = 4)
                AND (
                      ((SUBSTRING("JOB TITLESLASHJOB CODE RA", 1, 3)) <> 'SC2')
                      OR (((SUBSTRING("JOB TITLESLASHJOB CODE RA", 1, 3)) IS NULL))
                    )
              )
                THEN '               '
              ELSE 'Union job Error'
            END
          )
        ELSE '               '
      END
    ) AS PROBLEMS,
    *
  
  FROM Join_499_inner AS in0

),

Filter_501 AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM Formula_500_0 AS in0
  
  WHERE (NOT((PROBLEMS IS NULL)))

),

AlteryxSelect_504 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    PROBLEMS AS PROBLEMS,
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA"
  
  FROM Filter_501 AS in0

),

Filter_491 AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM Formula_490_1 AS in0
  
  WHERE (("COMPANY CODE RA" = '130') AND (CAST("COMPANY CODE RA" AS STRING) IN ('130', '155', '251')))

),

Filter_494 AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM DynamicRename_493 AS in0
  
  WHERE ("COMPANY_UNION JOB" = '130')

),

Join_492_left_UnionLeftOuter AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_491 AS in0
  LEFT JOIN Filter_494 AS in1
     ON (
      (
        (in0."COMPANY CODE RA" = in1."COMPANY_UNION JOB")
        AND (in0."JOB CODE_RA" = in1."UNION CODE_UNION JOB")
      )
      AND (in0."JOB TITLESLASHJOB CODE RA" = in1."JOB TITLE BEGINS WITHSLASHCONTAINS_UNION JOB")
    )

),

Formula_483_0 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    (
      CASE
        WHEN (("COMPANY_UNION JOB" IS NULL))
          THEN 'Union job Error'
        ELSE '               '
      END
    ) AS PROBLEMS,
    *
  
  FROM Join_492_left_UnionLeftOuter AS in0

),

Filter_502 AS (

  {#VisualGroup: UnionJobError#}
  SELECT * 
  
  FROM Formula_483_0 AS in0
  
  WHERE (NOT((PROBLEMS IS NULL)))

),

AlteryxSelect_503 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    PROBLEMS AS PROBLEMS
  
  FROM Filter_502 AS in0

),

Union_505 AS (

  {#VisualGroup: UnionJobError#}
  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_503', 'AlteryxSelect_504'], 
      [
        '[{"name": "EMP ID_RA", "dataType": "String"}, {"name": "PROBLEMS", "dataType": "String"}]', 
        '[{"name": "EMP ID_RA", "dataType": "String"}, {"name": "PROBLEMS", "dataType": "String"}, {"name": "EMPLOYEE ID_RA", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_506 AS (

  {#VisualGroup: UnionJobError#}
  SELECT 
    MIN(PROBLEMS) AS PROBLEMS,
    "EMP ID_RA" AS "EMP ID_RA"
  
  FROM Union_505 AS in0
  
  GROUP BY "EMP ID_RA"

),

AlteryxSelect_201 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    CO AS CO,
    "EMP ID" AS "EMP ID",
    DEPARTMENT AS DEPARTMENT,
    "EMPLOYEE ID" AS "EMPLOYEE ID",
    CAST(NULL AS STRING) AS COMPANY
  
  FROM AlteryxSelect_515 AS in0

),

Formula_200_0 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    CAST(CO AS STRING) AS "COMPANY CODE",
    CAST((SUBSTRING(DEPARTMENT, 1, ((CHARINDEX('_', DEPARTMENT)) - 1))) AS STRING) AS "DEPT CODE",
    *
  
  FROM AlteryxSelect_201 AS in0

),

DynamicRename_206 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Formula_200_0'], 
      ['DEPARTMENT', 'COMPANY', 'EMPLOYEE ID', 'DEPT CODE', 'EMP ID', 'COMPANY CODE', 'CO'], 
      'advancedRename', 
      ['COMPANY CODE', 'DEPT CODE', 'CO', 'EMP ID', 'DEPARTMENT', 'EMPLOYEE ID', 'COMPANY'], 
      'Suffix', 
      '', 
      "concat(column_name, '_alpha')"
    )
  }}

),

Join_204_left_UnionLeftOuter AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    in0.*,
    in1.*
  
  FROM DynamicRename_206 AS in0
  LEFT JOIN DynamicRename_205 AS in1
     ON ((in0.CO_ALPHA = in1.COMPANY_DEPT) AND (in0."DEPT CODE_ALPHA" = in1."DEPT ID_DEPT"))

),

Formula_208_0 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    (
      CASE
        WHEN ((COMPANY_DEPT IS NULL))
          THEN 'Dept Code'
        ELSE '         '
      END
    ) AS PROBLEM,
    *
  
  FROM Join_204_left_UnionLeftOuter AS in0

),

Filter_212 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT * 
  
  FROM Formula_208_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

AlteryxSelect_215 AS (

  {#VisualGroup: DeptCodefixedononboarding#}
  SELECT 
    "EMP ID_ALPHA" AS "EMP ID_ALPHA",
    PROBLEM AS PROBLEM,
    CAST(NULL AS STRING) AS "EMP ID_RA",
    CAST(NULL AS STRING) AS "DEPT CODE_RA",
    CAST(NULL AS STRING) AS "EMPLOYEE ID_RA",
    CAST(NULL AS STRING) AS "SET ID_DEPT"
  
  FROM Filter_212 AS in0

),

DynamicRename_385 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    F1 AS "EFF DATE",
    F2 AS STATUS,
    F3 AS DESCR,
    F4 AS "SHORT DESC",
    F5 AS "SET ID",
    F6 AS ACCT
  
  FROM Accountsdata_xl_311 AS in0

),

DynamicRename_385_row_number AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_385'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_385_filter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM DynamicRename_385_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_385_drop_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_385_filter AS in0

),

AlteryxSelect_386 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "SET ID" AS "SET ID",
    ACCT AS ACCT
  
  FROM DynamicRename_385_drop_0 AS in0

),

DynamicRename_387 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_386'], 
      ['SET ID', 'ACCT'], 
      'advancedRename', 
      ['SET ID', 'ACCT'], 
      'Suffix', 
      '', 
      "concat(column_name, '_Acct')"
    )
  }}

),

DynamicRename_381 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    F1 AS "DEPARTMENT PARANTHESESOPENHR-01PARANTHESESCLOSE",
    F2 AS "SET ID FOR LOCATION CODES PARANTHESESOPENHR-03PARANTHESESCLOSE",
    F3 AS "JOB CODE PARANTHESESOPENHR-02PARANTHESESCLOSE",
    F4 AS "BU VALUE",
    F5 AS "BU DESCRIPTION",
    F6 AS "COMPANY CRITERIA",
    F7 AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM Gen_00_datakey__28 AS in0

),

DynamicRename_381_row_number AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.RecordID(
      ['DynamicRename_381'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_381_filter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM DynamicRename_381_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR ((PROPHECY_ROW_ID IS NULL))
        )

),

DynamicRename_381_drop_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_381_filter AS in0

),

AlteryxSelect_380 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "BU VALUE" AS "BU VALUE",
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE"
  
  FROM DynamicRename_381_drop_0 AS in0

),

DynamicRename_383_before AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE" AS "GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE",
    * EXCLUDE ("GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE")
  
  FROM AlteryxSelect_380 AS in0

),

DynamicRename_383 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_383_before'], 
      ['BU VALUE', 'GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE'], 
      'advancedRename', 
      ['GL ACCOUNT TABLE PARANTHESESOPENFS__05PARANTHESESCLOSE', 'BU VALUE'], 
      'Suffix', 
      '', 
      "concat(column_name, '_datakey')"
    )
  }}

),

DynamicRename_383_after AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE",
    *
  
  FROM DynamicRename_383 AS in0

),

Formula_377_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    CAST((SUBSTRING("BUSINESS UNIT", 1, ((CHARINDEX(':', "BUSINESS UNIT")) - 1))) AS STRING) AS "BUSINESS UNIT EXT",
    CAST((GET((SPLIT((REGEXP_REPLACE("COMBOSLASHACCT CODE", '_', ' ')), '\\s+')), CAST(2 AS INTEGER))) AS STRING) AS "ACCT CODE",
    *
  
  FROM AlteryxSelect_397 AS in0

),

DynamicRename_382 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  {{
    prophecy_basics.MultiColumnRename(
      ['Formula_377_0'], 
      [
        'DOB', 
        'SAL PLAN', 
        'ENTRY DATE', 
        'COUNTRY2', 
        'MAIL DROP', 
        'REASON', 
        'COMP RATE', 
        'LOCATION', 
        'SUPV ID', 
        'DEPARTMENT', 
        'STND HRSMT', 
        'FSLASHP', 
        'INCR DT', 
        'ALIGHT BENCODE', 
        'FIN SECTOR', 
        'LOCATION SET ID', 
        'COMBOSLASHACCT CODE', 
        'NAME', 
        'ADDRESS 2', 
        'FIRST NAME', 
        'DEPTIDCOMMA4COMMA1', 
        'LAST NAME', 
        'SHFT RATE', 
        'TITLE', 
        'MAR STATUS', 
        'FRINGE CD', 
        'ES BA FUNCTION HREP', 
        'SUPERVISOR NAME', 
        'HOME PHONE O', 
        'COUNTRY', 
        'EMPLOYEE ID', 
        'GLC CODE', 
        'FLSA STAT', 
        'REHIRE DT', 
        'SUBFUNCTION', 
        'ELIG FLD 1', 
        'ADDRESS 3', 
        'WORK SCHEDULE', 
        'ELIG FID 2', 
        'FTE', 
        'LAST FOUR OF SSH', 
        'SEQUERCE', 
        'HREPID', 
        'BARG UNIT', 
        'STATE2', 
        'ELIA FLD 9', 
        'BUSINESS UNIT EXT', 
        'DEPT FUNCTION', 
        'EMP ID', 
        'WORK PHONE', 
        'TAX LOC', 
        'WIIRS COMP', 
        'ELIG FID 5', 
        'DEPT', 
        'CHNG AMT', 
        'YEAR', 
        'ELIG FID', 
        'GRADE', 
        'EMPL CLASS', 
        'DEPT DATE', 
        'HRBP NAME', 
        'VIT TCG', 
        'ELIG FID A', 
        'BUSINESS UNIT2', 
        'DEPTIDS2', 
        'SSA', 
        'ELIG FLD 7', 
        'CO', 
        'UNION CODE', 
        'ADDRESS 4', 
        'LOC DESCR', 
        'BUSINESS UNIT', 
        'JOB TITLE', 
        'UNION SENIORITY DATE', 
        'CITIZENSHIP STATUS', 
        'AGE', 
        'SALARY SET ID', 
        'ORIG MIRE DATE', 
        'BA', 
        'DIRSLASHIND', 
        'JOB CODE', 
        'SENICE MONT', 
        'COMPANY SENIORITY D', 
        'EMAL', 
        'BID CODE', 
        'CITY', 
        'BIRTHPLACE', 
        'LABOR CODE', 
        'RSLASHT', 
        'WORK PERIOD', 
        'ELIG FID 4', 
        'JOB FUND', 
        'TERM DATE', 
        'SHIFT', 
        'POSTAL', 
        'SHIFT FCTR', 
        'MIRE DATE', 
        'ACTION DATE', 
        'STATE', 
        'BIRTH MM-DD', 
        'ACCT CODE', 
        'ALIGHT KCODE', 
        'SEX', 
        'EEO-1 CAT', 
        'PAY GROUP', 
        'ACTION', 
        'JOB GROUP', 
        'ADDRESS 1', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'MIDDLE', 
        'HR SECTOR', 
        'SERVICE DT', 
        'ANN BEN RT', 
        'TIME IN HR', 
        'PAY STATUS', 
        'GRADE DATE', 
        'SUPV EMAIL'
      ], 
      'advancedRename', 
      [
        'BUSINESS UNIT EXT', 
        'ACCT CODE', 
        'COMBOSLASHACCT CODE', 
        'JOB CODE', 
        'EMPLOYEE ID', 
        'DOB', 
        'GRADE DATE', 
        'INCR DT', 
        'SUPV ID', 
        'WORK PERIOD', 
        'BID CODE', 
        'ELIG FID', 
        'ELIG FID A', 
        'COMP RATE', 
        'ES BA FUNCTION HREP', 
        'BUSINESS UNIT', 
        'STATE2', 
        'ELIG FID 5', 
        'CHNG AMT', 
        'REHIRE DT', 
        'GLC CODE', 
        'SAL PLAN', 
        'EMP ID', 
        'MIDDLE', 
        'ALIGHT BENCODE', 
        'SHIFT FCTR', 
        'FRINGE CD', 
        'HR SECTOR', 
        'NAME', 
        'UNION CODE', 
        'ELIG FLD 1', 
        'TAX LOC', 
        'ELIA FLD 9', 
        'LAST NAME', 
        'FLSA STAT', 
        'DEPT FUNCTION', 
        'COMPANY SENIORITY D', 
        'CITY', 
        'ELIG FID 4', 
        'HOME PHONE O', 
        'TIME IN HR', 
        'SUBFUNCTION', 
        'ADDRESS 1', 
        'PAY GROUP', 
        'STND HRSMT', 
        'DEPT DATE', 
        'FTE', 
        'EEO-1 CAT', 
        'SUPERVISOR NAME', 
        'GRADE', 
        'HREPID', 
        'ACTION', 
        'HRBP NAME', 
        'EMPL CLASS', 
        'TITLE', 
        'SEQUERCE', 
        'BIRTHPLACE', 
        'JOB FUND', 
        'WORK PHONE', 
        'AGE', 
        'SEX', 
        'VIT TCG', 
        'YEAR', 
        'DEPTIDS2', 
        'SSA', 
        'JOB GROUP', 
        'ADDRESS 4', 
        'EMAL', 
        'DEPTIDCOMMA4COMMA1', 
        'JOB TITLE', 
        'SERVICE DT', 
        'FSLASHP', 
        'MAIL DROP', 
        'LOC DESCR', 
        'SALARY SET ID', 
        'FIRST NAME', 
        'WIIRS COMP', 
        'BA', 
        'PAY STATUS', 
        'DEPT', 
        'BUSINESS UNIT2', 
        'ADDRESS 3', 
        'SUPV EMAIL', 
        'SENICE MONT', 
        'CO', 
        'LOCATION', 
        'UNION SENIORITY DATE', 
        'SHIFT', 
        'ENTRY DATE', 
        'LOCATION SET ID', 
        'BIRTH MM-DD', 
        'ELIG FID 2', 
        'LAST FOUR OF SSH', 
        'MIRE DATE', 
        'ALIGHT KCODE', 
        'FIN SECTOR', 
        'CITIZENSHIP STATUS', 
        'ACTION DATE', 
        'JOBCODE SET ID', 
        'DATE ENTERED', 
        'COUNTRY', 
        'ANN BEN RT', 
        'REASON', 
        'SHFT RATE', 
        'WORK SCHEDULE', 
        'ORIG MIRE DATE', 
        'COUNTRY2', 
        'DEPARTMENT', 
        'ELIG FLD 7', 
        'DIRSLASHIND', 
        'STATE', 
        'RSLASHT', 
        'BARG UNIT', 
        'MAR STATUS', 
        'ADDRESS 2', 
        'LABOR CODE', 
        'TERM DATE', 
        'POSTAL'
      ], 
      'Suffix', 
      '', 
      "concat(column_name, '_RA')"
    )
  }}

),

Join_388_inner AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM DynamicRename_382 AS in0
  INNER JOIN DynamicRename_383_after AS in1
     ON (in0."BUSINESS UNIT EXT_RA" = in1."BU VALUE_DATAKEY")

),

AlteryxSelect_389 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    "ACCT CODE_RA" AS "ACCT CODE_RA",
    "BU VALUE_DATAKEY" AS "BU VALUE_DATAKEY",
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA",
    CAST(NULL AS STRING) AS "GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY"
  
  FROM Join_388_inner AS in0

),

Join_390_left_UnionLeftOuter AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_389 AS in0
  LEFT JOIN DynamicRename_387 AS in1
     ON (
      (in0."GL ACCOUNT TABLE PARANTHESESOPENFS-05PARANTHESESCLOSE_DATAKEY" = in1."SET ID_ACCT")
      AND (in0."ACCT CODE_RA" = in1.ACCT_ACCT)
    )

),

Formula_392_0 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    (
      CASE
        WHEN (("SET ID_ACCT" IS NULL))
          THEN 'BU_Acct Code Mismatch'
        ELSE '                     '
      END
    ) AS PROBLEM,
    *
  
  FROM Join_390_left_UnionLeftOuter AS in0

),

Filter_393 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT * 
  
  FROM Formula_392_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

AlteryxSelect_394 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM,
    "EMPLOYEE ID_RA" AS "EMPLOYEE ID_RA"
  
  FROM Filter_393 AS in0

),

Summarize_581 AS (

  {#VisualGroup: BUAcctCodeMismatch#}
  SELECT 
    DISTINCT "EMP ID_RA" AS "EMP ID_RA",
    PROBLEM AS PROBLEM
  
  FROM AlteryxSelect_394 AS in0

),

AlteryxSelect_336 AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__AlteryxSelect_336')}}

),

Filter_347 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT * 
  
  FROM AlteryxSelect_336 AS in0
  
  WHERE ("COMPANY CODE" = '130')

),

Work_ScheduleVa_335 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Work_ScheduleVa_335') }}

),

Filter_345 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT * 
  
  FROM Work_ScheduleVa_335 AS in0
  
  WHERE (NOT(CONTAINS(DESCRIPTION, 'Any Value')))

),

Join_337_left_UnionLeftOuter AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    in0.POSTAL AS POSTAL,
    in0.STATE2 AS STATE2,
    in0."ALIGHT BENCODE" AS "ALIGHT BENCODE",
    in0."EMPL CLASS" AS "EMPL CLASS",
    in0."MAIL DROP" AS "MAIL DROP",
    in0."COMP RATE" AS "COMP RATE",
    in0.CITY AS CITY,
    in0."MIRE DATE" AS "MIRE DATE",
    in0.NAME AS NAME,
    in0."UNION SENIORITY DATE" AS "UNION SENIORITY DATE",
    in0.BA AS BA,
    in0.FTE AS FTE,
    in0.LOCATION AS LOCATION,
    in0."WORK PHONE" AS "WORK PHONE",
    in0.STATE AS STATE,
    in0."SAL PLAN" AS "SAL PLAN",
    in0.CO AS CO,
    in0."ADDRESS 4" AS "ADDRESS 4",
    in0."SERVICE DT" AS "SERVICE DT",
    in0."EMPLOYEE ID" AS "EMPLOYEE ID",
    in0."JOBCODE SET ID" AS "JOBCODE SET ID",
    in0."JOB CODE" AS "JOB CODE",
    in0.DOB AS DOB,
    in1.DESCRIPTION AS DESCRIPTION,
    in0.COMPANY_CODE AS COMPANY_CODE,
    in0."LABOR CODE" AS "LABOR CODE",
    in0."ELIG FID 2" AS "ELIG FID 2",
    in0.HREPID AS HREPID,
    in0."SHFT RATE" AS "SHFT RATE",
    in0."HRBP NAME" AS "HRBP NAME",
    in0."ELIG FLD 1" AS "ELIG FLD 1",
    in0."SUPERVISOR NAME" AS "SUPERVISOR NAME",
    in0."CHNG AMT" AS "CHNG AMT",
    in0.EMAL AS EMAL,
    in0."COMPANY CODE" AS "COMPANY CODE",
    in0."JOB GROUP" AS "JOB GROUP",
    in0.SUBFUNCTION AS SUBFUNCTION,
    in0."PAY STATUS" AS "PAY STATUS",
    in0."ALIGHT KCODE" AS "ALIGHT KCODE",
    in0.SHIFT AS SHIFT,
    in0."LAST NAME" AS "LAST NAME",
    in0."FLSA STAT" AS "FLSA STAT",
    in0."DEPT DATE" AS "DEPT DATE",
    in0."ES BA FUNCTION HREP" AS "ES BA FUNCTION HREP",
    in0.SEQUERCE AS SEQUERCE,
    in0."BUSINESS UNIT" AS "BUSINESS UNIT",
    in0."HR SECTOR" AS "HR SECTOR",
    in0."ENTRY DATE" AS "ENTRY DATE",
    in0."ANN BEN RT" AS "ANN BEN RT",
    in0."YEAR" AS YEAR,
    in0."ORIG MIRE DATE" AS "ORIG MIRE DATE",
    in0."TAX LOC" AS "TAX LOC",
    in0."VIT TCG" AS "VIT TCG",
    in0.DEPT AS DEPT,
    in0."BID CODE" AS "BID CODE",
    in0.DEPTIDS2 AS DEPTIDS2,
    in0.COUNTRY AS COUNTRY,
    in0."EMP ID" AS "EMP ID",
    in0."SHIFT FCTR" AS "SHIFT FCTR",
    in0."ADDRESS 3" AS "ADDRESS 3",
    in0."SALARY SET ID" AS "SALARY SET ID",
    in0.REASON AS REASON,
    in0.AGE AS AGE,
    in0.MIDDLE AS MIDDLE,
    in0.SEX AS SEX,
    in0."DEPT FUNCTION" AS "DEPT FUNCTION",
    in0."FIN SECTOR" AS "FIN SECTOR",
    in0."REHIRE DT" AS "REHIRE DT",
    in0."FIRST NAME" AS "FIRST NAME",
    in0.DEPARTMENT AS DEPARTMENT,
    in0.DEPTIDCOMMA4COMMA1 AS DEPTIDCOMMA4COMMA1,
    in0."ELIA FLD 9" AS "ELIA FLD 9",
    in0."ELIG FID 5" AS "ELIG FID 5",
    in0."HOME PHONE O" AS "HOME PHONE O",
    in0."UNION CODE" AS "UNION CODE",
    in0."EEO-1 CAT" AS "EEO-1 CAT",
    in0."DATE ENTERED" AS "DATE ENTERED",
    in0."JOB TITLE" AS "JOB TITLE",
    in0."PAY GROUP" AS "PAY GROUP",
    in0.GRADE AS GRADE,
    in0."STND HRSMT" AS "STND HRSMT",
    in0."GLC CODE" AS "GLC CODE",
    in0."SUPV EMAIL" AS "SUPV EMAIL",
    in0."TERM DATE" AS "TERM DATE",
    in0."COMPANY SENIORITY D" AS "COMPANY SENIORITY D",
    in0."ELIG FLD 7" AS "ELIG FLD 7",
    in0."ELIG FID" AS "ELIG FID",
    in0.COUNTRY2 AS COUNTRY2,
    in0."BUSINESS UNIT2" AS "BUSINESS UNIT2",
    in0."ELIG FID A" AS "ELIG FID A",
    in0."ADDRESS 2" AS "ADDRESS 2",
    in0."WORK PERIOD" AS "WORK PERIOD",
    in0.RSLASHT AS RSLASHT,
    in0."LOC DESCR" AS "LOC DESCR",
    in0."LOCATION SET ID" AS "LOCATION SET ID",
    in0."JOB FUND" AS "JOB FUND",
    in0."GRADE DATE" AS "GRADE DATE",
    in0."BIRTH MM-DD" AS "BIRTH MM-DD",
    in0."WORK SCHEDULE" AS "WORK SCHEDULE",
    in0."SENICE MONT" AS "SENICE MONT",
    in0."ACTION DATE" AS "ACTION DATE",
    in0.BIRTHPLACE AS BIRTHPLACE,
    in0.TITLE AS TITLE,
    in0."FRINGE CD" AS "FRINGE CD",
    in0."ELIG FID 4" AS "ELIG FID 4",
    in0.DIRSLASHIND AS DIRSLASHIND,
    in0."CITIZENSHIP STATUS" AS "CITIZENSHIP STATUS",
    in0."ADDRESS 1" AS "ADDRESS 1",
    in0."WIIRS COMP" AS "WIIRS COMP",
    in0.SSA AS SSA,
    in0.ACTION AS ACTION,
    in0."INCR DT" AS "INCR DT",
    in0."MAR STATUS" AS "MAR STATUS",
    in0."TIME IN HR" AS "TIME IN HR",
    in0.FSLASHP AS FSLASHP,
    in0."COMBOSLASHACCT CODE" AS "COMBOSLASHACCT CODE",
    in0."BARG UNIT" AS "BARG UNIT",
    in0."SUPV ID" AS "SUPV ID",
    in0."LAST FOUR OF SSH" AS "LAST FOUR OF SSH"
  
  FROM Filter_347 AS in0
  LEFT JOIN Filter_345 AS in1
     ON ((in0."COMPANY CODE" = in1.COMPANY) AND (in0."WORK SCHEDULE" = in1.DESCRIPTION))

),

Filter_347_reject AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__Filter_347_reject')}}

),

Filter_345_reject AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__Filter_345_reject')}}

),

Join_346_left AS (

  {#VisualGroup: WorkSchedule#}
  SELECT in0.*
  
  FROM Filter_347_reject AS in0
  LEFT JOIN Filter_345_reject AS in1
     ON (in0."COMPANY CODE" = in1.COMPANY)

),

Union_338 AS (

  {#VisualGroup: WorkSchedule#}
  {{
    prophecy_basics.UnionByName(
      ['Join_346_left', 'Join_337_left_UnionLeftOuter'], 
      [
        '[{"name": "COMPANY_CODE", "dataType": "String"}, {"name": "COMPANY CODE", "dataType": "String"}, {"name": "JOB CODE", "dataType": "String"}, {"name": "EMPLOYEE ID", "dataType": "String"}, {"name": "DOB", "dataType": "Date"}, {"name": "GRADE DATE", "dataType": "Date"}, {"name": "INCR DT", "dataType": "Date"}, {"name": "SUPV ID", "dataType": "Float"}, {"name": "WORK PERIOD", "dataType": "String"}, {"name": "BID CODE", "dataType": "Float"}, {"name": "ELIG FID", "dataType": "String"}, {"name": "ELIG FID A", "dataType": "String"}, {"name": "COMP RATE", "dataType": "Float"}, {"name": "ES BA FUNCTION HREP", "dataType": "String"}, {"name": "BUSINESS UNIT", "dataType": "String"}, {"name": "STATE2", "dataType": "String"}, {"name": "ELIG FID 5", "dataType": "String"}, {"name": "COMBOSLASHACCT CODE", "dataType": "Float"}, {"name": "CHNG AMT", "dataType": "Float"}, {"name": "REHIRE DT", "dataType": "Date"}, {"name": "GLC CODE", "dataType": "String"}, {"name": "SAL PLAN", "dataType": "String"}, {"name": "EMP ID", "dataType": "Float"}, {"name": "MIDDLE", "dataType": "String"}, {"name": "ALIGHT BENCODE", "dataType": "String"}, {"name": "SHIFT FCTR", "dataType": "String"}, {"name": "FRINGE CD", "dataType": "String"}, {"name": "HR SECTOR", "dataType": "String"}, {"name": "NAME", "dataType": "String"}, {"name": "UNION CODE", "dataType": "String"}, {"name": "ELIG FLD 1", "dataType": "String"}, {"name": "TAX LOC", "dataType": "String"}, {"name": "ELIA FLD 9", "dataType": "String"}, {"name": "LAST NAME", "dataType": "String"}, {"name": "FLSA STAT", "dataType": "String"}, {"name": "DEPT FUNCTION", "dataType": "String"}, {"name": "COMPANY SENIORITY D", "dataType": "Date"}, {"name": "CITY", "dataType": "String"}, {"name": "ELIG FID 4", "dataType": "String"}, {"name": "HOME PHONE O", "dataType": "String"}, {"name": "TIME IN HR", "dataType": "Float"}, {"name": "SUBFUNCTION", "dataType": "String"}, {"name": "ADDRESS 1", "dataType": "String"}, {"name": "PAY GROUP", "dataType": "String"}, {"name": "STND HRSMT", "dataType": "Float"}, {"name": "DEPT DATE", "dataType": "Date"}, {"name": "FTE", "dataType": "Float"}, {"name": "EEO-1 CAT", "dataType": "Float"}, {"name": "SUPERVISOR NAME", "dataType": "String"}, {"name": "GRADE", "dataType": "String"}, {"name": "HREPID", "dataType": "Float"}, {"name": "ACTION", "dataType": "String"}, {"name": "HRBP NAME", "dataType": "String"}, {"name": "EMPL CLASS", "dataType": "String"}, {"name": "TITLE", "dataType": "String"}, {"name": "SEQUERCE", "dataType": "Float"}, {"name": "BIRTHPLACE", "dataType": "String"}, {"name": "JOB FUND", "dataType": "String"}, {"name": "WORK PHONE", "dataType": "String"}, {"name": "AGE", "dataType": "Float"}, {"name": "SEX", "dataType": "String"}, {"name": "VIT TCG", "dataType": "String"}, {"name": "YEAR", "dataType": "Float"}, {"name": "DEPTIDS2", "dataType": "String"}, {"name": "SSA", "dataType": "String"}, {"name": "JOB GROUP", "dataType": "String"}, {"name": "ADDRESS 4", "dataType": "String"}, {"name": "EMAL", "dataType": "String"}, {"name": "DEPTIDCOMMA4COMMA1", "dataType": "String"}, {"name": "JOB TITLE", "dataType": "String"}, {"name": "SERVICE DT", "dataType": "Date"}, {"name": "FSLASHP", "dataType": "String"}, {"name": "MAIL DROP", "dataType": "String"}, {"name": "LOC DESCR", "dataType": "String"}, {"name": "SALARY SET ID", "dataType": "String"}, {"name": "FIRST NAME", "dataType": "String"}, {"name": "WIIRS COMP", "dataType": "Float"}, {"name": "BA", "dataType": "String"}, {"name": "PAY STATUS", "dataType": "String"}, {"name": "DEPT", "dataType": "String"}, {"name": "BUSINESS UNIT2", "dataType": "String"}, {"name": "ADDRESS 3", "dataType": "String"}, {"name": "SUPV EMAIL", "dataType": "String"}, {"name": "SENICE MONT", "dataType": "Float"}, {"name": "CO", "dataType": "String"}, {"name": "LOCATION", "dataType": "String"}, {"name": "UNION SENIORITY DATE", "dataType": "Date"}, {"name": "SHIFT", "dataType": "String"}, {"name": "ENTRY DATE", "dataType": "Date"}, {"name": "LOCATION SET ID", "dataType": "String"}, {"name": "BIRTH MM-DD", "dataType": "Date"}, {"name": "ELIG FID 2", "dataType": "String"}, {"name": "LAST FOUR OF SSH", "dataType": "Float"}, {"name": "MIRE DATE", "dataType": "Date"}, {"name": "ALIGHT KCODE", "dataType": "String"}, {"name": "FIN SECTOR", "dataType": "String"}, {"name": "CITIZENSHIP STATUS", "dataType": "String"}, {"name": "ACTION DATE", "dataType": "Date"}, {"name": "JOBCODE SET ID", "dataType": "Float"}, {"name": "DATE ENTERED", "dataType": "Date"}, {"name": "COUNTRY", "dataType": "String"}, {"name": "ANN BEN RT", "dataType": "Float"}, {"name": "REASON", "dataType": "String"}, {"name": "SHFT RATE", "dataType": "Float"}, {"name": "WORK SCHEDULE", "dataType": "String"}, {"name": "ORIG MIRE DATE", "dataType": "Date"}, {"name": "COUNTRY2", "dataType": "String"}, {"name": "DEPARTMENT", "dataType": "String"}, {"name": "ELIG FLD 7", "dataType": "String"}, {"name": "DIRSLASHIND", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "RSLASHT", "dataType": "String"}, {"name": "BARG UNIT", "dataType": "String"}, {"name": "MAR STATUS", "dataType": "String"}, {"name": "ADDRESS 2", "dataType": "String"}, {"name": "LABOR CODE", "dataType": "Float"}, {"name": "TERM DATE", "dataType": "Date"}, {"name": "POSTAL", "dataType": "Float"}]', 
        '[{"name": "POSTAL", "dataType": "Float"}, {"name": "STATE2", "dataType": "String"}, {"name": "ALIGHT BENCODE", "dataType": "String"}, {"name": "EMPL CLASS", "dataType": "String"}, {"name": "MAIL DROP", "dataType": "String"}, {"name": "COMP RATE", "dataType": "Float"}, {"name": "CITY", "dataType": "String"}, {"name": "MIRE DATE", "dataType": "Date"}, {"name": "NAME", "dataType": "String"}, {"name": "UNION SENIORITY DATE", "dataType": "Date"}, {"name": "BA", "dataType": "String"}, {"name": "FTE", "dataType": "Float"}, {"name": "LOCATION", "dataType": "String"}, {"name": "WORK PHONE", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "SAL PLAN", "dataType": "String"}, {"name": "CO", "dataType": "String"}, {"name": "ADDRESS 4", "dataType": "String"}, {"name": "SERVICE DT", "dataType": "Date"}, {"name": "EMPLOYEE ID", "dataType": "String"}, {"name": "JOBCODE SET ID", "dataType": "Float"}, {"name": "JOB CODE", "dataType": "String"}, {"name": "DOB", "dataType": "Date"}, {"name": "DESCRIPTION", "dataType": "String"}, {"name": "COMPANY_CODE", "dataType": "String"}, {"name": "LABOR CODE", "dataType": "Float"}, {"name": "ELIG FID 2", "dataType": "String"}, {"name": "HREPID", "dataType": "Float"}, {"name": "SHFT RATE", "dataType": "Float"}, {"name": "HRBP NAME", "dataType": "String"}, {"name": "ELIG FLD 1", "dataType": "String"}, {"name": "SUPERVISOR NAME", "dataType": "String"}, {"name": "CHNG AMT", "dataType": "Float"}, {"name": "EMAL", "dataType": "String"}, {"name": "COMPANY CODE", "dataType": "String"}, {"name": "JOB GROUP", "dataType": "String"}, {"name": "SUBFUNCTION", "dataType": "String"}, {"name": "PAY STATUS", "dataType": "String"}, {"name": "ALIGHT KCODE", "dataType": "String"}, {"name": "SHIFT", "dataType": "String"}, {"name": "LAST NAME", "dataType": "String"}, {"name": "FLSA STAT", "dataType": "String"}, {"name": "DEPT DATE", "dataType": "Date"}, {"name": "ES BA FUNCTION HREP", "dataType": "String"}, {"name": "SEQUERCE", "dataType": "Float"}, {"name": "BUSINESS UNIT", "dataType": "String"}, {"name": "HR SECTOR", "dataType": "String"}, {"name": "ENTRY DATE", "dataType": "Date"}, {"name": "ANN BEN RT", "dataType": "Float"}, {"name": "YEAR", "dataType": "Float"}, {"name": "ORIG MIRE DATE", "dataType": "Date"}, {"name": "TAX LOC", "dataType": "String"}, {"name": "VIT TCG", "dataType": "String"}, {"name": "DEPT", "dataType": "String"}, {"name": "BID CODE", "dataType": "Float"}, {"name": "DEPTIDS2", "dataType": "String"}, {"name": "COUNTRY", "dataType": "String"}, {"name": "EMP ID", "dataType": "Float"}, {"name": "SHIFT FCTR", "dataType": "String"}, {"name": "ADDRESS 3", "dataType": "String"}, {"name": "SALARY SET ID", "dataType": "String"}, {"name": "REASON", "dataType": "String"}, {"name": "AGE", "dataType": "Float"}, {"name": "MIDDLE", "dataType": "String"}, {"name": "SEX", "dataType": "String"}, {"name": "DEPT FUNCTION", "dataType": "String"}, {"name": "FIN SECTOR", "dataType": "String"}, {"name": "REHIRE DT", "dataType": "Date"}, {"name": "FIRST NAME", "dataType": "String"}, {"name": "DEPARTMENT", "dataType": "String"}, {"name": "DEPTIDCOMMA4COMMA1", "dataType": "String"}, {"name": "ELIA FLD 9", "dataType": "String"}, {"name": "ELIG FID 5", "dataType": "String"}, {"name": "HOME PHONE O", "dataType": "String"}, {"name": "UNION CODE", "dataType": "String"}, {"name": "EEO-1 CAT", "dataType": "Float"}, {"name": "DATE ENTERED", "dataType": "Date"}, {"name": "JOB TITLE", "dataType": "String"}, {"name": "PAY GROUP", "dataType": "String"}, {"name": "GRADE", "dataType": "String"}, {"name": "STND HRSMT", "dataType": "Float"}, {"name": "GLC CODE", "dataType": "String"}, {"name": "SUPV EMAIL", "dataType": "String"}, {"name": "TERM DATE", "dataType": "Date"}, {"name": "COMPANY SENIORITY D", "dataType": "Date"}, {"name": "ELIG FLD 7", "dataType": "String"}, {"name": "ELIG FID", "dataType": "String"}, {"name": "COUNTRY2", "dataType": "String"}, {"name": "BUSINESS UNIT2", "dataType": "String"}, {"name": "ELIG FID A", "dataType": "String"}, {"name": "ADDRESS 2", "dataType": "String"}, {"name": "WORK PERIOD", "dataType": "String"}, {"name": "RSLASHT", "dataType": "String"}, {"name": "LOC DESCR", "dataType": "String"}, {"name": "LOCATION SET ID", "dataType": "String"}, {"name": "JOB FUND", "dataType": "String"}, {"name": "GRADE DATE", "dataType": "Date"}, {"name": "BIRTH MM-DD", "dataType": "Date"}, {"name": "WORK SCHEDULE", "dataType": "String"}, {"name": "SENICE MONT", "dataType": "Float"}, {"name": "ACTION DATE", "dataType": "Date"}, {"name": "BIRTHPLACE", "dataType": "String"}, {"name": "TITLE", "dataType": "String"}, {"name": "FRINGE CD", "dataType": "String"}, {"name": "ELIG FID 4", "dataType": "String"}, {"name": "DIRSLASHIND", "dataType": "String"}, {"name": "CITIZENSHIP STATUS", "dataType": "String"}, {"name": "ADDRESS 1", "dataType": "String"}, {"name": "WIIRS COMP", "dataType": "Float"}, {"name": "SSA", "dataType": "String"}, {"name": "ACTION", "dataType": "String"}, {"name": "INCR DT", "dataType": "Date"}, {"name": "MAR STATUS", "dataType": "String"}, {"name": "TIME IN HR", "dataType": "Float"}, {"name": "FSLASHP", "dataType": "String"}, {"name": "COMBOSLASHACCT CODE", "dataType": "Float"}, {"name": "BARG UNIT", "dataType": "String"}, {"name": "SUPV ID", "dataType": "Float"}, {"name": "LAST FOUR OF SSH", "dataType": "Float"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_339_0 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    (
      CASE
        WHEN ((DESCRIPTION IS NULL))
          THEN 'Work Schedule'
        ELSE '             '
      END
    ) AS PROBLEMS,
    *
  
  FROM Union_338 AS in0

),

Filter_340 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT * 
  
  FROM Formula_339_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEMS)) = 0)
        )

),

AlteryxSelect_341 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    "EMP ID" AS "EMP ID",
    PROBLEMS AS PROBLEMS
  
  FROM Filter_340 AS in0

),

Summarize_342 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    MAX(PROBLEMS) AS PROBLEMS,
    "EMP ID" AS "EMP ID"
  
  FROM AlteryxSelect_341 AS in0
  
  GROUP BY "EMP ID"

),

Formula_539_0 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    (
      CASE
        WHEN ("BUSINESS UNIT EXT_RA" = "BU VALUE_DATAKEY")
          THEN 't'
        ELSE 'f'
      END
    ) AS PROBLEMS,
    *
  
  FROM Join_535_inner AS in0

),

Summarize_541 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    MAX(PROBLEMS) AS MAX_PROBLEMS,
    "EMP ID_RA" AS "EMP ID_RA"
  
  FROM Formula_539_0 AS in0
  
  GROUP BY "EMP ID_RA"

),

Formula_542_0 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT 
    (
      CASE
        WHEN (MAX_PROBLEMS = 'f')
          THEN 'BA/Company Mismatch'
        ELSE '                   '
      END
    ) AS PROBLEM,
    *
  
  FROM Summarize_541 AS in0

),

Filter_543 AS (

  {#VisualGroup: BAcompanymismatch#}
  SELECT * 
  
  FROM Formula_542_0 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROBLEM)) = 0)
        )

),

Union_482 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Summarize_580', 
        'Summarize_528', 
        'Summarize_506', 
        'Filter_543', 
        'AlteryxSelect_321', 
        'Summarize_581', 
        'Summarize_481', 
        'Summarize_342', 
        'AlteryxSelect_215'
      ], 
      [
        '[{"name": "EMP ID_RA", "dataType": "String"}, {"name": "PROBLEM", "dataType": "String"}]', 
        '[{"name": "EMP ID_RA", "dataType": "String"}, {"name": "PROBLEM", "dataType": "String"}]', 
        '[{"name": "PROBLEMS", "dataType": "String"}, {"name": "EMP ID_RA", "dataType": "String"}]', 
        '[{"name": "PROBLEM", "dataType": "String"}, {"name": "MAX_PROBLEMS", "dataType": "String"}, {"name": "EMP ID_RA", "dataType": "String"}]', 
        '[{"name": "ACCT CODE_RA", "dataType": "String"}, {"name": "BU VALUE_DATAKEY", "dataType": "String"}, {"name": "PROBLEM", "dataType": "String"}, {"name": "EMPLOYEE ID_RA", "dataType": "String"}]', 
        '[{"name": "EMP ID_RA", "dataType": "String"}, {"name": "PROBLEM", "dataType": "String"}]', 
        '[{"name": "PROBLEM", "dataType": "String"}, {"name": "EMP ID", "dataType": "Float"}]', 
        '[{"name": "PROBLEMS", "dataType": "String"}, {"name": "EMP ID", "dataType": "Float"}]', 
        '[{"name": "EMP ID_ALPHA", "dataType": "String"}, {"name": "PROBLEM", "dataType": "String"}, {"name": "EMP ID_RA", "dataType": "String"}, {"name": "DEPT CODE_RA", "dataType": "String"}, {"name": "EMPLOYEE ID_RA", "dataType": "String"}, {"name": "SET ID_DEPT", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_510 AS (

  SELECT 
    LISTAGG(PROBLEM, ', ') AS PROBLEM,
    "EMP ID" AS "EMP ID"
  
  FROM Union_482 AS in0
  
  GROUP BY "EMP ID"

),

Join_511_inner AS (

  SELECT 
    in1."EMP ID" AS "RIGHT_EMP ID",
    in0.*,
    in1.* EXCLUDE ("EMP ID")
  
  FROM AlteryxSelect_509 AS in0
  INNER JOIN Summarize_510 AS in1
     ON (CAST(in0."EMP ID" AS DOUBLE) = in1."EMP ID")

),

AlteryxSelect_582 AS (

  SELECT 
    "EMP ID" AS "EMP ID",
    "RIGHT_EMP ID" AS "RIGHT_EMP ID",
    PROBLEM AS PROBLEM
  
  FROM Join_511_inner AS in0

)

SELECT *

FROM AlteryxSelect_582
