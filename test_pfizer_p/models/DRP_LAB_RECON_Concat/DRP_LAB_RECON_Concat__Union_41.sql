{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH C1071032_LB003__3 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_LAB_RECON_Concat', 'C1071032_LB003__3') }}

),

RecordID_73 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM C1071032_LB003__3

),

C1071032_LB003__52 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_LAB_RECON_Concat', 'C1071032_LB003__52') }}

),

Union_41_reformat_0 AS (

  SELECT 
    `Accession Number` AS `Accession Number`,
    Aging AS Aging,
    CUS_VISITMAP AS CUS_VISITMAP,
    `Date of Visit paranthesesOpenInFormparanthesesClose` AS `Date of Visit paranthesesOpenInFormparanthesesClose`,
    `Discrepancy category` AS `Discrepancy category`,
    `Enrollment Group` AS `Enrollment Group`,
    FileName AS FileName,
    `Lab Date of Collection` AS `Lab Date of Collection`,
    `Lab Time of Collection` AS `Lab Time of Collection`,
    `Lab Type` AS `Lab Type`,
    CAST(`Main IC Date paranthesesOpenInFormparanthesesClose` AS string) AS `Main IC Date paranthesesOpenInFormparanthesesClose`,
    RecordID AS RecordID,
    `Related Samples` AS `Related Samples`,
    `Site Country` AS `Site Country`,
    `Study Center Name` AS `Study Center Name`,
    `Study ID` AS `Study ID`,
    `Subject Number` AS `Subject Number`,
    `Visit Name paranthesesOpenCentral LabsparanthesesClose` AS `Visit Name paranthesesOpenCentral LabsparanthesesClose`,
    `Visit Name paranthesesOpenInFormparanthesesClose` AS `Visit Name paranthesesOpenInFormparanthesesClose`
  
  FROM RecordID_73 AS in0

),

RecordID_74 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM C1071032_LB003__52

),

Union_41_reformat_1 AS (

  SELECT 
    `Accession Number` AS `Accession Number`,
    Aging AS Aging,
    CUS_VISITMAP AS CUS_VISITMAP,
    `Date of Visit paranthesesOpenInFormparanthesesClose` AS `Date of Visit paranthesesOpenInFormparanthesesClose`,
    `Discrepancy category` AS `Discrepancy category`,
    `Enrollment Group` AS `Enrollment Group`,
    FileName AS FileName,
    `Lab Date of Collection` AS `Lab Date of Collection`,
    `Lab Time of Collection` AS `Lab Time of Collection`,
    `Lab Type` AS `Lab Type`,
    CAST(`Main IC Date paranthesesOpenInFormparanthesesClose` AS string) AS `Main IC Date paranthesesOpenInFormparanthesesClose`,
    RecordID AS RecordID,
    `Related Samples` AS `Related Samples`,
    `Site Country` AS `Site Country`,
    `Study Center Name` AS `Study Center Name`,
    `Study ID` AS `Study ID`,
    `Subject Number` AS `Subject Number`,
    `Visit Name paranthesesOpenCentral LabsparanthesesClose` AS `Visit Name paranthesesOpenCentral LabsparanthesesClose`,
    `Visit Name paranthesesOpenInFormparanthesesClose` AS `Visit Name paranthesesOpenInFormparanthesesClose`
  
  FROM RecordID_74 AS in0

),

Union_41 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_41_reformat_0', 'Union_41_reformat_1'], 
      [
        '[{"name": "Accession Number", "dataType": "String"}, {"name": "Aging", "dataType": "Double"}, {"name": "CUS_VISITMAP", "dataType": "String"}, {"name": "Date of Visit paranthesesOpenInFormparanthesesClose", "dataType": "String"}, {"name": "Discrepancy category", "dataType": "String"}, {"name": "Enrollment Group", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "Lab Date of Collection", "dataType": "String"}, {"name": "Lab Time of Collection", "dataType": "String"}, {"name": "Lab Type", "dataType": "String"}, {"name": "Main IC Date paranthesesOpenInFormparanthesesClose", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Related Samples", "dataType": "String"}, {"name": "Site Country", "dataType": "String"}, {"name": "Study Center Name", "dataType": "Double"}, {"name": "Study ID", "dataType": "String"}, {"name": "Subject Number", "dataType": "String"}, {"name": "Visit Name paranthesesOpenCentral LabsparanthesesClose", "dataType": "String"}, {"name": "Visit Name paranthesesOpenInFormparanthesesClose", "dataType": "String"}]', 
        '[{"name": "Accession Number", "dataType": "String"}, {"name": "Aging", "dataType": "Double"}, {"name": "CUS_VISITMAP", "dataType": "String"}, {"name": "Date of Visit paranthesesOpenInFormparanthesesClose", "dataType": "String"}, {"name": "Discrepancy category", "dataType": "String"}, {"name": "Enrollment Group", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "Lab Date of Collection", "dataType": "String"}, {"name": "Lab Time of Collection", "dataType": "String"}, {"name": "Lab Type", "dataType": "String"}, {"name": "Main IC Date paranthesesOpenInFormparanthesesClose", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Related Samples", "dataType": "String"}, {"name": "Site Country", "dataType": "String"}, {"name": "Study Center Name", "dataType": "Double"}, {"name": "Study ID", "dataType": "String"}, {"name": "Subject Number", "dataType": "String"}, {"name": "Visit Name paranthesesOpenCentral LabsparanthesesClose", "dataType": "String"}, {"name": "Visit Name paranthesesOpenInFormparanthesesClose", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_41
