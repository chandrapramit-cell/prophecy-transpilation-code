{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH DynamicInput_184 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_184') }}

),

DynamicInput_64 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_64') }}

),

Union_185 AS (

  {{
    prophecy_basics.UnionByName(
      ['DynamicInput_64', 'DynamicInput_184'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "PartNumber", "dataType": "String"}, {"name": "Note", "dataType": "String"}, {"name": "Facility", "dataType": "String"}, {"name": "Plant", "dataType": "String"}, {"name": "TransactionType", "dataType": "String"}, {"name": "TransactedBy", "dataType": "String"}, {"name": "DateTransacted", "dataType": "Timestamp"}, {"name": "TransactionId", "dataType": "String"}]', 
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "PartNumber", "dataType": "String"}, {"name": "Note", "dataType": "String"}, {"name": "Facility", "dataType": "String"}, {"name": "Plant", "dataType": "String"}, {"name": "TransactionType", "dataType": "String"}, {"name": "TransactedBy", "dataType": "String"}, {"name": "DateTransacted", "dataType": "Timestamp"}, {"name": "TransactionId", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_178_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Plant IS NULL) AS BOOLEAN)
          THEN ''
        ELSE Plant
      END
    ) AS string) AS Plant,
    * EXCEPT (`plant`)
  
  FROM Union_185 AS in0

),

AlteryxSelect_23 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__AlteryxSelect_23')}}

),

Join_65_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM Formula_178_0 AS in0
  INNER JOIN AlteryxSelect_23 AS in1
     ON (in0.Facility = in1.Facility)

),

AlteryxSelect_67 AS (

  SELECT 
    TransactionId AS TransactionId,
    TransactionType AS TransactionType,
    DateTransacted AS DateTransacted,
    TransactedBy AS TransactedBy,
    PartNumber AS PartNumber,
    Quantity AS Quantity,
    Note AS Note,
    Facility AS Facility,
    Plant AS Plant
  
  FROM Join_65_inner AS in0

),

AccountingTrans_71 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'AccountingTrans_71') }}

),

Join_80_inner AS (

  SELECT 
    in0.TransactionId AS TransactionId,
    in1.`Accounting Transaction Type` AS TransactionType,
    in0.DateTransacted AS DateTransacted,
    in0.TransactedBy AS TransactedBy,
    in0.PartNumber AS PartNumber,
    in0.Quantity AS Quantity,
    in0.Note AS Note,
    in0.Facility AS Facility,
    in0.Plant AS Plant,
    in0.* EXCEPT (`TransactionId`, 
    `DateTransacted`, 
    `TransactedBy`, 
    `PartNumber`, 
    `Quantity`, 
    `Note`, 
    `Facility`, 
    `Plant`, 
    `TransactionType`),
    in1.* EXCEPT (`Accounting Transaction Type`, `Base Transaction Type`)
  
  FROM AlteryxSelect_67 AS in0
  INNER JOIN AccountingTrans_71 AS in1
     ON (in0.TransactionType = in1.`Base Transaction Type`)

),

CrossTab_81_0 AS (

  SELECT 
    (
      CASE
        WHEN (TransactionType IS NULL)
          THEN '_Null_'
        ELSE TransactionType
      END
    ) AS TransactionType,
    * EXCEPT (`transactiontype`)
  
  FROM Join_80_inner AS in0

),

CrossTab_81_1 AS (

  SELECT 
    (REGEXP_REPLACE(TransactionType, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS TransactionType,
    * EXCEPT (`transactiontype`)
  
  FROM CrossTab_81_0 AS in0

)

SELECT *

FROM CrossTab_81_1
