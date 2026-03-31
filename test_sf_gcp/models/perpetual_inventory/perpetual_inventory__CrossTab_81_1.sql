{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
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
        '[{"name": "TRANSACTIONTYPE", "dataType": "String"}, {"name": "DATETRANSACTED", "dataType": "Timestamp"}, {"name": "QUANTITY", "dataType": "Float"}, {"name": "NOTE", "dataType": "String"}, {"name": "TRANSACTEDBY", "dataType": "String"}, {"name": "PARTNUMBER", "dataType": "String"}, {"name": "PLANT", "dataType": "String"}, {"name": "FACILITY", "dataType": "String"}, {"name": "TRANSACTIONID", "dataType": "String"}]', 
        '[{"name": "TRANSACTIONTYPE", "dataType": "String"}, {"name": "DATETRANSACTED", "dataType": "Timestamp"}, {"name": "QUANTITY", "dataType": "Float"}, {"name": "NOTE", "dataType": "String"}, {"name": "TRANSACTEDBY", "dataType": "String"}, {"name": "PARTNUMBER", "dataType": "String"}, {"name": "PLANT", "dataType": "String"}, {"name": "FACILITY", "dataType": "String"}, {"name": "TRANSACTIONID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_178_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (PLANT IS NULL)
          THEN ''
        ELSE PLANT
      END
    ) AS string) AS PLANT,
    * EXCLUDE ("PLANT")
  
  FROM Union_185 AS in0

),

AlteryxSelect_23 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__AlteryxSelect_23')}}

),

Join_65_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("FACILITY")
  
  FROM Formula_178_0 AS in0
  INNER JOIN AlteryxSelect_23 AS in1
     ON (in0.FACILITY = in1.FACILITY)

),

AlteryxSelect_67 AS (

  SELECT 
    TRANSACTIONID AS TRANSACTIONID,
    TRANSACTIONTYPE AS TRANSACTIONTYPE,
    DATETRANSACTED AS DATETRANSACTED,
    TRANSACTEDBY AS TRANSACTEDBY,
    PARTNUMBER AS PARTNUMBER,
    QUANTITY AS QUANTITY,
    NOTE AS NOTE,
    FACILITY AS FACILITY,
    PLANT AS PLANT
  
  FROM Join_65_inner AS in0

),

AccountingTrans_71 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'AccountingTrans_71') }}

),

Join_80_inner AS (

  SELECT 
    in0.TRANSACTIONID AS TRANSACTIONID,
    in1."ACCOUNTING TRANSACTION TYPE" AS TRANSACTIONTYPE,
    in0.DATETRANSACTED AS DATETRANSACTED,
    in0.TRANSACTEDBY AS TRANSACTEDBY,
    in0.PARTNUMBER AS PARTNUMBER,
    in0.QUANTITY AS QUANTITY,
    in0.NOTE AS NOTE,
    in0.FACILITY AS FACILITY,
    in0.PLANT AS PLANT,
    in0.* EXCLUDE ("TRANSACTIONID", 
    "DATETRANSACTED", 
    "TRANSACTEDBY", 
    "PARTNUMBER", 
    "QUANTITY", 
    "NOTE", 
    "FACILITY", 
    "PLANT", 
    "TRANSACTIONTYPE"),
    in1.* EXCLUDE ("ACCOUNTING TRANSACTION TYPE", "BASE TRANSACTION TYPE")
  
  FROM AlteryxSelect_67 AS in0
  INNER JOIN AccountingTrans_71 AS in1
     ON in0.TRANSACTIONTYPE = in1."BASE TRANSACTION TYPE"

),

CrossTab_81_0 AS (

  SELECT 
    (
      CASE
        WHEN (TRANSACTIONTYPE IS NULL)
          THEN '_Null_'
        ELSE TRANSACTIONTYPE
      END
    ) AS TRANSACTIONTYPE,
    * EXCLUDE ("TRANSACTIONTYPE")
  
  FROM Join_80_inner AS in0

),

CrossTab_81_1 AS (

  SELECT 
    (REGEXP_REPLACE(TRANSACTIONTYPE, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS TRANSACTIONTYPE,
    * EXCLUDE ("TRANSACTIONTYPE")
  
  FROM CrossTab_81_0 AS in0

)

SELECT *

FROM CrossTab_81_1
