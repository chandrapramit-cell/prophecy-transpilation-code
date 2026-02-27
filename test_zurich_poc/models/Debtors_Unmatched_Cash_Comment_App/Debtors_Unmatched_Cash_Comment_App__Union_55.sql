{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_89 AS (

  SELECT * 
  
  FROM {{ ref('seed_89')}}

),

Filter_107 AS (

  SELECT *
  
  FROM {{ ref('Debtors_Unmatched_Cash_Comment_App__Filter_107')}}

),

Filter_97 AS (

  SELECT * 
  
  FROM Filter_107 AS in0
  
  WHERE (KEY_9AX = 'PLACE_HOLDER')

),

TextInput_89_cast AS (

  SELECT CAST(TRANSACTION_ID AS string) AS TRANSACTION_ID
  
  FROM TextInput_89 AS in0

),

Formula_90_0 AS (

  SELECT 
    CAST({{ var('Text_Box_94') }} AS string) AS COMMENTS,
    CAST({{ var('variable__cloudUserId') }} AS string) AS EMAIL_ID,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS COMMENT_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_CRTN_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_UPDATE_DATE,
    CAST('COMMENTARY' AS string) AS CTL_SRC_SYS_SET_NAME,
    CAST('AA' AS string) AS COMMENT_CATEGORY,
    *
  
  FROM TextInput_89_cast AS in0

),

TextToColumns_100 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_90_0'], 
      'TRANSACTION_ID', 
      ";", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'TRANSACTION_ID', 
      'TRANSACTION_ID', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_100_dropGem_0 AS (

  SELECT 
    generatedColumnName AS TRANSACTION_ID,
    * EXCEPT (`generatedColumnName`, `transaction_id`)
  
  FROM TextToColumns_100 AS in0

),

Cleanse_101 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_100_dropGem_0'], 
      [
        { "name": "CTL_REC_CRTN_DATE", "dataType": "Timestamp" }, 
        { "name": "CTL_REC_UPDATE_DATE", "dataType": "Timestamp" }, 
        { "name": "COMMENTS", "dataType": "String" }, 
        { "name": "TRANSACTION_ID", "dataType": "String" }, 
        { "name": "COMMENT_CATEGORY", "dataType": "String" }, 
        { "name": "CTL_SRC_SYS_SET_NAME", "dataType": "String" }, 
        { "name": "EMAIL_ID", "dataType": "String" }, 
        { "name": "COMMENT_DATE", "dataType": "Timestamp" }
      ], 
      'keepOriginal', 
      ['TRANSACTION_ID'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

AlteryxSelect_91 AS (

  SELECT 
    TRANSACTION_ID AS Transaction_ID,
    CAST(COMMENTS AS string) AS COMMENTS,
    EMAIL_ID AS CLIENT_ID,
    * EXCEPT (`Transaction_ID`, `COMMENTS`, `EMAIL_ID`)
  
  FROM Cleanse_101 AS in0

),

AppendFields_99 AS (

  SELECT 
    in0.* EXCEPT (`FIRST_NAME`, 
    `SURNAME`, 
    `ROLE`, 
    `KEY_5AX`, 
    `KEY_9AX`, 
    `UPDATED_USER`, 
    `CURRENT_RECORD`, 
    `VALID_FROM`, 
    `VALID_TO`, 
    `CTL_REC_CRTN_DATE`, 
    `CTL_REC_UPDATE_DATE`, 
    `CTL_SRC_SYS_SET_NAME`),
    in1.*
  
  FROM Filter_97 AS in0
  INNER JOIN AlteryxSelect_91 AS in1
     ON TRUE

),

Filter_26 AS (

  SELECT *
  
  FROM {{ ref('Debtors_Unmatched_Cash_Comment_App__Filter_26')}}

),

Filter_15 AS (

  SELECT * 
  
  FROM Filter_26 AS in0
  
  WHERE (KEY_9AX = 'PLACE_HOLDER')

),

TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(TRANSACTION_ID AS string) AS TRANSACTION_ID
  
  FROM TextInput_1 AS in0

),

Formula_2_0 AS (

  SELECT 
    CAST({{ var('Text_Box_7') }} AS string) AS COMMENTS,
    CAST({{ var('variable__cloudUserId') }} AS string) AS EMAIL_ID,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS COMMENT_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_CRTN_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_UPDATE_DATE,
    CAST('COMMENTARY' AS string) AS CTL_SRC_SYS_SET_NAME,
    CAST({{ var('Drop_Down_65') }} AS string) AS COMMENT_CATEGORY,
    *
  
  FROM TextInput_1_cast AS in0

),

TextToColumns_19 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_2_0'], 
      'TRANSACTION_ID', 
      ";", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'TRANSACTION_ID', 
      'TRANSACTION_ID', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_19_dropGem_0 AS (

  SELECT 
    generatedColumnName AS TRANSACTION_ID,
    * EXCEPT (`generatedColumnName`, `transaction_id`)
  
  FROM TextToColumns_19 AS in0

),

Cleanse_20 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_19_dropGem_0'], 
      [
        { "name": "CTL_REC_CRTN_DATE", "dataType": "Timestamp" }, 
        { "name": "CTL_REC_UPDATE_DATE", "dataType": "Timestamp" }, 
        { "name": "COMMENTS", "dataType": "String" }, 
        { "name": "TRANSACTION_ID", "dataType": "String" }, 
        { "name": "COMMENT_CATEGORY", "dataType": "String" }, 
        { "name": "CTL_SRC_SYS_SET_NAME", "dataType": "String" }, 
        { "name": "EMAIL_ID", "dataType": "String" }, 
        { "name": "COMMENT_DATE", "dataType": "Timestamp" }
      ], 
      'keepOriginal', 
      ['TRANSACTION_ID'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

AlteryxSelect_3 AS (

  SELECT 
    TRANSACTION_ID AS Transaction_ID,
    CAST(COMMENTS AS string) AS COMMENTS,
    EMAIL_ID AS CLIENT_ID,
    * EXCEPT (`Transaction_ID`, `COMMENTS`, `EMAIL_ID`)
  
  FROM Cleanse_20 AS in0

),

AppendFields_17 AS (

  SELECT 
    in0.* EXCEPT (`FIRST_NAME`, 
    `SURNAME`, 
    `ROLE`, 
    `KEY_5AX`, 
    `KEY_9AX`, 
    `UPDATED_USER`, 
    `CURRENT_RECORD`, 
    `VALID_FROM`, 
    `VALID_TO`, 
    `CTL_REC_CRTN_DATE`, 
    `CTL_REC_UPDATE_DATE`, 
    `CTL_SRC_SYS_SET_NAME`),
    in1.*
  
  FROM Filter_15 AS in0
  INNER JOIN AlteryxSelect_3 AS in1
     ON TRUE

),

Filter_111 AS (

  SELECT *
  
  FROM {{ ref('Debtors_Unmatched_Cash_Comment_App__Filter_111')}}

),

Filter_115 AS (

  SELECT * 
  
  FROM Filter_111 AS in0
  
  WHERE (KEY_9AX = 'PLACE_HOLDER')

),

Unique_117 AS (

  SELECT * 
  
  FROM Filter_115 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EMAIL ORDER BY EMAIL) = 1

),

BULK_COMMENTS_x_56 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Debtors_Unmatched_Cash_Comment_App', 'BULK_COMMENTS_x_56') }}

),

Formula_37_0 AS (

  SELECT 
    CAST({{ var('variable__cloudUserId') }} AS string) AS EMAIL_ID,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS COMMENT_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_CRTN_DATE,
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS CTL_REC_UPDATE_DATE,
    CAST('COMMENTARY' AS string) AS CTL_SRC_SYS_SET_NAME,
    *
  
  FROM BULK_COMMENTS_x_56 AS in0

),

AlteryxSelect_38 AS (

  SELECT 
    COMMENTS AS COMMENTS,
    EMAIL_ID AS CLIENT_ID,
    * EXCEPT (`COMMENTS`, `EMAIL_ID`)
  
  FROM Formula_37_0 AS in0

),

AppendFields_47 AS (

  SELECT 
    in0.* EXCEPT (`FIRST_NAME`, 
    `SURNAME`, 
    `ROLE`, 
    `KEY_5AX`, 
    `KEY_9AX`, 
    `UPDATED_USER`, 
    `CURRENT_RECORD`, 
    `VALID_FROM`, 
    `VALID_TO`, 
    `CTL_REC_CRTN_DATE`, 
    `CTL_REC_UPDATE_DATE`, 
    `CTL_SRC_SYS_SET_NAME`, 
    `USER_TYPE`),
    in1.*
  
  FROM Unique_117 AS in0
  INNER JOIN AlteryxSelect_38 AS in1
     ON TRUE

),

Union_55 AS (

  {{
    prophecy_basics.UnionByName(
      ['AppendFields_99', 'AppendFields_47', 'AppendFields_17'], 
      [
        '[{"name": "CTL_REC_CRTN_DATE", "dataType": "Timestamp"}, {"name": "CLIENT_ID", "dataType": "String"}, {"name": "CTL_REC_UPDATE_DATE", "dataType": "Timestamp"}, {"name": "COMMENTS", "dataType": "String"}, {"name": "COMMENT_CATEGORY", "dataType": "String"}, {"name": "CTL_SRC_SYS_SET_NAME", "dataType": "String"}, {"name": "EMAIL", "dataType": "String"}, {"name": "COMMENT_DATE", "dataType": "Timestamp"}, {"name": "USER_TYPE", "dataType": "String"}, {"name": "Transaction_ID", "dataType": "String"}]', 
        '[{"name": "CTL_REC_CRTN_DATE", "dataType": "Timestamp"}, {"name": "CLIENT_ID", "dataType": "String"}, {"name": "CTL_REC_UPDATE_DATE", "dataType": "Timestamp"}, {"name": "COMMENTS", "dataType": "String"}, {"name": "TRANSACTION_ID", "dataType": "String"}, {"name": "COMMENT_CATEGORY", "dataType": "String"}, {"name": "CTL_SRC_SYS_SET_NAME", "dataType": "String"}, {"name": "EMAIL", "dataType": "String"}, {"name": "COMMENT_DATE", "dataType": "Timestamp"}]', 
        '[{"name": "CTL_REC_CRTN_DATE", "dataType": "Timestamp"}, {"name": "CLIENT_ID", "dataType": "String"}, {"name": "CTL_REC_UPDATE_DATE", "dataType": "Timestamp"}, {"name": "COMMENTS", "dataType": "String"}, {"name": "COMMENT_CATEGORY", "dataType": "String"}, {"name": "CTL_SRC_SYS_SET_NAME", "dataType": "String"}, {"name": "EMAIL", "dataType": "String"}, {"name": "COMMENT_DATE", "dataType": "Timestamp"}, {"name": "USER_TYPE", "dataType": "String"}, {"name": "Transaction_ID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_55
