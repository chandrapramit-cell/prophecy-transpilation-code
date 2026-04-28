{{
  config({    
    "materialized": "table",
    "alias": "ADDRESS_STAGING",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

),

SQ_WORK_CARDSORIG_CUST_ADD_PAF AS (

  SELECT 
    /*+ parallel(paf) parallel(r) parallel(ca) parallel(a) use_hash(paf r ca a)*/
    DISTINCT r.adid,
    paf.paf_out1,
    paf.paf_out2,
    paf.paf_out3,
    paf.paf_out4,
    paf.paf_out5,
    paf.paf_out6,
    paf.paf_out10,
    paf.paf_pafstate,
    paf.paf_area,
    paf.paf_district,
    paf.paf_sector,
    paf.paf_unit,
    paf.paf_dps,
    paf.paf_xcoordinate,
    paf.paf_ycoordinate
  
  FROM work_cardsorig_cust_add_paf AS paf, resides AS r, client_alias AS ca, address AS a
  
  WHERE ltrim(rtrim(ca.source_client_ref (+))) = ltrim(rtrim(paf.customer_number))
        and ca.source_type (+) = 0
        and ca.source_system_code (+) = 'U'
        and ca.class (+) = '585'
        and ca.clid = r.clid (+)
        and r.eto (+) IS NULL
        and r.source_type (+) = 0
        and r.adid = a.adid (+)
        and a.eto (+) IS NULL
        and (
              nvl(a.line1, '#') != nvl(paf.paf_out1, '#')
              or nvl(a.line2, '#') != nvl(paf.paf_out2, '#')
              or nvl(a.line3, '#') != nvl(paf.paf_out3, '#')
              or nvl(a.line4, '#') != nvl(paf.paf_out4, '#')
              or nvl(a.line5, '#') != nvl(paf.paf_out5, '#')
              or nvl(a.postcode, '#') != upper(nvl(nvl(paf.paf_out6, paf.paf_out10), '#'))
              or nvl(a.area, '#') != nvl(paf.paf_area, '#')
              or nvl(a.district, '#') != nvl(paf.paf_district, '#')
              or nvl(a.sector, '#') != nvl(paf.paf_sector, '#')
              or nvl(a.unit, '#') != nvl(paf.paf_unit, '#')
              or nvl(a.dps, '#') != nvl(paf.paf_dps, '#')
              or nvl(a.pafstate, '#') != nvl(paf.paf_pafstate, '#')
              or nvl(a.xcoordinate, 99999999) != nvl(paf.paf_xcoordinate, 99999999)
              or nvl(a.ycoordinate, 99999999) != nvl(paf.paf_ycoordinate, 99999999)
            )

),

EXP_ADDRESS_STAGING_LOOKUP_65 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_2,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.PAF_OUT6 AS PAF_OUT6,
    in1.PAF_OUT10 AS PAF_OUT10,
    in1.PAF_PAFSTATE AS PAF_PAFSTATE,
    in1.PAF_AREA AS PAF_AREA,
    in1.PAF_DISTRICT AS PAF_DISTRICT,
    in1.PAF_SECTOR AS PAF_SECTOR,
    in1.PAF_UNIT AS PAF_UNIT,
    in1.PAF_DPS AS PAF_DPS,
    in1.PAF_XCOORDINATE AS PAF_XCOORDINATE,
    in1.PAF_YCOORDINATE AS PAF_YCOORDINATE,
    in1.ADID AS ADID,
    in1.PAF_OUT1 AS PAF_OUT1,
    in1.PAF_OUT2 AS PAF_OUT2,
    in1.PAF_OUT3 AS PAF_OUT3,
    in1.PAF_OUT4 AS PAF_OUT4,
    in1.PAF_OUT5 AS PAF_OUT5
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SQ_WORK_CARDSORIG_CUST_ADD_PAF AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_ADDRESS_STAGING_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_2 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_ADDRESS_STAGING_LOOKUP_65 AS in0

),

EXP_ADDRESS_STAGING AS (

  SELECT 
    ADID AS ADID,
    PAF_OUT1 AS PAF_OUT1,
    PAF_OUT2 AS PAF_OUT2,
    PAF_OUT3 AS PAF_OUT3,
    PAF_OUT4 AS PAF_OUT4,
    PAF_OUT5 AS PAF_OUT5,
    PAF_PAFSTATE AS PAF_PAFSTATE,
    PAF_AREA AS PAF_AREA,
    PAF_DISTRICT AS PAF_DISTRICT,
    PAF_SECTOR AS PAF_SECTOR,
    PAF_UNIT AS PAF_UNIT,
    PAF_DPS AS PAF_DPS,
    PAF_XCOORDINATE AS PAF_XCOORDINATE,
    PAF_YCOORDINATE AS PAF_YCOORDINATE,
    50 AS CLASS,
    'U' AS SOURCE_SYSTEM_CODE,
    UPPER((
      CASE
        WHEN CAST((PAF_OUT6 IS NULL) AS BOOL)
          THEN PAF_OUT10
        ELSE PAF_OUT6
      END
    )) AS POSTCODE_OUT,
    (
      CASE
        WHEN CAST((
          (
            CASE
              WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
              ELSE CAST(NULL AS STRING)
            END
          ) IS NULL
        ) AS BOOL)
          THEN (ERROR('No Business Date found on dbattriute'))
        ELSE (
          PARSE_TIMESTAMP(
            '%Y%m%d', 
            CAST(SUBSTRING(
              (
                CASE
                  WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                  ELSE CAST(NULL AS STRING)
                END
              ), 
              0, 
              8) AS STRING))
        )
      END
    ) AS business_date,
    'UK' AS COUNTRY
  
  FROM EXP_ADDRESS_STAGING_VARS_0 AS in0

),

INS_ADDRESS_STAGING_add_missing_column_0 AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS MAILSORT,
    CAST(NULL AS STRING) AS CHECK_CHAR,
    CAST(NULL AS STRING) AS CDB_CLIENT_STATUS,
    CAST(NULL AS TIMESTAMP) AS CDB_DATE_ADDRESS_CHANGE,
    *
  
  FROM EXP_ADDRESS_STAGING AS in0

),

INS_ADDRESS_STAGING_add_missing_column_EXPR_64 AS (

  SELECT 
    ADID AS ADID,
    COUNTRY AS COUNTRY,
    PAF_OUT2 AS LINE2,
    CDB_DATE_ADDRESS_CHANGE AS CDB_DATE_ADDRESS_CHANGE,
    PAF_UNIT AS UNIT,
    PAF_OUT1 AS LINE1,
    PAF_OUT5 AS LINE5,
    CDB_CLIENT_STATUS AS CDB_CLIENT_STATUS,
    PAF_DPS AS DPS,
    PAF_DISTRICT AS DISTRICT,
    POSTCODE_OUT AS POSTCODE,
    PAF_PAFSTATE AS PAFSTATE,
    PAF_SECTOR AS SECTOR,
    PAF_XCOORDINATE AS XCOORDINATE,
    PAF_YCOORDINATE AS YCOORDINATE,
    CHECK_CHAR AS CHECK_CHAR,
    PAF_AREA AS AREA,
    MAILSORT AS MAILSORT,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    CLASS AS CLASS,
    PAF_OUT4 AS LINE4,
    PAF_OUT3 AS LINE3
  
  FROM INS_ADDRESS_STAGING_add_missing_column_0 AS in0

)

SELECT *

FROM INS_ADDRESS_STAGING_add_missing_column_EXPR_64
