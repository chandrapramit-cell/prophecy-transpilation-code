{{
  config({    
    "materialized": "table",
    "alias": "WORK_CARDSORIG_CUST_ADD_PAF",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Shortcut_to_PAFUsingQAS AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_cust_add_paf', 'Shortcut_to_PAFUsingQAS') }}

),

SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_cust_add_paf__SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0')}}

),

WORK_CARDSORIG_CUST_ADD_PAF_EXP_JOIN AS (

  SELECT 
    in1.pafstate AS pafstate,
    in1.area AS area,
    in1.out6 AS out6,
    in1.out2 AS out2,
    in1.out7 AS out7,
    in0.ADDRESSLINE4 AS ADDRESSLINE4,
    in1.dataplus1 AS dataplus1,
    in0.ADDRESSLINE2 AS ADDRESSLINE2,
    in0.POSTCODE AS POSTCODE,
    in0.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.out3 AS out3,
    in0.ADDRESSLINE3 AS ADDRESSLINE3,
    in0.COUNTRY AS COUNTRY,
    in1.sector AS sector,
    in1.dataplus2 AS dataplus2,
    in1.out4 AS out4,
    in1.dataplus3 AS dataplus3,
    in0.prophecy_sk AS prophecy_sk,
    in1.out9 AS out9,
    in0.ADDRESSLINE6 AS ADDRESSLINE6,
    in1.unit AS unit,
    in1.out1 AS out1,
    in0.ADDRESSLINE5 AS ADDRESSLINE5,
    in1.district AS district,
    in1.out5 AS out5,
    in0.ADDRESSLINE1 AS ADDRESSLINE1,
    in1.out8 AS out8,
    in1.out10 AS out10
  
  FROM SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0 AS in0
  INNER JOIN Shortcut_to_PAFUsingQAS AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

WORK_CARDSORIG_CUST_ADD_PAF_EXP_JOIN_EXPR_69 AS (

  SELECT 
    ADDRESSLINE3 AS ADDRESSLINE3,
    ADDRESSLINE4 AS ADDRESSLINE4,
    ADDRESSLINE5 AS ADDRESSLINE5,
    ADDRESSLINE6 AS ADDRESSLINE6,
    POSTCODE AS POSTCODE,
    COUNTRY AS COUNTRY,
    ADDRESSLINE1 AS ADDRESSLINE2,
    ADDRESSLINE2 AS ADDRESSLINE1,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    prophecy_sk AS prophecy_sk,
    out1 AS PAF_OUT1,
    out2 AS PAF_OUT2,
    out3 AS PAF_OUT3,
    out4 AS PAF_OUT4,
    out5 AS PAF_OUT5,
    out6 AS PAF_OUT6,
    out7 AS PAF_OUT7,
    out8 AS PAF_OUT8,
    out9 AS PAF_OUT9,
    out10 AS PAF_OUT10,
    pafstate AS PAF_PAFSTATE,
    area AS PAF_AREA,
    district AS PAF_DISTRICT,
    sector AS PAF_SECTOR,
    unit AS PAF_UNIT,
    dataplus1 AS PAF_DPS,
    dataplus2 AS PAF_XCOORDINATE,
    dataplus3 AS PAF_YCOORDINATE
  
  FROM WORK_CARDSORIG_CUST_ADD_PAF_EXP_JOIN AS in0

)

SELECT *

FROM WORK_CARDSORIG_CUST_ADD_PAF_EXP_JOIN_EXPR_69
