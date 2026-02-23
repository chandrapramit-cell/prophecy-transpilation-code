{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH dcm_phpdatabase_4 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'dcm_phpdatabase_4_ref') }}

),

dcm_phpdatabase_3 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'dcm_phpdatabase_3_ref') }}

),

Join_13_inner AS (

  SELECT 
    in1.Code_List_Qualifier_Code_12 AS Code_List_Qualifier_Code_12,
    in1.Sys_Claim_ID AS Sys_Claim_ID,
    in1.Code_List_Qualifier_Code_7 AS Code_List_Qualifier_Code_7,
    in1.Occurrence_Span_Code_5 AS Occurrence_Span_Code_5,
    in1.Code_List_Qualifier_Code_11 AS Code_List_Qualifier_Code_11,
    in1.Code_List_Qualifier_Code_3 AS Code_List_Qualifier_Code_3,
    in0.ClaimId AS ClaimId,
    in1.Occurrence_Span_Code_11 AS Occurrence_Span_Code_11,
    in0.ServiceDateStart AS ServiceDateStart,
    in1.Code_List_Qualifier_Code_2 AS Code_List_Qualifier_Code_2,
    in1.Occurrence_Span_Code_7 AS Occurrence_Span_Code_7,
    in1.Code_List_Qualifier_Code_8 AS Code_List_Qualifier_Code_8,
    in1.Occurrence_Span_Code_12 AS Occurrence_Span_Code_12,
    in1.Occurrence_Span_Code_2 AS Occurrence_Span_Code_2,
    in1.Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    in0.MemberId AS MemberId,
    in0.ClaimType AS ClaimType,
    in1.Occurrence_Span_Code_6 AS Occurrence_Span_Code_6,
    in1.Code_List_Qualifier_Code_10 AS Code_List_Qualifier_Code_10,
    in0.PrimaryDiagnosis AS PrimaryDiagnosis,
    in1.Code_List_Qualifier_Code_1 AS Code_List_Qualifier_Code_1,
    in1.Code_List_Qualifier_Code_6 AS Code_List_Qualifier_Code_6,
    in0.ClaimState AS ClaimState,
    in1.Occurrence_Span_Code_8 AS Occurrence_Span_Code_8,
    in1.Code_List_Qualifier_Code_9 AS Code_List_Qualifier_Code_9,
    in1.Occurrence_Span_Code_1 AS Occurrence_Span_Code_1,
    in1.Occurrence_Span_Code_3 AS Occurrence_Span_Code_3,
    in1.Occurrence_Span_Code_9 AS Occurrence_Span_Code_9,
    in1.Code_List_Qualifier_Code_5 AS Code_List_Qualifier_Code_5,
    in1.Occurrence_Span_Code_10 AS Occurrence_Span_Code_10,
    in1.Occurrence_Span_Code_4 AS Occurrence_Span_Code_4,
    in1.Code_List_Qualifier_Code_4 AS Code_List_Qualifier_Code_4
  
  FROM dcm_phpdatabase_4 AS in0
  INNER JOIN dcm_phpdatabase_3 AS in1
     ON (in0.ClaimId = in1.Sys_Claim_ID)

),

Filter_18 AS (

  SELECT * 
  
  FROM Join_13_inner AS in0
  
  WHERE (Code_List_Qualifier_Code_1 = 'ABK')

),

AlteryxSelect_15 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Code_List_Qualifier_Code_1 AS Code_List_Qualifier_Code_1,
    Occurrence_Span_Code_1 AS Occurrence_Span_Code_1,
    Code_List_Qualifier_Code_2 AS Code_List_Qualifier_Code_2,
    Occurrence_Span_Code_2 AS Occurrence_Span_Code_2,
    Code_List_Qualifier_Code_3 AS Code_List_Qualifier_Code_3,
    Occurrence_Span_Code_3 AS Occurrence_Span_Code_3,
    Code_List_Qualifier_Code_4 AS Code_List_Qualifier_Code_4,
    Occurrence_Span_Code_4 AS Occurrence_Span_Code_4,
    Code_List_Qualifier_Code_5 AS Code_List_Qualifier_Code_5,
    Occurrence_Span_Code_5 AS Occurrence_Span_Code_5,
    Code_List_Qualifier_Code_6 AS Code_List_Qualifier_Code_6,
    Occurrence_Span_Code_6 AS Occurrence_Span_Code_6,
    Code_List_Qualifier_Code_7 AS Code_List_Qualifier_Code_7,
    Occurrence_Span_Code_7 AS Occurrence_Span_Code_7,
    Code_List_Qualifier_Code_8 AS Code_List_Qualifier_Code_8,
    Occurrence_Span_Code_8 AS Occurrence_Span_Code_8,
    Code_List_Qualifier_Code_9 AS Code_List_Qualifier_Code_9,
    Occurrence_Span_Code_9 AS Occurrence_Span_Code_9,
    Code_List_Qualifier_Code_10 AS Code_List_Qualifier_Code_10,
    Occurrence_Span_Code_10 AS Occurrence_Span_Code_10,
    Code_List_Qualifier_Code_11 AS Code_List_Qualifier_Code_11,
    Occurrence_Span_Code_11 AS Occurrence_Span_Code_11,
    Code_List_Qualifier_Code_12 AS Code_List_Qualifier_Code_12,
    Occurrence_Span_Code_12 AS Occurrence_Span_Code_12,
    NULL AS Date_Format_Qualifier_1,
    NULL AS Date_Format_Qualifier_2,
    NULL AS Date_Format_Qualifier_3,
    NULL AS Date_Format_Qualifier_4,
    NULL AS Date_Format_Qualifier_5,
    NULL AS Date_Format_Qualifier_6,
    NULL AS Date_Format_Qualifier_7,
    NULL AS Date_Format_Qualifier_8,
    NULL AS Date_Format_Qualifier_9,
    NULL AS Date_Format_Qualifier_10,
    NULL AS Date_Format_Qualifier_11,
    NULL AS Date_Format_Qualifier_12
  
  FROM Filter_18 AS in0

),

AlteryxSelect_21 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_3 AS DIAG_CD,
    Date_Format_Qualifier_3 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_34 AS (

  SELECT * 
  
  FROM AlteryxSelect_21 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_27 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_9 AS DIAG_CD,
    Date_Format_Qualifier_9 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_40 AS (

  SELECT * 
  
  FROM AlteryxSelect_27 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_20 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_2 AS DIAG_CD,
    Date_Format_Qualifier_2 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_33 AS (

  SELECT * 
  
  FROM AlteryxSelect_20 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_24 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_6 AS DIAG_CD,
    Date_Format_Qualifier_6 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_37 AS (

  SELECT * 
  
  FROM AlteryxSelect_24 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_22 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_4 AS DIAG_CD,
    Date_Format_Qualifier_4 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_35 AS (

  SELECT * 
  
  FROM AlteryxSelect_22 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_30 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_12 AS DIAG_CD,
    Date_Format_Qualifier_12 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_43 AS (

  SELECT * 
  
  FROM AlteryxSelect_30 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_19 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_1 AS DIAG_CD,
    Date_Format_Qualifier_1 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_32 AS (

  SELECT * 
  
  FROM AlteryxSelect_19 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_23 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_5 AS DIAG_CD,
    Date_Format_Qualifier_5 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_36 AS (

  SELECT * 
  
  FROM AlteryxSelect_23 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_26 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_8 AS DIAG_CD,
    Date_Format_Qualifier_8 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_39 AS (

  SELECT * 
  
  FROM AlteryxSelect_26 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_29 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_11 AS DIAG_CD,
    Date_Format_Qualifier_11 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_42 AS (

  SELECT * 
  
  FROM AlteryxSelect_29 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_25 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_7 AS DIAG_CD,
    Date_Format_Qualifier_7 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_38 AS (

  SELECT * 
  
  FROM AlteryxSelect_25 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

AlteryxSelect_28 AS (

  SELECT 
    MemberId AS MemberId,
    ServiceDateStart AS ServiceDateStart,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    Occurrence_Span_Code_10 AS DIAG_CD,
    Date_Format_Qualifier_10 AS variableDate
  
  FROM AlteryxSelect_15 AS in0

),

Filter_41 AS (

  SELECT * 
  
  FROM AlteryxSelect_28 AS in0
  
  WHERE (DIAG_CD IS NOT NULL)

),

Union_44 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Filter_42', 
        'Filter_43', 
        'Filter_40', 
        'Filter_34', 
        'Filter_37', 
        'Filter_39', 
        'Filter_41', 
        'Filter_35', 
        'Filter_32', 
        'Filter_33', 
        'Filter_36', 
        'Filter_38'
      ], 
      [
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]', 
        '[{"name": "MemberId", "dataType": "Numeric"}, {"name": "ServiceDateStart", "dataType": "Date"}, {"name": "Sys_Claim_HI_ID", "dataType": "Numeric"}, {"name": "Sys_Claim_ID", "dataType": "Numeric"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "variableDate", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_70 AS (

  SELECT 
    ServiceDateStart AS EFF_DATE,
    * EXCEPT (`ServiceDateStart`)
  
  FROM Union_44 AS in0

),

Formula_45_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I21', '410', 'I22', '412'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I252'))
        )
          THEN 'MYOCARDIAL_INFARCTION'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I099', 'I110', 'I130', 'I132', 'I255', 'I420', 'I425', 'I426', 'I427', 'I428', 'I429', 'P290'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I43', 'I50', '428'))
        )
          THEN 'CHF'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I731', 'I738', 'I739', 'I771', 'I790', 'I792', 'K551', 'K558', 'K559', 'Z958', 'Z959', '4439', '7854', 'V434'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I70', 'I71', '441'))
        )
          THEN 'PERIPHERAL_VASCULAR_DISEASE'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('H340'))
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
                 'G45',
                 'G46',
                 'I60',
                 'I61',
                 'I62',
                 'I63',
                 'I64',
                 'I65',
                 'I66',
                 'I67',
                 'I68',
                 'I69',
                 '430',
                 '431',
                 '432',
                 '433',
                 '434',
                 '435',
                 '436',
                 '437',
                 '438'
               )
             )
        )
          THEN 'CEREBROVASCULAR DISEASE'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('F051', 'G311'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('F00', 'F01', 'F02', 'F03', 'G30', '290'))
        )
          THEN 'DEMENTIA'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I278', 'I279', 'J684', 'J701', 'J703', '5064'))
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
                 'J40',
                 'J41',
                 'J42',
                 'J43',
                 'J44',
                 'J45',
                 'J46',
                 'J47',
                 'J60',
                 'J61',
                 'J62',
                 'J63',
                 'J64',
                 'J65',
                 'J66',
                 'J67',
                 '490',
                 '491',
                 '492',
                 '493',
                 '494',
                 '495',
                 '496',
                 '497',
                 '498',
                 '499',
                 '500',
                 '501',
                 '502',
                 '503',
                 '504',
                 '505'
               )
             )
        )
          THEN 'COPD'
        WHEN (
          (
            (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('M315', 'M351', 'M353', 'M360', '7100', '7101', '7104', '7140', '7141', '7142'))
            OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('M05', 'M06', 'M32', 'M33', 'M34', '725'))
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 5)) AS VARCHAR (1000)) IN ('71481'))
        )
          THEN 'RHEUMATIC_DISEASE'
        WHEN CAST((CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('K25', 'K26', 'K27', 'K28', '531', '532', '533', '534')) AS BOOLEAN)
          THEN 'PEPTIC_ULCER_DISEASE'
        WHEN (
          (
            (
              CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
                'K700',
                'K701',
                'K702',
                'K703',
                'K709',
                'K713',
                'K714',
                'K715',
                'K717',
                'K760',
                'K762',
                'K763',
                'K764',
                'K768',
                'K769',
                'Z944',
                '5712',
                '5714',
                '5715',
                '5716',
                'I850',
                'I859',
                'I864',
                'I982',
                'K704',
                'K711',
                'K721',
                'K729',
                'K765',
                'K766',
                'K767',
                '4560',
                '4562',
                '5722',
                '5723',
                '5724',
                '5725',
                '5726',
                '5727',
                '5728'
              )
            )
            OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('B18', 'K73', 'K74'))
          )
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 5)) AS VARCHAR (1000)) IN (
                 '45600',
                 '45601',
                 '45602',
                 '45603',
                 '45604',
                 '45605',
                 '45606',
                 '45607',
                 '45608',
                 '45609',
                 '45610',
                 '45611',
                 '45612',
                 '45613',
                 '45614',
                 '45615',
                 '45616',
                 '45617',
                 '45618',
                 '45619',
                 '45620',
                 '45621'
               )
             )
        )
          THEN 'LIVER_DISEASE'
        WHEN CAST((
          CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
            'E100',
            'E10l',
            'E106',
            'E108',
            'E109',
            'E110',
            'E111',
            'E116',
            'E118',
            'E119',
            'E120',
            'E121',
            'E126',
            'E128',
            'E129',
            'E130',
            'E131',
            'E136',
            'E138',
            'E139',
            'E140',
            'E141',
            'E146',
            'E148',
            'E149',
            '2500',
            '2501',
            '2502',
            '2503',
            '2504',
            '2505',
            '2506',
            '2507',
            'E102',
            'E103',
            'E104',
            'E105',
            'E112',
            'E113',
            'E114',
            'E115',
            'E117',
            'E122',
            'E123',
            'E124',
            'E125',
            'E127',
            'E132',
            'E133',
            'E134',
            'E135',
            'E137',
            'E142',
            'E143',
            'E144',
            'E145',
            'E147'
          )
        ) AS BOOLEAN)
          THEN 'DIABETES'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('G041', 'G114', 'G801', 'G802', 'G831', 'G832', 'G833', 'G834', 'G839', '3441'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('G81', 'G82', '342'))
        )
          THEN 'HEMIPLEGIA_OR_PARAPLEGIA'
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
              'I120',
              'I131',
              'N032',
              'N033',
              'N034',
              'N035',
              'N036',
              'N037',
              'N052',
              'N053',
              'N054',
              'N055',
              'N056',
              'N057',
              'N250',
              'Z490',
              'Z491',
              'Z492',
              'Z940',
              'Z992'
            )
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('N18', 'N19', '582', '585', '586', '588', '583'))
        )
          THEN 'RENAL_DISEASE'
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
              'C00',
              'C01',
              'C02',
              'C03',
              'C04',
              'C05',
              'C06',
              'C07',
              'C08',
              'C09',
              'C10',
              'C11',
              'C12',
              'C13',
              'C14',
              'C15',
              'C16',
              'C17',
              'C18',
              'C19',
              'C20',
              'C21',
              'C22',
              'C23',
              'C24',
              'C25',
              'C26',
              'C30',
              'C31',
              'C32',
              'C33',
              'C34',
              'C37',
              'C38',
              'C39',
              'C40',
              'C41',
              'C43',
              'C45',
              'C46',
              'C47',
              'C48',
              'C49',
              'C50',
              'C51',
              'C52',
              'C53',
              'C54',
              'C55',
              'C56',
              'C57',
              'C58',
              'C60',
              'C61',
              'C62',
              'C63',
              'C64',
              'C65',
              'C66',
              'C67',
              'C68',
              'C69',
              'C70',
              'C71',
              'C72',
              'C73',
              'C74',
              'C75',
              'C76',
              'C81',
              'C82',
              'C83',
              'C84',
              'C85',
              'C88',
              'C90',
              'C91',
              'C92',
              'C93',
              'C94',
              'C95',
              'C96',
              'C97',
              '140',
              '141',
              '142',
              '143',
              '144',
              '145',
              '146',
              '147',
              '148',
              '149',
              '150',
              '151',
              '152',
              '153',
              '154',
              '155',
              '156',
              '157',
              '158',
              '159',
              '160',
              '161',
              '162',
              '163',
              '164',
              '165',
              '166',
              '167',
              '168',
              '169',
              '170',
              '171',
              '172',
              '174',
              '200',
              '201',
              '202',
              '203',
              '204',
              '205',
              '206',
              '207',
              '208'
            )
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('1958'))
        )
          THEN 'MALIGNANCY'
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('C77', 'C78', 'C79', 'C80', '196', '197', '198'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('1990', '1991'))
        )
          THEN 'METASTATIC_SOLID_TUMOR'
        WHEN CAST((CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('B20', 'B21', 'B22', 'B24', '042', '043', '044')) AS BOOLEAN)
          THEN 'AIDS'
        ELSE 'NONE'
      END
    ) AS VARCHAR (1000)) AS CATEGORY,
    CAST((
      CASE
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I21', '410', 'I22', '412'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I252'))
        )
          THEN 1
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I099', 'I110', 'I130', 'I132', 'I255', 'I420', 'I425', 'I426', 'I427', 'I428', 'I429', 'P290'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I43', 'I50', '428'))
        )
          THEN 1
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I731', 'I738', 'I739', 'I771', 'I790', 'I792', 'K551', 'K558', 'K559', 'Z958', 'Z959', '4439', '7854', 'V434'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('I70', 'I71', '441'))
        )
          THEN 1
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('H340'))
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
                 'G45',
                 'G46',
                 'I60',
                 'I61',
                 'I62',
                 'I63',
                 'I64',
                 'I65',
                 'I66',
                 'I67',
                 'I68',
                 'I69',
                 '430',
                 '431',
                 '432',
                 '433',
                 '434',
                 '435',
                 '436',
                 '437',
                 '438'
               )
             )
        )
          THEN 1
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('F051', 'G311'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('F00', 'F01', 'F02', 'F03', 'G30', '290'))
        )
          THEN 1
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('I278', 'I279', 'J684', 'J701', 'J703', '5064'))
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
                 'J40',
                 'J41',
                 'J42',
                 'J43',
                 'J44',
                 'J45',
                 'J46',
                 'J47',
                 'J60',
                 'J61',
                 'J62',
                 'J63',
                 'J64',
                 'J65',
                 'J66',
                 'J67',
                 '490',
                 '491',
                 '492',
                 '493',
                 '494',
                 '495',
                 '496',
                 '497',
                 '498',
                 '499',
                 '500',
                 '501',
                 '502',
                 '503',
                 '504',
                 '505'
               )
             )
        )
          THEN 1
        WHEN (
          (
            (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('M315', 'M351', 'M353', 'M360', '7100', '7101', '7104', '7140', '7141', '7142'))
            OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('M05', 'M06', 'M32', 'M33', 'M34', '725'))
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 5)) AS VARCHAR (1000)) IN ('71481'))
        )
          THEN 1
        WHEN CAST((CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('K25', 'K26', 'K27', 'K28', '531', '532', '533', '534')) AS BOOLEAN)
          THEN 1
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
              'K700',
              'K701',
              'K702',
              'K703',
              'K709',
              'K713',
              'K714',
              'K715',
              'K717',
              'K760',
              'K762',
              'K763',
              'K764',
              'K768',
              'K769',
              'Z944',
              '5712',
              '5714',
              '5715',
              '5716',
              '4560',
              '4562'
            )
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('B18', 'K73', 'K74'))
        )
          THEN 1
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
              'I850',
              'I859',
              'I864',
              'I982',
              'K704',
              'K711',
              'K721',
              'K729',
              'K765',
              'K766',
              'K767',
              '5722',
              '5723',
              '5724',
              '5725',
              '5726',
              '5727',
              '5728'
            )
          )
          OR (
               CAST((SUBSTRING(DIAG_CD, 1, 5)) AS VARCHAR (1000)) IN (
                 '45600',
                 '45601',
                 '45602',
                 '45603',
                 '45604',
                 '45605',
                 '45606',
                 '45607',
                 '45608',
                 '45609',
                 '45610',
                 '45611',
                 '45612',
                 '45613',
                 '45614',
                 '45615',
                 '45616',
                 '45617',
                 '45618',
                 '45619',
                 '45620',
                 '45621'
               )
             )
        )
          THEN 3
        WHEN CAST((
          CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
            'E100',
            'E101',
            'E106',
            'E108',
            'E109',
            'E110',
            'E111',
            'E116',
            'E118',
            'E119',
            'E120',
            'E121',
            'E126',
            'E128',
            'E129',
            'E130',
            'E131',
            'E136',
            'E138',
            'E139',
            'E140',
            'E141',
            'E146',
            'E148',
            'E149',
            '2500',
            '2501',
            '2502',
            '2503',
            '2507'
          )
        ) AS BOOLEAN)
          THEN 1
        WHEN CAST((
          CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
            '2504',
            '2505',
            '2506',
            'E102',
            'E103',
            'E104',
            'E105',
            'E112',
            'E113',
            'E114',
            'E115',
            'E117',
            'E122',
            'E123',
            'E124',
            'E125',
            'E127',
            'E132',
            'E133',
            'E134',
            'E135',
            'E137',
            'E142',
            'E143',
            'E144',
            'E145',
            'E147'
          )
        ) AS BOOLEAN)
          THEN 3
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('G041', 'G114', 'G801', 'G802', 'G831', 'G832', 'G833', 'G834', 'G839', '3441'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('G81', 'G82', '342'))
        )
          THEN 2
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN (
              'I120',
              'I131',
              'N032',
              'N033',
              'N034',
              'N035',
              'N036',
              'N037',
              'N052',
              'N053',
              'N054',
              'N055',
              'N056',
              'N057',
              'N250',
              'Z490',
              'Z491',
              'Z492',
              'Z940',
              'Z992'
            )
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('N18', 'N19', '582', '585', '586', '588', '583'))
        )
          THEN 2
        WHEN (
          (
            CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN (
              'C00',
              'C01',
              'C02',
              'C03',
              'C04',
              'C05',
              'C06',
              'C07',
              'C08',
              'C09',
              'C10',
              'C11',
              'C12',
              'C13',
              'C14',
              'C15',
              'C16',
              'C17',
              'C18',
              'C19',
              'C20',
              'C21',
              'C22',
              'C23',
              'C24',
              'C25',
              'C26',
              'C30',
              'C31',
              'C32',
              'C33',
              'C34',
              'C37',
              'C38',
              'C39',
              'C40',
              'C41',
              'C43',
              'C45',
              'C46',
              'C47',
              'C48',
              'C49',
              'C50',
              'C51',
              'C52',
              'C53',
              'C54',
              'C55',
              'C56',
              'C57',
              'C58',
              'C60',
              'C61',
              'C62',
              'C63',
              'C64',
              'C65',
              'C66',
              'C67',
              'C68',
              'C69',
              'C70',
              'C71',
              'C72',
              'C73',
              'C74',
              'C75',
              'C76',
              'C81',
              'C82',
              'C83',
              'C84',
              'C85',
              'C88',
              'C90',
              'C91',
              'C92',
              'C93',
              'C94',
              'C95',
              'C96',
              'C97',
              '140',
              '141',
              '142',
              '143',
              '144',
              '145',
              '146',
              '147',
              '148',
              '149',
              '150',
              '151',
              '152',
              '153',
              '154',
              '155',
              '156',
              '157',
              '158',
              '159',
              '160',
              '161',
              '162',
              '163',
              '164',
              '165',
              '166',
              '167',
              '168',
              '169',
              '170',
              '171',
              '172',
              '174',
              '200',
              '201',
              '202',
              '203',
              '204',
              '205',
              '206',
              '207',
              '208'
            )
          )
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('1958'))
        )
          THEN 2
        WHEN (
          (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('C77', 'C78', 'C79', 'C80', '196', '197', '198'))
          OR (CAST((SUBSTRING(DIAG_CD, 1, 4)) AS VARCHAR (1000)) IN ('1990', '1991'))
        )
          THEN 6
        WHEN CAST((CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('B20', 'B21', 'B22', 'B24', '042', '043', '044')) AS BOOLEAN)
          THEN 6
        ELSE 0
      END
    ) AS INTEGER) AS POINTS,
    *
  
  FROM AlteryxSelect_70 AS in0

),

Filter_46 AS (

  SELECT * 
  
  FROM Formula_45_0 AS in0
  
  WHERE (CATEGORY <> 'NONE')

),

Summarize_58 AS (

  SELECT DISTINCT MemberId AS MemberId
  
  FROM Filter_46 AS in0

),

AlteryxSelect_69 AS (

  SELECT *
  
  FROM {{ ref('CCI__AlteryxSelect_69')}}

),

Join_59_inner AS (

  SELECT in0.*
  
  FROM AlteryxSelect_69 AS in0
  INNER JOIN Summarize_58 AS in1
     ON (in0.MemberId = in1.MemberId)

),

Union_64_reformat_1 AS (

  SELECT 
    CATEGORY AS CATEGORY,
    (PARSE_DATE('%Y-%m-%d', EFF_DATE)) AS EFF_DATE,
    MemberId AS MemberId,
    (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', MONTH)) AS Month,
    POINTS AS POINTS
  
  FROM Join_59_inner AS in0

),

Summarize_62 AS (

  SELECT *
  
  FROM {{ ref('CCI__Summarize_62')}}

),

AppendFields_63 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_62 AS in0
  INNER JOIN Filter_46 AS in1
     ON true

),

Union_64_reformat_0 AS (

  SELECT 
    CATEGORY AS CATEGORY,
    CAST(DIAG_CD AS STRING) AS DIAG_CD,
    CAST(variableDate AS STRING) AS variableDate,
    (PARSE_DATE('%Y-%m-%d', EFF_DATE)) AS EFF_DATE,
    MemberId AS MemberId,
    POINTS AS POINTS,
    Sys_Claim_HI_ID AS Sys_Claim_HI_ID,
    Sys_Claim_ID AS Sys_Claim_ID,
    (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', YR_MO)) AS YR_MO
  
  FROM AppendFields_63 AS in0

),

Union_64 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_64_reformat_1', 'Union_64_reformat_0'], 
      [
        '[{"name": "MemberId", "dataType": "Integer"}, {"name": "CATEGORY", "dataType": "String"}, {"name": "Month", "dataType": "Timestamp"}, {"name": "EFF_DATE", "dataType": "Timestamp"}, {"name": "POINTS", "dataType": "Integer"}]', 
        '[{"name": "variableDate", "dataType": "String"}, {"name": "MemberId", "dataType": "Integer"}, {"name": "CATEGORY", "dataType": "String"}, {"name": "EFF_DATE", "dataType": "Date"}, {"name": "POINTS", "dataType": "Integer"}, {"name": "DIAG_CD", "dataType": "String"}, {"name": "Sys_Claim_ID", "dataType": "Integer"}, {"name": "Sys_Claim_HI_ID", "dataType": "Integer"}, {"name": "YR_MO", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_65 AS (

  SELECT * 
  
  FROM Union_64 AS in0
  
  WHERE ((PARSE_DATE('%Y-%m-%d', EFF_DATE)) <= (PARSE_DATE('%Y-%m-%d', YR_MO)))

),

Unique_78 AS (

  SELECT * 
  
  FROM Filter_65 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MemberId, CATEGORY, YR_MO ORDER BY MemberId, CATEGORY, YR_MO) = 1

)

SELECT *

FROM Unique_78
