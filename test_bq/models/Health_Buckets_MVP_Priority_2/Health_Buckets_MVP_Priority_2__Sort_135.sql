{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH GenerateRows_128 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Health_Buckets_MVP_Priority_2', 'GenerateRows_128') }}

),

MultiRowFormula_129_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__MultiRowFormula_129_row_id_drop_0')}}

),

Sort_130 AS (

  SELECT * 
  
  FROM GenerateRows_128 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC, previous ASC

),

MultiRowFormula_131_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM Sort_130 AS in0

),

MultiRowFormula_131 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_131_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_131 AS in0

),

Join_126_inner AS (

  SELECT 
    in0.MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    in0.RecordNum AS RecordNum,
    in0.NumRows AS NumRows,
    in0.previous AS previous,
    in1.RecordNum AS Right_RecordNum,
    in0.`weight field` AS `weight field`,
    in0.YearMonth AS YearMonth,
    in1.TotalRVU AS TotalRVU,
    in1.V AS V,
    in1.VPP AS VPP,
    in1.ACC AS ACC,
    in1.RunTot_Inflection AS RunTot_Inflection,
    in1.Avg_TotalRVU AS Avg_TotalRVU,
    in1.Avg_TotalRVU_Full AS Avg_TotalRVU_Full,
    in1.Avg_TotalRVU_Pred AS Avg_TotalRVU_Pred,
    in1.CountDistinct_CLM_ID AS CountDistinct_CLM_ID,
    in1.MBR_AGE_RANGE AS MBR_AGE_RANGE,
    in1.* EXCEPT (`MBR_INDV_BE_KEY`, 
    `RecordNum`, 
    `YearMonth`, 
    `TotalRVU`, 
    `V`, 
    `VPP`, 
    `ACC`, 
    `RunTot_Inflection`, 
    `Avg_TotalRVU`, 
    `Avg_TotalRVU_Full`, 
    `Avg_TotalRVU_Pred`, 
    `CountDistinct_CLM_ID`, 
    `MBR_AGE_RANGE`)
  
  FROM MultiRowFormula_131_row_id_drop_0 AS in0
  INNER JOIN MultiRowFormula_129_row_id_drop_0 AS in1
     ON ((in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY) AND (in0.previous = in1.RecordNum))

),

Sort_134 AS (

  SELECT * 
  
  FROM Join_126_inner AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC, previous ASC, `weight field` ASC

),

MultiFieldFormula_136 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Sort_134'], 
      "CASE WHEN (pmod(`weight field`, 3) = 1) THEN (column_value / 3) WHEN (pmod(`weight field`, 3) = 2) THEN ((column_value * 2) / 3) ELSE column_value END", 
      [
        'MBR_INDV_BE_KEY', 
        'RecordNum', 
        'NumRows', 
        'previous', 
        'Right_RecordNum', 
        'weight field', 
        'YearMonth', 
        'TotalRVU', 
        'V', 
        'VPP', 
        'ACC', 
        'RunTot_Inflection', 
        'Avg_TotalRVU', 
        'Avg_TotalRVU_Full', 
        'Avg_TotalRVU_Pred', 
        'CountDistinct_CLM_ID', 
        'MBR_AGE_RANGE', 
        'DOB', 
        'SUB_ID', 
        '32CNTY_FLAG', 
        'RURAL_FLAG', 
        'MBR_EMAIL_ADDR', 
        'MBR_HOME_ADDR_PHN_NO', 
        'MBR_HOME_ADDR_CITY_NM', 
        'MAX_ENR_TERM_DT', 
        'SUB_CNTGS_CNTY_CD', 
        'MBR_HOME_ADDR_LN_2', 
        'MBR_HOME_ADDR_ZIP_CD_5', 
        'UNRELIABLE_HOME_ADDR_FLAG', 
        'SUB_OR_DPNDT_UNDER_18_FLAG', 
        'PROD_SH_NM_CAT_CD', 
        'SUB_MKTNG_METRO_RURAL_CD', 
        'MBR_CELL_PHN_NO', 
        'GRP_NM', 
        'GRP_ID', 
        'EXPRNC_CAT_CD', 
        'FUND_CAT_CD', 
        'GRP_INDUSTRY', 
        'MAX_ENR_EFF_DT', 
        'CLS_PLN_DESC', 
        'TOTAL_LIVING_IN_HOUSEHOLD', 
        'PROD_SH_NM_DLVRY_METH_CD', 
        'SPIRA_FLAG', 
        'PROD_SK', 
        'MBR_HOME_ADDR_CNTY_NM', 
        'PROD_SH_NM', 
        'MBR_FULL_NM', 
        'FNCL_LOB_DESC', 
        'CLS_ID', 
        'SPIRA_BNF_ID', 
        'MBR_GNDR_CD', 
        'MBR_HOME_ADDR_LN_1', 
        'FUND_CAT_DESC', 
        'SUB_FLAG', 
        'POPULATION_TYPE', 
        'GRP_INDUSTRY_ROLLUP', 
        'MBR_ID', 
        'MBR_RELSHP_CD', 
        'MBR_HOME_ADDR_ST_CD', 
        'MBR_AGE', 
        'DIRECT_PAY_IN', 
        'FNCL_LOB_CD', 
        'FNCL_MKT_SEG_NM', 
        'TOTAL_POLICY_MEMBERS', 
        'MIN_RELATIONSHIP_EFF_DT', 
        'MBR_UNIQ_KEY'
      ], 
      ['TotalRVU'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Summarize_137 AS (

  SELECT 
    AVG(TotalRVU) AS Avg_TotalRVU,
    MAX(MBR_AGE_RANGE) AS Max_MBR_AGE_RANGE,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth
  
  FROM MultiFieldFormula_136 AS in0
  
  GROUP BY 
    MBR_INDV_BE_KEY, YearMonth

),

DynamicRename_162 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['Summarize_137'], 
      ['Max_MBR_AGE_RANGE'], 
      'advancedRename', 
      ['MBR_INDV_BE_KEY', 'YearMonth', 'Avg_TotalRVU', 'Max_MBR_AGE_RANGE'], 
      'Suffix', 
      '', 
      "
      CASE
        WHEN column_name LIKE 'Max_%' THEN SUBSTRING(column_name, LENGTH('Max_') + 1, LENGTH(column_name) - LENGTH('Max_'))
        ELSE column_name
      END
      "
    )
  }}

),

Sort_135 AS (

  SELECT * 
  
  FROM DynamicRename_162 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC

)

SELECT *

FROM Sort_135
