{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_295",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH aka_GPDIP_EDLUD_298 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_298') }}

),

aka_GPDIP_EDLUD_302 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_302') }}

),

Filter_303_reject AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_302 AS in0
  
  WHERE (
          (
            NOT(
              name = 'PFLEET_SUB_COUNTRY_ROW_ID')
          ) OR ((name = 'PFLEET_SUB_COUNTRY_ROW_ID') IS NULL)
        )

),

Summarize_305 AS (

  SELECT 
    concat_ws(',', array_sort(collect_list(encode(CAST(value AS STRING), 'utf-8')))) AS r_version_label,
    r_object_id AS r_object_id
  
  FROM Filter_303_reject AS in0
  
  GROUP BY r_object_id

),

Filter_303 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_302 AS in0
  
  WHERE (name = 'PFLEET_SUB_COUNTRY_ROW_ID')

),

AlteryxSelect_327 AS (

  SELECT 
    CAST(value AS INT) AS pfleet_subcountry_row_id,
    * EXCEPT (`value`)
  
  FROM Filter_303 AS in0

),

Join_306_inner AS (

  SELECT 
    in0.r_object_id AS r_object_id,
    in0.pfleet_subcountry_row_id AS pfleet_subcountry_row_id,
    in1.r_version_label AS r_version_label
  
  FROM AlteryxSelect_327 AS in0
  INNER JOIN Summarize_305 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

aka_GPDIP_EDLUD_289 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_289') }}

),

Cleanse_290 AS (

  {{
    prophecy_basics.DataCleansing(
      ['aka_GPDIP_EDLUD_289'], 
      [
        { "name": "is_active", "dataType": "String" }, 
        { "name": "is_static", "dataType": "String" }, 
        { "name": "extract_date", "dataType": "Timestamp" }, 
        { "name": "product_name", "dataType": "String" }, 
        { "name": "country_abbreviation", "dataType": "String" }, 
        { "name": "app_country_id", "dataType": "Integer" }, 
        { "name": "country_name", "dataType": "String" }
      ], 
      'makeUppercase', 
      ['country_name', 'product_name'], 
      false, 
      '', 
      false, 
      0, 
      false, 
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

Join_310_inner AS (

  SELECT 
    in0.* EXCEPT (`country_abbreviation`, `product_name`, `is_static`, `is_active`),
    in1.* EXCEPT (`pfleet_subcountry_row_id`)
  
  FROM Cleanse_290 AS in0
  INNER JOIN Join_306_inner AS in1
     ON (in0.app_country_id = in1.pfleet_subcountry_row_id)

),

Join_299_inner AS (

  SELECT 
    in0.app_country_id AS sub_country_id,
    in0.country_name AS sub_country_name,
    in0.r_object_id AS r_object_id,
    in1.i_chronicle_id AS i_chronicle_id,
    in1.object_name AS object_name,
    in1.title AS title,
    in1.xm_status AS xm_status,
    in1.subtype AS subtype,
    in0.r_version_label AS r_version_label,
    in1.xm_language AS xm_language,
    in0.extract_date AS extract_date,
    in0.* EXCEPT (`app_country_id`, `country_name`, `r_object_id`, `r_version_label`, `extract_date`),
    in1.* EXCEPT (`r_object_id`, `i_chronicle_id`, `object_name`, `title`, `xm_status`, `i_has_folder`, `subtype`, `xm_language`)
  
  FROM Join_310_inner AS in0
  INNER JOIN aka_GPDIP_EDLUD_298 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Formula_300_0 AS (

  SELECT 
    CAST((CONCAT('http://gdms.pfizer.com/gdms/drl/objectId/', r_object_id)) AS string) AS document_url,
    *
  
  FROM Join_299_inner AS in0

),

aka_GPDIP_EDLUD_324 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_324') }}

),

Filter_323_reject AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_324 AS in0
  
  WHERE (
          (
            NOT(
              name = 'PFLEET_SUB_COUNTRY_ROW_ID')
          ) OR ((name = 'PFLEET_SUB_COUNTRY_ROW_ID') IS NULL)
        )

),

Summarize_317 AS (

  SELECT 
    concat_ws(',', array_sort(collect_list(encode(CAST(value AS STRING), 'utf-8')))) AS r_version_label,
    r_object_id AS r_object_id
  
  FROM Filter_323_reject AS in0
  
  GROUP BY r_object_id

),

Filter_323 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_324 AS in0
  
  WHERE (name = 'PFLEET_SUB_COUNTRY_ROW_ID')

),

AlteryxSelect_326 AS (

  SELECT 
    CAST(value AS INT) AS pfleet_subcountry_row_id,
    * EXCEPT (`value`)
  
  FROM Filter_323 AS in0

),

Join_322_inner AS (

  SELECT 
    in0.r_object_id AS r_object_id,
    in0.pfleet_subcountry_row_id AS pfleet_subcountry_row_id,
    in1.r_version_label AS r_version_label
  
  FROM AlteryxSelect_326 AS in0
  INNER JOIN Summarize_317 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Join_318_inner AS (

  SELECT 
    in0.* EXCEPT (`country_abbreviation`, `product_name`, `is_static`, `is_active`),
    in1.* EXCEPT (`pfleet_subcountry_row_id`)
  
  FROM Cleanse_290 AS in0
  INNER JOIN Join_322_inner AS in1
     ON (in0.app_country_id = in1.pfleet_subcountry_row_id)

),

aka_GPDIP_EDLUD_312 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_312') }}

),

Join_313_inner AS (

  SELECT 
    in0.app_country_id AS sub_country_id,
    in0.country_name AS sub_country_name,
    in0.r_object_id AS r_object_id,
    in1.i_chronicle_id AS i_chronicle_id,
    in1.object_name AS object_name,
    in1.title AS title,
    in1.xm_status AS xm_status,
    in1.subtype AS subtype,
    in0.r_version_label AS r_version_label,
    in1.xm_language AS xm_language,
    in0.extract_date AS extract_date,
    in0.* EXCEPT (`app_country_id`, `country_name`, `r_object_id`, `r_version_label`, `extract_date`),
    in1.* EXCEPT (`r_object_id`, `i_chronicle_id`, `object_name`, `title`, `xm_status`, `i_has_folder`, `subtype`, `xm_language`)
  
  FROM Join_318_inner AS in0
  INNER JOIN aka_GPDIP_EDLUD_312 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Formula_314_0 AS (

  SELECT 
    CAST((CONCAT('http://gdms.pfizer.com/gdms/drl/objectId/', r_object_id)) AS string) AS document_url,
    *
  
  FROM Join_313_inner AS in0

),

Union_292 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_300_0', 'Formula_314_0'], 
      [
        '[{"name": "document_url", "dataType": "String"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "sub_country_name", "dataType": "String"}, {"name": "r_object_id", "dataType": "String"}, {"name": "i_chronicle_id", "dataType": "String"}, {"name": "object_name", "dataType": "String"}, {"name": "title", "dataType": "String"}, {"name": "xm_status", "dataType": "String"}, {"name": "subtype", "dataType": "String"}, {"name": "r_version_label", "dataType": "String"}, {"name": "xm_language", "dataType": "String"}, {"name": "extract_date", "dataType": "Timestamp"}]', 
        '[{"name": "document_url", "dataType": "String"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "sub_country_name", "dataType": "String"}, {"name": "r_object_id", "dataType": "String"}, {"name": "i_chronicle_id", "dataType": "String"}, {"name": "object_name", "dataType": "String"}, {"name": "title", "dataType": "String"}, {"name": "xm_status", "dataType": "String"}, {"name": "subtype", "dataType": "String"}, {"name": "r_version_label", "dataType": "String"}, {"name": "xm_language", "dataType": "String"}, {"name": "extract_date", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_294 AS (

  SELECT 
    sub_country_name AS sub_country_name,
    r_object_id AS r_object_id,
    object_name AS object_name,
    title AS title,
    xm_status AS xm_status,
    subtype AS subtype,
    * EXCEPT (`sub_country_name`, `r_object_id`, `object_name`, `title`, `xm_status`, `subtype`)
  
  FROM Union_292 AS in0

)

SELECT *

FROM AlteryxSelect_294
