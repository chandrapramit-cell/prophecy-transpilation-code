{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_92",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH aka_GPDIP_EDLUD_11 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Product_Selection_Disabled', 'aka_GPDIP_EDLUD_11') }}

),

Filter_15 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_11 AS in0
  
  WHERE (CAST(name AS string) IN ('GENERIC_NAME', 'PFE_XM_P_COMPOUND_NUM', 'PROPRIETARY_NAME'))

),

aka_GPDIP_EDLUD_113 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Product_Selection_Disabled', 'aka_GPDIP_EDLUD_113') }}

),

Cleanse_121 AS (

  {{
    prophecy_basics.DataCleansing(
      ['aka_GPDIP_EDLUD_113'], 
      [
        { "name": "is_active", "dataType": "String" }, 
        { "name": "is_static", "dataType": "String" }, 
        { "name": "product_name", "dataType": "String" }, 
        { "name": "parent_app_country_id", "dataType": "Integer" }, 
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

Filter_118_reject AS (

  SELECT * 
  
  FROM Cleanse_121 AS in0
  
  WHERE ((NOT(NOT(parent_app_country_id IS NULL))) OR ((NOT(parent_app_country_id IS NULL)) IS NULL))

),

Filter_118 AS (

  SELECT * 
  
  FROM Cleanse_121 AS in0
  
  WHERE (NOT(parent_app_country_id IS NULL))

),

Join_119_inner AS (

  SELECT 
    in0.is_active AS is_active,
    in0.is_static AS is_static,
    in0.product_name AS product_name,
    in0.parent_app_country_id AS parent_app_country_id,
    in0.app_country_id AS sub_country_id,
    in1.country_abbreviation AS country_abbreviation,
    in1.country_name AS country_name,
    in0.country_name AS sub_country_name
  
  FROM Filter_118 AS in0
  INNER JOIN Filter_118_reject AS in1
     ON (in0.parent_app_country_id = in1.app_country_id)

),

Filter_88 AS (

  SELECT * 
  
  FROM Join_119_inner AS in0
  
  WHERE (country_name = 'Core Document')

),

Filter_180 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_11 AS in0
  
  WHERE (name = 'PFLEET_SUB_COUNTRY_ROW_ID')

),

AlteryxSelect_181 AS (

  SELECT 
    CAST(value AS INT) AS `value`,
    * EXCEPT (`value`)
  
  FROM Filter_180 AS in0

),

Join_6_inner AS (

  SELECT 
    in1.r_object_id AS r_object_id,
    in0.product_name AS product_name,
    in0.sub_country_id AS sub_country_id,
    in0.country_abbreviation AS country_abbreviation,
    in0.country_name AS country_name
  
  FROM Filter_88 AS in0
  INNER JOIN AlteryxSelect_181 AS in1
     ON (in0.sub_country_id = in1.value)

),

aka_GPDIP_EDLUD_9 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Product_Selection_Disabled', 'aka_GPDIP_EDLUD_9') }}

),

AlteryxSelect_18 AS (

  SELECT 
    r_object_id AS r_object_id,
    i_has_folder AS i_has_folder,
    i_chronicle_id AS i_chronicle_id
  
  FROM aka_GPDIP_EDLUD_9 AS in0

),

Filter_17 AS (

  SELECT * 
  
  FROM AlteryxSelect_18 AS in0
  
  WHERE (i_has_folder = CAST('1' AS INTEGER))

),

Join_10_inner AS (

  SELECT 
    in1.r_object_id AS r_object_id,
    in0.product_name AS product_name,
    in0.sub_country_id AS sub_country_id,
    in0.country_abbreviation AS country_abbreviation,
    in1.i_chronicle_id AS i_chronicle_id,
    in0.country_name AS country_name
  
  FROM Join_6_inner AS in0
  INNER JOIN Filter_17 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Join_12_inner AS (

  SELECT 
    in1.name AS name,
    in1.i_position AS i_position,
    in0.r_object_id AS r_object_id,
    in0.product_name AS product_name,
    in0.sub_country_id AS sub_country_id,
    in0.country_abbreviation AS country_abbreviation,
    in1.value AS `value`,
    in0.i_chronicle_id AS i_chronicle_id,
    in0.country_name AS country_name
  
  FROM Join_10_inner AS in0
  INNER JOIN Filter_15 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Join_6_left AS (

  SELECT in0.*
  
  FROM Filter_88 AS in0
  ANTI JOIN AlteryxSelect_181 AS in1
     ON (in0.sub_country_id = in1.value)

),

Union_89 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_6_left', 'Join_12_inner'], 
      [
        '[{"name": "is_active", "dataType": "String"}, {"name": "is_static", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "parent_app_country_id", "dataType": "Integer"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "sub_country_name", "dataType": "String"}]', 
        '[{"name": "name", "dataType": "String"}, {"name": "i_position", "dataType": "Integer"}, {"name": "r_object_id", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "value", "dataType": "String"}, {"name": "i_chronicle_id", "dataType": "String"}, {"name": "country_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

CrossTab_23 AS (

  SELECT *
  
  FROM (
    SELECT 
      product_name,
      country_name,
      country_abbreviation,
      sub_country_id,
      r_object_id,
      i_chronicle_id,
      i_position,
      name,
      VALUE
    
    FROM Union_89 AS in0
  )
  PIVOT (
    CONCAT_WS(', ', COLLECT_LIST(VALUE)) AS Concat
    FOR name
    IN (
      'PFE_XM_P_COMPOUND_NUM', '_Null_', 'GENERIC_NAME', 'PROPRIETARY_NAME'
    )
  )

),

AlteryxSelect_27 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    PROPRIETARY_NAME AS PROPRIETARY_NAME
  
  FROM CrossTab_23 AS in0

),

Unique_31 AS (

  SELECT * 
  
  FROM AlteryxSelect_27 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, PROPRIETARY_NAME ORDER BY product_name, country_name, PROPRIETARY_NAME) = 1

),

Filter_41 AS (

  SELECT * 
  
  FROM Unique_31 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROPRIETARY_NAME)) = 0)
        )

),

Summarize_44 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(PROPRIETARY_NAME AS STRING), 'utf-8')))) AS trade_name_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_41 AS in0
  
  GROUP BY 
    product_name, country_name

),

AlteryxSelect_24 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    country_abbreviation AS country_abbreviation,
    sub_country_id AS sub_country_id
  
  FROM CrossTab_23 AS in0

),

Unique_28 AS (

  SELECT * 
  
  FROM AlteryxSelect_24 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, country_abbreviation, sub_country_id ORDER BY product_name, country_name, country_abbreviation, sub_country_id) = 1

),

Filter_38 AS (

  SELECT * 
  
  FROM Unique_28 AS in0
  
  WHERE (
          NOT(
            (LENGTH(CAST(sub_country_id AS string))) = 0)
        )

),

Summarize_33 AS (

  SELECT 
    concat(concat_ws('|', array_sort(collect_list(encode(CAST(sub_country_id AS STRING), 'utf-8')))), '|') AS sub_country_id_set,
    product_name AS product_name,
    country_name AS country_name,
    country_abbreviation AS country_abbreviation
  
  FROM Filter_38 AS in0
  
  GROUP BY 
    product_name, country_name, country_abbreviation

),

AlteryxSelect_26 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    PFE_XM_P_COMPOUND_NUM AS PFE_XM_P_COMPOUND_NUM
  
  FROM CrossTab_23 AS in0

),

Unique_30 AS (

  SELECT * 
  
  FROM AlteryxSelect_26 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, PFE_XM_P_COMPOUND_NUM ORDER BY product_name, country_name, PFE_XM_P_COMPOUND_NUM) = 1

),

Filter_40 AS (

  SELECT * 
  
  FROM Unique_30 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PFE_XM_P_COMPOUND_NUM)) = 0)
        )

),

Summarize_43 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(PFE_XM_P_COMPOUND_NUM AS STRING), 'utf-8')))) AS compound_number_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_40 AS in0
  
  GROUP BY 
    product_name, country_name

),

AlteryxSelect_25 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    GENERIC_NAME AS GENERIC_NAME
  
  FROM CrossTab_23 AS in0

),

Unique_29 AS (

  SELECT * 
  
  FROM AlteryxSelect_25 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, GENERIC_NAME ORDER BY product_name, country_name, GENERIC_NAME) = 1

),

Filter_39 AS (

  SELECT * 
  
  FROM Unique_29 AS in0
  
  WHERE (
          NOT(
            (LENGTH(GENERIC_NAME)) = 0)
        )

),

Summarize_42 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(GENERIC_NAME AS STRING), 'utf-8')))) AS generic_name_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_39 AS in0
  
  GROUP BY 
    product_name, country_name

),

JoinMultiple_45 AS (

  SELECT 
    in1.generic_name_set AS generic_name_set,
    in2.compound_number_set AS compound_number_set,
    in0.product_name AS product_name,
    in3.trade_name_set AS trade_name_set,
    in0.country_abbreviation AS country_abbreviation,
    in0.country_name AS country_name,
    in0.sub_country_id_set AS sub_country_id_set
  
  FROM Summarize_33 AS in0
  FULL JOIN Summarize_42 AS in1
     ON ((in0.product_name = in1.product_name) AND (in0.country_name = in1.country_name))
  FULL JOIN Summarize_43 AS in2
     ON (
      (coalesce(in0.product_name, in1.product_name) = in2.product_name)
      AND (coalesce(in0.country_name, in1.country_name) = in2.country_name)
    )
  FULL JOIN Summarize_44 AS in3
     ON (
      (coalesce(in0.product_name, in1.product_name, in2.product_name) = in3.product_name)
      AND (coalesce(in0.country_name, in1.country_name, in2.country_name) = in3.country_name)
    )

),

Filter_88_reject AS (

  SELECT * 
  
  FROM Join_119_inner AS in0
  
  WHERE (
          (
            NOT(
              country_name = 'Core Document')
          ) OR ((country_name = 'Core Document') IS NULL)
        )

),

aka_GPDIP_EDLUD_133 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Product_Selection_Disabled', 'aka_GPDIP_EDLUD_133') }}

),

AlteryxSelect_123 AS (

  SELECT 
    r_object_id AS r_object_id,
    i_has_folder AS i_has_folder,
    i_chronicle_id AS i_chronicle_id
  
  FROM aka_GPDIP_EDLUD_133 AS in0

),

Filter_124 AS (

  SELECT * 
  
  FROM AlteryxSelect_123 AS in0
  
  WHERE (i_has_folder = CAST('1' AS INTEGER))

),

aka_GPDIP_EDLUD_132 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Product_Selection_Disabled', 'aka_GPDIP_EDLUD_132') }}

),

Filter_142 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_132 AS in0
  
  WHERE (name = 'PFLEET_SUB_COUNTRY_ROW_ID')

),

AlteryxSelect_143 AS (

  SELECT 
    CAST(value AS INT) AS `value`,
    * EXCEPT (`value`)
  
  FROM Filter_142 AS in0

),

Join_128_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`r_object_id`, `i_has_folder`, `i_chronicle_id`)
  
  FROM AlteryxSelect_143 AS in0
  INNER JOIN Filter_124 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Join_125_inner AS (

  SELECT 
    in1.r_object_id AS r_object_id,
    in0.product_name AS product_name,
    in0.sub_country_id AS sub_country_id,
    in0.country_abbreviation AS country_abbreviation,
    in0.country_name AS country_name
  
  FROM Filter_88_reject AS in0
  INNER JOIN Join_128_inner AS in1
     ON (in0.sub_country_id = in1.value)

),

Filter_130 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_132 AS in0
  
  WHERE (CAST(name AS string) IN ('GENERIC_NAME', 'PFE_XM_P_COMPOUND_NUM', 'PROPRIETARY_NAME'))

),

Join_129_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`r_object_id`)
  
  FROM Join_125_inner AS in0
  INNER JOIN Filter_130 AS in1
     ON (in0.r_object_id = in1.r_object_id)

),

Join_125_left AS (

  SELECT in0.*
  
  FROM Filter_88_reject AS in0
  ANTI JOIN Join_128_inner AS in1
     ON (in0.sub_country_id = in1.value)

),

Union_131 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_125_left', 'Join_129_inner'], 
      [
        '[{"name": "is_active", "dataType": "String"}, {"name": "is_static", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "parent_app_country_id", "dataType": "Integer"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "sub_country_name", "dataType": "String"}]', 
        '[{"name": "r_object_id", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "sub_country_id", "dataType": "Integer"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "i_position", "dataType": "Integer"}, {"name": "name", "dataType": "String"}, {"name": "value", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

CrossTab_144 AS (

  SELECT *
  
  FROM (
    SELECT 
      product_name,
      country_name,
      country_abbreviation,
      sub_country_id,
      r_object_id,
      i_position,
      name,
      VALUE
    
    FROM Union_131 AS in0
  )
  PIVOT (
    CONCAT_WS(', ', COLLECT_LIST(VALUE)) AS Concat
    FOR name
    IN (
      'PFE_XM_P_COMPOUND_NUM', '_Null_', 'GENERIC_NAME', 'PROPRIETARY_NAME'
    )
  )

),

AlteryxSelect_152 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    GENERIC_NAME AS GENERIC_NAME
  
  FROM CrossTab_144 AS in0

),

Unique_153 AS (

  SELECT * 
  
  FROM AlteryxSelect_152 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, GENERIC_NAME ORDER BY product_name, country_name, GENERIC_NAME) = 1

),

AlteryxSelect_158 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    PFE_XM_P_COMPOUND_NUM AS PFE_XM_P_COMPOUND_NUM
  
  FROM CrossTab_144 AS in0

),

AlteryxSelect_146 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    country_abbreviation AS country_abbreviation,
    CAST(sub_country_id AS string) AS sub_country_id
  
  FROM CrossTab_144 AS in0

),

Unique_147 AS (

  SELECT * 
  
  FROM AlteryxSelect_146 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, country_abbreviation, sub_country_id ORDER BY product_name, country_name, country_abbreviation, sub_country_id) = 1

),

Filter_150 AS (

  SELECT * 
  
  FROM Unique_147 AS in0
  
  WHERE (
          NOT(
            (LENGTH(sub_country_id)) = 0)
        )

),

Summarize_148 AS (

  SELECT 
    concat(concat_ws('|', array_sort(collect_list(encode(CAST(sub_country_id AS STRING), 'utf-8')))), '|') AS sub_country_id_set,
    product_name AS product_name,
    country_name AS country_name,
    country_abbreviation AS country_abbreviation
  
  FROM Filter_150 AS in0
  
  GROUP BY 
    product_name, country_name, country_abbreviation

),

Unique_159 AS (

  SELECT * 
  
  FROM AlteryxSelect_158 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, PFE_XM_P_COMPOUND_NUM ORDER BY product_name, country_name, PFE_XM_P_COMPOUND_NUM) = 1

),

Filter_161 AS (

  SELECT * 
  
  FROM Unique_159 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PFE_XM_P_COMPOUND_NUM)) = 0)
        )

),

Summarize_162 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(PFE_XM_P_COMPOUND_NUM AS STRING), 'utf-8')))) AS compound_number_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_161 AS in0
  
  GROUP BY 
    product_name, country_name

),

Filter_155 AS (

  SELECT * 
  
  FROM Unique_153 AS in0
  
  WHERE (
          NOT(
            (LENGTH(GENERIC_NAME)) = 0)
        )

),

Summarize_156 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(GENERIC_NAME AS STRING), 'utf-8')))) AS generic_name_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_155 AS in0
  
  GROUP BY 
    product_name, country_name

),

AlteryxSelect_164 AS (

  SELECT 
    product_name AS product_name,
    country_name AS country_name,
    PROPRIETARY_NAME AS PROPRIETARY_NAME
  
  FROM CrossTab_144 AS in0

),

Unique_165 AS (

  SELECT * 
  
  FROM AlteryxSelect_164 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_name, country_name, PROPRIETARY_NAME ORDER BY product_name, country_name, PROPRIETARY_NAME) = 1

),

Filter_167 AS (

  SELECT * 
  
  FROM Unique_165 AS in0
  
  WHERE (
          NOT(
            (LENGTH(PROPRIETARY_NAME)) = 0)
        )

),

Summarize_168 AS (

  SELECT 
    concat_ws('|', array_sort(collect_list(encode(CAST(PROPRIETARY_NAME AS STRING), 'utf-8')))) AS trade_name_set,
    product_name AS product_name,
    country_name AS country_name
  
  FROM Filter_167 AS in0
  
  GROUP BY 
    product_name, country_name

),

JoinMultiple_145 AS (

  SELECT 
    in1.generic_name_set AS generic_name_set,
    in2.compound_number_set AS compound_number_set,
    in0.product_name AS product_name,
    in3.trade_name_set AS trade_name_set,
    in0.country_abbreviation AS country_abbreviation,
    in0.country_name AS country_name,
    in0.sub_country_id_set AS sub_country_id_set
  
  FROM Summarize_148 AS in0
  FULL JOIN Summarize_156 AS in1
     ON ((in0.product_name = in1.product_name) AND (in0.country_name = in1.country_name))
  FULL JOIN Summarize_162 AS in2
     ON (
      (coalesce(in0.product_name, in1.product_name) = in2.product_name)
      AND (coalesce(in0.country_name, in1.country_name) = in2.country_name)
    )
  FULL JOIN Summarize_168 AS in3
     ON (
      (coalesce(in0.product_name, in1.product_name, in2.product_name) = in3.product_name)
      AND (coalesce(in0.country_name, in1.country_name, in2.country_name) = in3.country_name)
    )

),

Union_84 AS (

  {{
    prophecy_basics.UnionByName(
      ['JoinMultiple_145', 'JoinMultiple_45'], 
      [
        '[{"name": "generic_name_set", "dataType": "String"}, {"name": "compound_number_set", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "trade_name_set", "dataType": "String"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "sub_country_id_set", "dataType": "String"}]', 
        '[{"name": "generic_name_set", "dataType": "String"}, {"name": "compound_number_set", "dataType": "String"}, {"name": "product_name", "dataType": "String"}, {"name": "trade_name_set", "dataType": "String"}, {"name": "country_abbreviation", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "sub_country_id_set", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_94_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS extract_date,
    *
  
  FROM Union_84 AS in0

)

SELECT *

FROM Formula_94_0
