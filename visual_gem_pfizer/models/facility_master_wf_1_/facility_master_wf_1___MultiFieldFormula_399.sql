{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default2"
  })
}}

WITH aka_GPDIP_EDLUD_28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_28') }}

),

AlteryxSelect_30 AS (

  SELECT * EXCEPT (`start_dt`, `end_dt`, `delete_flag`, `pmdm_guid`)
  
  FROM aka_GPDIP_EDLUD_28 AS in0

),

Summarize_45 AS (

  SELECT 
    MAX(load_date) AS Max_load_date,
    person_name_prefix AS person_name_prefix,
    person_id AS person_id,
    sip_person_id AS sip_person_id,
    ir_person_id AS ir_person_id,
    person_gender AS person_gender,
    person_full_name AS person_full_name,
    person_fst_name AS person_fst_name,
    pmdm_status_comment AS pmdm_status_comment,
    pmdm_status AS pmdm_status,
    person_last_name AS person_last_name,
    person_name_suffix AS person_name_suffix,
    person_mid_name AS person_mid_name
  
  FROM AlteryxSelect_30 AS in0
  
  GROUP BY 
    person_name_prefix, 
    person_id, 
    sip_person_id, 
    ir_person_id, 
    person_gender, 
    person_full_name, 
    person_fst_name, 
    pmdm_status_comment, 
    pmdm_status, 
    person_last_name, 
    person_name_suffix, 
    person_mid_name

),

aka_GPDIP_EDLUD_24 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_24') }}

),

AlteryxSelect_27 AS (

  SELECT * EXCEPT (`start_dt`, `end_dt`, `delete_flag`, `pmdm_guid`)
  
  FROM aka_GPDIP_EDLUD_24 AS in0

),

Cleanse_968 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_27'], 
      [
        { "name": "sip_facility_id", "dataType": "String" }, 
        { "name": "load_date", "dataType": "Timestamp" }, 
        { "name": "ir_facility_id", "dataType": "String" }, 
        { "name": "org_dept", "dataType": "String" }, 
        { "name": "org_name", "dataType": "String" }, 
        { "name": "organization_id", "dataType": "Double" }, 
        { "name": "org_tax_id", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['org_name'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
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

MultiRowFormula_1046_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM Cleanse_968 AS in0

),

MultiRowFormula_1046_0 AS (

  SELECT 
    (LAG(org_name, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS org_name_lag1,
    (LEAD(org_name, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS org_name_lead1,
    *
  
  FROM MultiRowFormula_1046_row_id_0 AS in0

),

MultiRowFormula_1046_1 AS (

  SELECT 
    (
      CASE
        WHEN (org_name = UPPER(org_name_lag1))
          THEN org_name_lag1
        WHEN (org_name = UPPER(org_name_lead1))
          THEN org_name_lead1
        ELSE org_name
      END
    ) AS org_name,
    * EXCEPT (`org_name_lag1`, `org_name_lead1`, `org_name`)
  
  FROM MultiRowFormula_1046_0 AS in0

),

MultiRowFormula_1046_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_1046_1 AS in0

),

Summarize_236 AS (

  SELECT 
    MAX(load_date) AS Max_load_date,
    sip_facility_id AS sip_facility_id,
    ir_facility_id AS ir_facility_id,
    org_dept AS org_dept,
    org_name AS org_name,
    organization_id AS organization_id,
    org_tax_id AS org_tax_id
  
  FROM MultiRowFormula_1046_row_id_drop_0 AS in0
  
  GROUP BY 
    sip_facility_id, ir_facility_id, org_dept, org_name, organization_id, org_tax_id

),

aka_GPDIP_EDLUD_2 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_2') }}

),

aka_GPDIP_EDLUD_1056 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_1056') }}

),

aka_GPDIP_EDLUD_1053 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_1053') }}

),

AlteryxSelect_1054 AS (

  SELECT 
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    * EXCEPT (`latitude`, `longitude`)
  
  FROM aka_GPDIP_EDLUD_1053 AS in0

),

Join_1057_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.address_id = in1.address_id)
          THEN in1.longitude
        ELSE NULL
      END
    ) AS goog_longitude,
    (
      CASE
        WHEN (in0.address_id = in1.address_id)
          THEN in1.latitude
        ELSE NULL
      END
    ) AS goog_latitude,
    in0.*,
    in1.* EXCEPT (`address_id`, `latitude`, `longitude`)
  
  FROM AlteryxSelect_1054 AS in0
  LEFT JOIN aka_GPDIP_EDLUD_1056 AS in1
     ON (in0.address_id = in1.address_id)

),

Formula_1059_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (NOT(goog_latitude IS NULL))
          THEN goog_latitude
        ELSE CAST(latitude AS string)
      END
    ) AS DOUBLE) AS latitude,
    CAST((
      CASE
        WHEN (NOT(goog_longitude IS NULL))
          THEN goog_longitude
        ELSE CAST(longitude AS string)
      END
    ) AS DOUBLE) AS longitude,
    * EXCEPT (`latitude`, `longitude`)
  
  FROM Join_1057_left_UnionLeftOuter AS in0

),

AlteryxSelect_1061 AS (

  SELECT * EXCEPT (`goog_latitude`, `goog_longitude`)
  
  FROM Formula_1059_0 AS in0

),

Cleanse_1055 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_1061'], 
      [
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "postal_cd", "dataType": "String" }, 
        { "name": "mail_box_suite", "dataType": "String" }, 
        { "name": "delete_flag", "dataType": "String" }, 
        { "name": "load_date", "dataType": "Timestamp" }, 
        { "name": "zip5", "dataType": "String" }, 
        { "name": "address_line1_cleansed", "dataType": "String" }, 
        { "name": "pmdm_guid", "dataType": "String" }, 
        { "name": "street_continued", "dataType": "String" }, 
        { "name": "address_id", "dataType": "Double" }, 
        { "name": "start_dt", "dataType": "Timestamp" }, 
        { "name": "zip4", "dataType": "String" }, 
        { "name": "end_dt", "dataType": "Timestamp" }, 
        { "name": "geoaccuracy", "dataType": "String" }, 
        { "name": "geocodingsystem", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "street", "dataType": "String" }, 
        { "name": "state_province_county", "dataType": "String" }, 
        { "name": "street_cleansed", "dataType": "String" }, 
        { "name": "country_name", "dataType": "String" }, 
        { "name": "address_line2_cleansed", "dataType": "String" }
      ], 
      'makeTitleCase', 
      ['city_town_village'], 
      true, 
      '', 
      true, 
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

CreatePoints_199_cast_lonlat AS (

  SELECT 
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(latitude AS DOUBLE) AS latitude,
    * EXCEPT (`longitude`, `latitude`)
  
  FROM Cleanse_1055 AS in0

),

CreatePoints_199 AS (

  {{
    DatabricksSqlSpatial.CreatePoint(
      'CreatePoints_199_cast_lonlat', 
      [['longitude', 'latitude', 'Centroid']]
    )
  }}

),

cb_2018_us_stat_207 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'cb_2018_us_stat_207') }}

),

AlteryxSelect_208 AS (

  SELECT 
    STATEFP AS STATEFP,
    STUSPS AS STUSPS,
    NAME AS StateName
  
  FROM cb_2018_us_stat_207 AS in0

),

cb_2018_us_coun_204 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'cb_2018_us_coun_204') }}

),

Join_195_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`STATEFP`)
  
  FROM cb_2018_us_coun_204 AS in0
  INNER JOIN AlteryxSelect_208 AS in1
     ON (in0.STATEFP = in1.STATEFP)

),

AlteryxSelect_196 AS (

  SELECT 
    GEOID AS FIPS,
    NAME AS County,
    SpatialObj AS SpatialObj,
    STUSPS AS State2,
    StateName AS State
  
  FROM Join_195_inner AS in0

),

SpatialMatch_200_unmatched AS (

  {{ prophecy_basics.ToDo('SpatialMatch is not supported.') }}

),

SpatialMatch_200 AS (

  {{ prophecy_basics.ToDo('SpatialMatch is not supported.') }}

),

Union_201 AS (

  {{
    prophecy_basics.UnionByName(
      ['SpatialMatch_200', 'SpatialMatch_200_unmatched'], 
      [
        '[{"name": "postal_cd", "dataType": "String"}, {"name": "mail_box_suite", "dataType": "String"}, {"name": "delete_flag", "dataType": "String"}, {"name": "load_date", "dataType": "Timestamp"}, {"name": "latitude", "dataType": "Double"}, {"name": "SpatialObj", "dataType": "String"}, {"name": "zip5", "dataType": "String"}, {"name": "FIPS", "dataType": "String"}, {"name": "address_line1_cleansed", "dataType": "String"}, {"name": "pmdm_guid", "dataType": "String"}, {"name": "street_continued", "dataType": "String"}, {"name": "address_id", "dataType": "Double"}, {"name": "start_dt", "dataType": "Timestamp"}, {"name": "State2", "dataType": "String"}, {"name": "zip4", "dataType": "String"}, {"name": "end_dt", "dataType": "Timestamp"}, {"name": "geoaccuracy", "dataType": "String"}, {"name": "geocodingsystem", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "County", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "Centroid", "dataType": "String"}, {"name": "street", "dataType": "String"}, {"name": "state_province_county", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "street_cleansed", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "address_line2_cleansed", "dataType": "String"}]', 
        '[{"name": "postal_cd", "dataType": "String"}, {"name": "mail_box_suite", "dataType": "String"}, {"name": "delete_flag", "dataType": "String"}, {"name": "load_date", "dataType": "Timestamp"}, {"name": "latitude", "dataType": "Double"}, {"name": "zip5", "dataType": "String"}, {"name": "address_line1_cleansed", "dataType": "String"}, {"name": "pmdm_guid", "dataType": "String"}, {"name": "street_continued", "dataType": "String"}, {"name": "address_id", "dataType": "Double"}, {"name": "start_dt", "dataType": "Timestamp"}, {"name": "zip4", "dataType": "String"}, {"name": "end_dt", "dataType": "Timestamp"}, {"name": "geoaccuracy", "dataType": "String"}, {"name": "geocodingsystem", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "Centroid", "dataType": "String"}, {"name": "street", "dataType": "String"}, {"name": "state_province_county", "dataType": "String"}, {"name": "street_cleansed", "dataType": "String"}, {"name": "country_name", "dataType": "String"}, {"name": "address_line2_cleansed", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_8 AS (

  SELECT * EXCEPT (`start_dt`, `end_dt`, `Centroid`, `SpatialObj`)
  
  FROM Union_201 AS in0

),

Formula_148_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((latitude IS NULL) AS BOOLEAN)
          THEN NULL
        WHEN (latitude < 0)
          THEN (CEIL((latitude * 1000)) / 1000)
        ELSE (FLOOR((latitude * 1000)) / 1000)
      END
    ) AS string) AS latitude1,
    CAST((
      CASE
        WHEN CAST((longitude IS NULL) AS BOOLEAN)
          THEN NULL
        WHEN (longitude < 0)
          THEN (CEIL((longitude * 1000)) / 1000)
        ELSE (FLOOR((longitude * 1000)) / 1000)
      END
    ) AS string) AS longitude1,
    *
  
  FROM AlteryxSelect_8 AS in0

),

Summarize_21 AS (

  SELECT 
    MAX((
      CASE
        WHEN (NOT(latitude IS NULL))
          THEN load_date
        ELSE NULL
      END
    )) AS Max_load_date,
    postal_cd AS postal_cd,
    mail_box_suite AS mail_box_suite,
    delete_flag AS delete_flag,
    latitude AS latitude,
    zip5 AS zip5,
    FIPS AS FIPS,
    address_line1_cleansed AS address_line1_cleansed,
    pmdm_guid AS pmdm_guid,
    street_continued AS street_continued,
    address_id AS address_id,
    State2 AS State2,
    zip4 AS zip4,
    geoaccuracy AS geoaccuracy,
    geocodingsystem AS geocodingsystem,
    longitude AS longitude,
    County AS County,
    city_town_village AS city_town_village,
    longitude1 AS longitude1,
    street AS street,
    state_province_county AS state_province_county,
    State AS State,
    street_cleansed AS street_cleansed,
    latitude1 AS latitude1,
    country_name AS country_name,
    address_line2_cleansed AS address_line2_cleansed
  
  FROM Formula_148_0 AS in0
  
  GROUP BY 
    postal_cd, 
    mail_box_suite, 
    delete_flag, 
    latitude, 
    zip5, 
    FIPS, 
    address_line1_cleansed, 
    pmdm_guid, 
    street_continued, 
    address_id, 
    State2, 
    zip4, 
    geoaccuracy, 
    geocodingsystem, 
    longitude, 
    County, 
    city_town_village, 
    longitude1, 
    street, 
    state_province_county, 
    State, 
    street_cleansed, 
    latitude1, 
    country_name, 
    address_line2_cleansed

),

Join_31_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.address_id = in1.address_id)
          THEN in1.address_id
        ELSE NULL
      END
    ) AS Right_address_id,
    in0.*,
    in1.* EXCEPT (`Max_load_date`, `delete_flag`, `pmdm_guid`, `address_id`)
  
  FROM aka_GPDIP_EDLUD_2 AS in0
  LEFT JOIN Summarize_21 AS in1
     ON (in0.address_id = in1.address_id)

),

Join_37_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`organization_id`, `Max_load_date`)
  
  FROM Join_31_left_UnionLeftOuter AS in0
  LEFT JOIN Summarize_236 AS in1
     ON (in0.organization_id = in1.organization_id)

),

Join_47_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.person_id = in1.person_id)
          THEN in1.person_id
        ELSE NULL
      END
    ) AS Right_person_id,
    in0.*,
    in1.* EXCEPT (`Max_load_date`, `person_id`)
  
  FROM Join_37_left_UnionLeftOuter AS in0
  LEFT JOIN Summarize_45 AS in1
     ON (in0.person_id = in1.person_id)

),

Formula_394_0 AS (

  SELECT 
    CAST(CURRENT_TIMESTAMP AS string) AS load_date,
    *
  
  FROM Join_47_left_UnionLeftOuter AS in0

),

MultiFieldFormula_983 AS (

  {#Applies multi-column edits to cleanse and standardize address-related fields for better consistency in downstream reporting.#}
  {{
    prophecy_basics.MultiColumnEdit(
      ['Formula_394_0'], 
      "regexp_replace(column_value, '\', '')", 
      [
        'load_date', 
        'Right_person_id', 
        'Right_address_id', 
        'study_id', 
        'person_id', 
        'primary_contact', 
        'address_id', 
        'contact_status', 
        'organization_id', 
        'contact_id', 
        'contact_role', 
        'study_site_number', 
        'postal_cd', 
        'mail_box_suite', 
        'latitude', 
        'zip5', 
        'FIPS', 
        'address_line1_cleansed', 
        'street_continued', 
        'State2', 
        'zip4', 
        'geoaccuracy', 
        'geocodingsystem', 
        'longitude', 
        'County', 
        'city_town_village', 
        'longitude1', 
        'street', 
        'state_province_county', 
        'State', 
        'street_cleansed', 
        'latitude1', 
        'country_name', 
        'address_line2_cleansed', 
        'sip_facility_id', 
        'ir_facility_id', 
        'org_dept', 
        'org_name', 
        'org_tax_id', 
        'person_name_prefix', 
        'sip_person_id', 
        'ir_person_id', 
        'person_gender', 
        'person_full_name', 
        'person_fst_name', 
        'pmdm_status_comment', 
        'pmdm_status', 
        'person_last_name', 
        'person_name_suffix', 
        'person_mid_name'
      ], 
      [
        'street_continued', 
        'state_province_county', 
        'address_line1_cleansed', 
        'address_line2_cleansed', 
        'street', 
        'street_cleansed', 
        'org_dept', 
        'org_name', 
        'person_fst_name', 
        'person_mid_name', 
        'person_last_name', 
        'person_full_name'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Cleanse_982 AS (

  {{
    prophecy_basics.DataCleansing(
      ['MultiFieldFormula_983'], 
      [
        { "name": "load_date", "dataType": "String" }, 
        { "name": "Right_person_id", "dataType": "Double" }, 
        { "name": "Right_address_id", "dataType": "Double" }, 
        { "name": "study_id", "dataType": "String" }, 
        { "name": "person_id", "dataType": "Double" }, 
        { "name": "primary_contact", "dataType": "String" }, 
        { "name": "address_id", "dataType": "Double" }, 
        { "name": "contact_status", "dataType": "String" }, 
        { "name": "organization_id", "dataType": "Double" }, 
        { "name": "contact_id", "dataType": "Decimal" }, 
        { "name": "contact_role", "dataType": "String" }, 
        { "name": "study_site_number", "dataType": "String" }, 
        { "name": "postal_cd", "dataType": "String" }, 
        { "name": "mail_box_suite", "dataType": "String" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "zip5", "dataType": "String" }, 
        { "name": "FIPS", "dataType": "String" }, 
        { "name": "address_line1_cleansed", "dataType": "String" }, 
        { "name": "street_continued", "dataType": "String" }, 
        { "name": "State2", "dataType": "String" }, 
        { "name": "zip4", "dataType": "String" }, 
        { "name": "geoaccuracy", "dataType": "String" }, 
        { "name": "geocodingsystem", "dataType": "String" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "County", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "longitude1", "dataType": "String" }, 
        { "name": "street", "dataType": "String" }, 
        { "name": "state_province_county", "dataType": "String" }, 
        { "name": "State", "dataType": "String" }, 
        { "name": "street_cleansed", "dataType": "String" }, 
        { "name": "latitude1", "dataType": "String" }, 
        { "name": "country_name", "dataType": "String" }, 
        { "name": "address_line2_cleansed", "dataType": "String" }, 
        { "name": "sip_facility_id", "dataType": "String" }, 
        { "name": "ir_facility_id", "dataType": "String" }, 
        { "name": "org_dept", "dataType": "String" }, 
        { "name": "org_name", "dataType": "String" }, 
        { "name": "org_tax_id", "dataType": "String" }, 
        { "name": "person_name_prefix", "dataType": "String" }, 
        { "name": "sip_person_id", "dataType": "String" }, 
        { "name": "ir_person_id", "dataType": "String" }, 
        { "name": "person_gender", "dataType": "String" }, 
        { "name": "person_full_name", "dataType": "String" }, 
        { "name": "person_fst_name", "dataType": "String" }, 
        { "name": "pmdm_status_comment", "dataType": "String" }, 
        { "name": "pmdm_status", "dataType": "String" }, 
        { "name": "person_last_name", "dataType": "String" }, 
        { "name": "person_name_suffix", "dataType": "String" }, 
        { "name": "person_mid_name", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['org_name'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
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

AlteryxSelect_388 AS (

  SELECT 
    contact_id AS contact_id,
    person_id AS person_id,
    organization_id AS organization_id,
    address_id AS address_id,
    contact_role AS contact_role,
    contact_status AS contact_status,
    primary_contact AS primary_contact,
    study_id AS study_id,
    study_site_number AS study_site_number,
    country_name AS country_name,
    mail_box_suite AS mail_box_suite,
    city_town_village AS city_town_village,
    postal_cd AS postal_cd,
    state_province_county AS state_province_county,
    address_line1_cleansed AS address_line1_cleansed,
    address_line2_cleansed AS address_line2_cleansed,
    geoaccuracy AS geoaccuracy,
    geocodingsystem AS geocodingsystem,
    street AS street,
    street_continued AS street_continued,
    street_cleansed AS street_cleansed,
    zip4 AS zip4,
    zip5 AS zip5,
    CAST(latitude1 AS DOUBLE) AS latitude1,
    CAST(longitude1 AS DOUBLE) AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    FIPS AS FIPS,
    County AS County,
    State2 AS State2,
    State AS State,
    org_name AS org_name,
    person_full_name AS person_full_name,
    load_date AS load_date
  
  FROM Cleanse_982 AS in0

),

MultiFieldFormula_399 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_388'], 
      "regexp_replace(column_value, '(?i)[^\\x20-\\x7E]', '')", 
      [
        'contact_id', 
        'person_id', 
        'organization_id', 
        'address_id', 
        'contact_role', 
        'contact_status', 
        'primary_contact', 
        'study_id', 
        'study_site_number', 
        'country_name', 
        'mail_box_suite', 
        'city_town_village', 
        'postal_cd', 
        'state_province_county', 
        'address_line1_cleansed', 
        'address_line2_cleansed', 
        'geoaccuracy', 
        'geocodingsystem', 
        'street', 
        'street_continued', 
        'street_cleansed', 
        'zip4', 
        'zip5', 
        'latitude1', 
        'longitude1', 
        'latitude', 
        'longitude', 
        'FIPS', 
        'County', 
        'State2', 
        'State', 
        'org_name', 
        'person_full_name', 
        'load_date'
      ], 
      [
        'contact_role', 
        'contact_status', 
        'primary_contact', 
        'study_id', 
        'study_site_number', 
        'country_name', 
        'mail_box_suite', 
        'city_town_village', 
        'postal_cd', 
        'state_province_county', 
        'address_line1_cleansed', 
        'address_line2_cleansed', 
        'geoaccuracy', 
        'geocodingsystem', 
        'street', 
        'street_continued', 
        'street_cleansed', 
        'zip4', 
        'zip5', 
        'FIPS', 
        'County', 
        'State2', 
        'State', 
        'person_full_name'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

)

SELECT *

FROM MultiFieldFormula_399
