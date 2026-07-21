{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_239 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Summarize_239')}}

),

Filter_221 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Filter_221')}}

),

Filter_240 AS (

  SELECT * 
  
  FROM Summarize_239 AS in0
  
  WHERE ((Count_lat = 1) AND (Count_long = 1))

),

Join_247_inner AS (

  SELECT 
    in1.latitude1 AS Right_latitude1,
    in1.longitude1 AS Right_longitude1,
    in0.*,
    in1.* EXCEPT (`latitude1`, `longitude1`)
  
  FROM Filter_221 AS in0
  INNER JOIN Filter_240 AS in1
     ON ((in0.latitude1 = in1.latitude1) AND (in0.longitude1 = in1.longitude1))

),

Formula_1006_0 AS (

  SELECT 
    CAST(city_town_village AS string) AS city_town_village_old,
    *
  
  FROM Join_247_inner AS in0

),

Union_897 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Union_897')}}

),

site_studysite__412 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_updated_macro_paths', 'site_studysite__412') }}

),

AlteryxSelect_413 AS (

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
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    CAST(latitude AS DECIMAL (19, 6)) AS latitude,
    CAST(longitude AS DECIMAL (19, 6)) AS longitude,
    fips AS fips,
    county AS county,
    state2 AS state2,
    state AS state,
    org_name AS org_name,
    person_full_name AS person_full_name,
    load_date AS load_date
  
  FROM site_studysite__412 AS in0

),

Summarize_1020 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Summarize_1020')}}

),

Filter_1022_reject AS (

  SELECT * 
  
  FROM Summarize_1020 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Unique_1025 AS (

  SELECT * 
  
  FROM Filter_1022_reject AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, CountDistinctNonNull_new_org_name_alteryx ORDER BY GroupID, CountDistinctNonNull_new_org_name_alteryx) = 1

),

Join_1023_inner AS (

  SELECT 
    in0.* EXCEPT (`GroupID`, `CountDistinctNonNull_new_org_name_alteryx`),
    in1.* EXCEPT (`CountDistinctNonNull_new_org_name_alteryx`)
  
  FROM Unique_1025 AS in0
  INNER JOIN Union_897 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Union_1024_reformat_1 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    lat AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    lot AS lot,
    new_org_name_alteryx AS new_org_name_alteryx,
    organization_name AS organization_name
  
  FROM Join_1023_inner AS in0

),

table_1070_Output17_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_1070_Output17_macro_op') }}

),

AlteryxSelect_1077 AS (

  SELECT 
    new_org_name_alteryx AS new_org_name_alteryx,
    org_name_keyword AS org_name_keyword,
    * EXCEPT (`new_org_name_alteryx`, `org_name_keyword`)
  
  FROM table_1070_Output17_macro_op AS in0

),

Union_1024_reformat_0 AS (

  SELECT 
    CAST(CountDistinctNonNull_new_org_name_alteryx AS INTEGER) AS CountDistinctNonNull_new_org_name_alteryx,
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    lat AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    lot AS lot,
    new_org_name_alteryx AS new_org_name_alteryx,
    CAST(org_name_keyword AS string) AS org_name_keyword,
    organization_name AS organization_name
  
  FROM AlteryxSelect_1077 AS in0

),

Union_1024 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_1024_reformat_1', 'Union_1024_reformat_0'], 
      [
        '[{"name": "latitude", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Decimal"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]', 
        '[{"name": "latitude", "dataType": "Decimal"}, {"name": "GroupID", "dataType": "Integer"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Decimal"}, {"name": "org_name_keyword", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer"}, {"name": "lot", "dataType": "Decimal"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Decimal"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Cleanse_933 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_1024'], 
      [
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "GroupID", "dataType": "String" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "org_name_keyword", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer" }, 
        { "name": "lot", "dataType": "Decimal" }, 
        { "name": "new_org_name_alteryx", "dataType": "String" }, 
        { "name": "city_town_village_old", "dataType": "String" }, 
        { "name": "lat", "dataType": "Decimal" }
      ], 
      'keepOriginal', 
      ['new_org_name_alteryx'], 
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

Summarize_934 AS (

  SELECT 
    COUNT(DISTINCT city_town_village) AS CountDistinctNonNull_city_town_village,
    new_org_name_alteryx AS new_org_name_alteryx
  
  FROM Cleanse_933 AS in0
  
  GROUP BY new_org_name_alteryx

),

Filter_936 AS (

  SELECT * 
  
  FROM Summarize_934 AS in0
  
  WHERE (CountDistinctNonNull_city_town_village > 1)

),

Join_935_right_UnionRightOuter AS (

  SELECT 
    in1.city_town_village AS city_town_village,
    in1.lat AS lat,
    in1.lot AS lot,
    in1.latitude AS latitude,
    in1.longitude AS longitude,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.organization_name AS organization_name,
    in0.CountDistinctNonNull_city_town_village AS CountDistinctNonNull_city_town_village,
    in1.CountDistinctNonNull_new_org_name_alteryx AS CountDistinctNonNull_new_org_name_alteryx,
    in1.GroupID AS GroupID,
    in1.city_town_village_old AS city_town_village_old,
    in1.org_name_keyword AS org_name_keyword,
    in0.* EXCEPT (`new_org_name_alteryx`, `CountDistinctNonNull_city_town_village`),
    in1.* EXCEPT (`city_town_village`, 
    `lat`, 
    `lot`, 
    `latitude`, 
    `longitude`, 
    `new_org_name_alteryx`, 
    `organization_name`, 
    `CountDistinctNonNull_new_org_name_alteryx`, 
    `GroupID`, 
    `city_town_village_old`, 
    `org_name_keyword`)
  
  FROM Filter_936 AS in0
  RIGHT JOIN Cleanse_933 AS in1
     ON (in0.new_org_name_alteryx = in1.new_org_name_alteryx)

),

Formula_938_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          ((CountDistinctNonNull_city_town_village > 1) AND ((LENGTH(new_org_name_alteryx)) > 0))
          AND ((LENGTH(city_town_village)) > 0)
        )
          THEN CAST((CONCAT(new_org_name_alteryx, ' - ', city_town_village)) AS string)
        WHEN ((CountDistinctNonNull_city_town_village > 1) AND ((LENGTH(new_org_name_alteryx)) = 0))
          THEN CAST(NULL AS string)
        ELSE new_org_name_alteryx
      END
    ) AS string) AS new_org_name_alteryx,
    * EXCEPT (`new_org_name_alteryx`)
  
  FROM Join_935_right_UnionRightOuter AS in0

),

AlteryxSelect_939 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS lat,
    lot AS lot,
    latitude AS latitude,
    longitude AS longitude,
    new_org_name_alteryx AS new_org_name_alteryx,
    organization_name AS organization_name,
    GroupID AS GroupID,
    city_town_village_old AS city_town_village_old,
    org_name_keyword AS org_name_keyword
  
  FROM Formula_938_0 AS in0

),

AlteryxSelect_778 AS (

  SELECT 
    lat AS lat,
    lot AS lot
  
  FROM AlteryxSelect_939 AS in0

),

Unique_783 AS (

  SELECT * 
  
  FROM AlteryxSelect_778 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot ORDER BY lat, lot) = 1

),

RecordID_776 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_783'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_779_inner AS (

  SELECT 
    in0.RecordID AS GroupID_Geo,
    in1.GroupID AS GroupID_Distance,
    in0.* EXCEPT (`RecordID`, `lat`, `lot`),
    in1.* EXCEPT (`GroupID`)
  
  FROM RecordID_776 AS in0
  INNER JOIN AlteryxSelect_939 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

AlteryxSelect_407 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS latitude1,
    lot AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    organization_name AS org_name,
    new_org_name_alteryx AS new_org_name,
    GroupID_Distance AS GroupID_Distance,
    GroupID_Geo AS GroupID_Geo,
    city_town_village_old AS city_town_village_old,
    org_name_keyword AS org_name_keyword
  
  FROM Join_779_inner AS in0

),

Union_405 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_407', 'Formula_1006_0'], 
      [
        '[{"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name_keyword", "dataType": "String"}, {"name": "GroupID_Distance", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "org_name", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "longitude1", "dataType": "Decimal"}, {"name": "GroupID_Geo", "dataType": "Integer"}, {"name": "latitude1", "dataType": "Decimal"}, {"name": "new_org_name", "dataType": "String"}]', 
        '[{"name": "latitude", "dataType": "Double"}, {"name": "Right_latitude1", "dataType": "Decimal"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "Right_longitude1", "dataType": "Decimal"}, {"name": "org_name", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "longitude1", "dataType": "Decimal"}, {"name": "contact_id", "dataType": "String"}, {"name": "latitude1", "dataType": "Decimal"}, {"name": "Count_lat", "dataType": "Double"}, {"name": "Count_long", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_419 AS (

  SELECT * 
  
  FROM Union_405 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village, 
  latitude1, 
  longitude1, 
  latitude, 
  longitude, 
  org_name, 
  city_town_village_old, 
  new_org_name, 
  GroupID_Distance, 
  GroupID_Geo ORDER BY city_town_village, latitude1, longitude1, latitude, longitude, org_name, city_town_village_old, new_org_name, GroupID_Distance, GroupID_Geo) = 1

),

Join_414_left_UnionLeftOuter AS (

  SELECT 
    in0.state2 AS state2,
    in0.postal_cd AS postal_cd,
    in0.mail_box_suite AS mail_box_suite,
    in0.load_date AS load_date,
    in0.latitude AS latitude,
    in0.zip5 AS zip5,
    in0.state AS state,
    in0.address_line1_cleansed AS address_line1_cleansed,
    in0.study_id AS study_id,
    in0.street_continued AS street_continued,
    in0.person_id AS person_id,
    in0.primary_contact AS primary_contact,
    in1.GroupID_Distance AS GroupID_Distance,
    in0.address_id AS address_id,
    in0.fips AS fips,
    in0.zip4 AS zip4,
    in0.geoaccuracy AS geoaccuracy,
    in0.geocodingsystem AS geocodingsystem,
    in0.longitude AS longitude,
    in1.org_name_keyword AS org_name_keyword,
    in0.county AS county,
    in0.person_full_name AS person_full_name,
    in0.city_town_village AS city_town_village,
    in0.org_name AS org_name,
    in0.contact_status AS contact_status,
    in0.organization_id AS organization_id,
    in0.longitude1 AS longitude1,
    in0.street AS street,
    in0.state_province_county AS state_province_county,
    in0.contact_id AS contact_id,
    in0.street_cleansed AS street_cleansed,
    in0.latitude1 AS latitude1,
    in0.country_name AS country_name,
    in0.contact_role AS contact_role,
    in1.new_org_name AS new_org_name,
    in0.study_site_number AS study_site_number,
    in0.address_line2_cleansed AS address_line2_cleansed,
    in1.GroupID_Geo AS GroupID_Geo,
    (
      CASE
        WHEN (
          (
            ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))
            AND (in0.org_name = in1.org_name)
          )
          AND (in0.city_town_village = in1.city_town_village_old)
        )
          THEN in1.city_town_village
        ELSE NULL
      END
    ) AS city_town_village_cleansed
  
  FROM AlteryxSelect_413 AS in0
  LEFT JOIN Unique_419 AS in1
     ON (
      (
        ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))
        AND (in0.org_name = in1.org_name)
      )
      AND (in0.city_town_village = in1.city_town_village_old)
    )

),

Formula_522_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (NOT(new_org_name IS NULL))
          THEN new_org_name
        ELSE org_name
      END
    ) AS string) AS org_name_adj,
    *
  
  FROM Join_414_left_UnionLeftOuter AS in0

),

Unique_424 AS (

  SELECT * 
  
  FROM Formula_522_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id, 
  person_id, 
  organization_id, 
  address_id, 
  contact_role, 
  contact_status, 
  primary_contact, 
  study_id, 
  study_site_number, 
  country_name, 
  mail_box_suite, 
  postal_cd, 
  state_province_county, 
  address_line1_cleansed, 
  address_line2_cleansed, 
  geoaccuracy, 
  geocodingsystem, 
  street, 
  street_continued, 
  street_cleansed, 
  zip4, 
  zip5, 
  fips, 
  county, 
  state2, 
  state, 
  person_full_name, 
  city_town_village, 
  city_town_village_cleansed, 
  latitude1, 
  longitude1, 
  latitude, 
  longitude, 
  org_name, 
  load_date, 
  GroupID_Distance, 
  GroupID_Geo, 
  new_org_name, 
  org_name_keyword, 
  org_name_adj ORDER BY contact_id, person_id, organization_id, address_id, contact_role, contact_status, primary_contact, study_id, study_site_number, country_name, mail_box_suite, postal_cd, state_province_county, address_line1_cleansed, address_line2_cleansed, geoaccuracy, geocodingsystem, street, street_continued, street_cleansed, zip4, zip5, fips, county, state2, state, person_full_name, city_town_village, city_town_village_cleansed, latitude1, longitude1, latitude, longitude, org_name, load_date, GroupID_Distance, GroupID_Geo, new_org_name, org_name_keyword, org_name_adj) = 1

),

AlteryxSelect_525 AS (

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
    fips AS fips,
    county AS county,
    state2 AS state2,
    state AS state,
    person_full_name AS person_full_name,
    city_town_village AS city_town_village,
    city_town_village_cleansed AS city_town_village_cleansed,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name,
    org_name_adj AS org_name_adj,
    org_name_keyword AS org_name_keyword,
    CAST(GroupID_Distance AS string) AS GroupID_Distance,
    CAST(GroupID_Geo AS string) AS GroupID_Geo,
    load_date AS load_date
  
  FROM Unique_424 AS in0

),

Formula_793_to_Formula_1078_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((GroupID_Distance IS NULL) AS BOOLEAN)
          THEN NULL
        ELSE (
          CONCAT(
            'Distance-', 
            (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID_Distance AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
        )
      END
    ) AS string) AS GroupID_Distance,
    CAST((
      CASE
        WHEN CAST((GroupID_Geo IS NULL) AS BOOLEAN)
          THEN NULL
        ELSE (
          CONCAT(
            'Geo-', 
            (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID_Geo AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
        )
      END
    ) AS string) AS GroupID_Geo,
    CAST(NULL AS string) AS variableNull,
    * EXCEPT (`groupid_distance`, `groupid_geo`)
  
  FROM AlteryxSelect_525 AS in0

)

SELECT *

FROM Formula_793_to_Formula_1078_0
