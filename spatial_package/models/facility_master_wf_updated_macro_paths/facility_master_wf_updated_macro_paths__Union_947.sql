{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_946_out1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_updated_macro_paths', 'Join_946', 'out1') }}

),

Join_946_out0 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_updated_macro_paths', 'Join_946', 'out0') }}

),

AlteryxSelect_952 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS lat,
    lot AS lot,
    latitude AS latitude,
    longitude AS longitude,
    organization_name AS organization_name,
    GroupID AS GroupID,
    new AS new_org_name_alteryx
  
  FROM Join_946_out0 AS in0

),

AlteryxSelect_953 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS lat,
    lot AS lot,
    latitude AS latitude,
    longitude AS longitude,
    new_org_name_alteryx AS new_org_name_alteryx,
    organization_name AS organization_name,
    GroupID AS GroupID
  
  FROM Join_946_out1 AS in0

),

Union_947 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_952', 'AlteryxSelect_953'], 
      [
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}, {"name": "latitude", "dataType": "String"}, {"name": "longitude", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}]', 
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}, {"name": "latitude", "dataType": "String"}, {"name": "longitude", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_947
