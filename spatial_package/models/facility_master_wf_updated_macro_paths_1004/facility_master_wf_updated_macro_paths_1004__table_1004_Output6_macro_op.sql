{{
  config({    
    "materialized": "incremental",
    "alias": "table_1004_Output6_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH MakeGroup_40_1004 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_updated_macro_paths_1004', 'MakeGroup_40_1004') }}

),

FindReplace_39_1004_allRules AS (

  SELECT collect_list(struct(variableKey AS variableKey, variableGroup AS variableGroup)) AS _rules
  
  FROM MakeGroup_40_1004 AS in0

),

Filter_26_1004 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths_1004__Filter_26_1004')}}

),

RecordID_38_1004 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_26_1004'], 
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

FindReplace_39_1004_join AS (

  SELECT 
    in0.latitude AS latitude,
    in0.organization_name AS organization_name,
    in0.longitude AS longitude,
    in0.city_town_village AS city_town_village,
    in0.RecordID AS RecordID,
    in1._rules AS _rules,
    in0.lot AS lot,
    in0.city_town_village_old AS city_town_village_old,
    in0.lat AS lat,
    in0.GroupID AS GroupID
  
  FROM RecordID_38_1004 AS in0
  FULL JOIN FindReplace_39_1004_allRules AS in1
     ON TRUE

),

FindReplace_39_1004_0 AS (

  SELECT 
    aggregate(
      _rules, 
      `organization_name`, 
      (acc, rule) -> regexp_replace(acc, concat('^', rule['variableKey'], '$'), rule['variableGroup'])) AS organization_name,
    * EXCEPT (`organization_name`)
  
  FROM FindReplace_39_1004_join AS in0

),

FindReplace_39_1004_reorg_0 AS (

  SELECT * EXCEPT (`_rules`)
  
  FROM FindReplace_39_1004_0 AS in0

),

Join_37_1004_inner AS (

  SELECT 
    in1.organization_name AS new_organization_name_fuzzy_unique,
    in0.latitude AS latitude,
    in0.organization_name AS organization_name,
    in0.longitude AS longitude,
    in0.city_town_village AS city_town_village,
    in0.lot AS lot,
    in0.city_town_village_old AS city_town_village_old,
    in0.lat AS lat,
    in0.GroupID AS GroupID
  
  FROM RecordID_38_1004 AS in0
  INNER JOIN FindReplace_39_1004_reorg_0 AS in1
     ON (in0.RecordID = in1.RecordID)

)

SELECT *

FROM Join_37_1004_inner
