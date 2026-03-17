{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_160",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH aka_GPDIP_EDLUD_151 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4_CRIS_Period_Comparison', 'aka_GPDIP_EDLUD_151') }}

),

FindReplace_231_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_151 AS in0

),

aka_GPDIP_EDLUD_150 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4_CRIS_Period_Comparison', 'aka_GPDIP_EDLUD_150') }}

),

Filter_157_reject AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_150 AS in0
  
  WHERE ((NOT(NOT(empl_id IS NULL))) OR ((NOT(empl_id IS NULL)) IS NULL))

),

Transpose_217_schemaTransform_0 AS (

  SELECT * EXCEPT (`empl_id`, `loaded_date`)
  
  FROM Filter_157_reject AS in0

),

Transpose_217 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_217_schemaTransform_0'], 
      ['period', 'position_number'], 
      [
        'user_group', 
        'domain', 
        'ntid', 
        'user_name', 
        'std_hours', 
        'division_id', 
        'division', 
        'zone_id', 
        'zone', 
        'line_id', 
        'line', 
        'function_id', 
        'variablefunction', 
        'org_id', 
        'organization', 
        'cost_center', 
        'cost_center_desc', 
        'user_type', 
        'at_access', 
        'empl_status', 
        'physical_site', 
        'country_code', 
        'supervisor_name', 
        'vendor_id', 
        'vendor_name', 
        'funding_id', 
        'additional_information', 
        'modified_by', 
        'modified_date', 
        'targeted_hire_date', 
        'management_level', 
        'position_name', 
        'budgeted_id', 
        'vol_invol', 
        'hr_std_hours', 
        'compensation_level', 
        'hire_date', 
        'subgroup_id', 
        'backfill_new_position', 
        'days_req_recruiting_date', 
        'position_status', 
        'flex_field_2', 
        'requisition_status', 
        'supervisor_id', 
        'flex_field_4', 
        'job_title', 
        'approved_to_hire_id', 
        'position_worker_type', 
        'flex_field_3', 
        'position_fill_date', 
        'category', 
        'replacement_for', 
        'recruiting_start_date', 
        'hiring_manager', 
        'contractor_hourly_rates', 
        'contractor_annual_rates', 
        'at_manual_profile', 
        'manual_std_hours', 
        'job_level', 
        'fte', 
        'purchase_order', 
        'hr_cost_center', 
        'at_cost_center', 
        'flex_field_1', 
        'requisition_id', 
        'action_to_take'
      ], 
      'Name', 
      'Value', 
      [
        'organization', 
        'empl_status', 
        'user_group', 
        'targeted_hire_date', 
        'cost_center_desc', 
        'hire_date', 
        'position_fill_date', 
        'fte', 
        'physical_site', 
        'position_worker_type', 
        'hiring_manager', 
        'requisition_id', 
        'at_access', 
        'vendor_id', 
        'requisition_status', 
        'line', 
        'cost_center', 
        'job_title', 
        'vendor_name', 
        'supervisor_name', 
        'subgroup_id', 
        'hr_cost_center', 
        'domain', 
        'flex_field_1', 
        'supervisor_id', 
        'backfill_new_position', 
        'budgeted_id', 
        'additional_information', 
        'approved_to_hire_id', 
        'hr_std_hours', 
        'variablefunction', 
        'recruiting_start_date', 
        'days_req_recruiting_date', 
        'ntid', 
        'division', 
        'flex_field_4', 
        'country_code', 
        'flex_field_2', 
        'at_cost_center', 
        'function_id', 
        'job_level', 
        'management_level', 
        'std_hours', 
        'contractor_hourly_rates', 
        'modified_by', 
        'division_id', 
        'action_to_take', 
        'position_status', 
        'user_type', 
        'funding_id', 
        'zone_id', 
        'contractor_annual_rates', 
        'purchase_order', 
        'position_name', 
        'category', 
        'flex_field_3', 
        'manual_std_hours', 
        'compensation_level', 
        'vol_invol', 
        'line_id', 
        'zone', 
        'modified_date', 
        'user_name', 
        'at_manual_profile', 
        'org_id', 
        'period', 
        'replacement_for', 
        'position_number'
      ], 
      true
    )
  }}

),

Filter_158_reject AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_151 AS in0
  
  WHERE ((NOT(NOT(empl_id IS NULL))) OR ((NOT(empl_id IS NULL)) IS NULL))

),

Transpose_218_schemaTransform_0 AS (

  SELECT * EXCEPT (`empl_id`, `loaded_date`)
  
  FROM Filter_158_reject AS in0

),

Transpose_218 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_218_schemaTransform_0'], 
      ['period', 'position_number'], 
      [
        'user_group', 
        'domain', 
        'ntid', 
        'user_name', 
        'std_hours', 
        'division_id', 
        'division', 
        'zone_id', 
        'zone', 
        'line_id', 
        'line', 
        'function_id', 
        'variablefunction', 
        'org_id', 
        'organization', 
        'cost_center', 
        'cost_center_desc', 
        'user_type', 
        'at_access', 
        'empl_status', 
        'physical_site', 
        'country_code', 
        'supervisor_name', 
        'vendor_id', 
        'vendor_name', 
        'funding_id', 
        'additional_information', 
        'modified_by', 
        'modified_date', 
        'targeted_hire_date', 
        'management_level', 
        'budgeted_id', 
        'hr_std_hours', 
        'compensation_level', 
        'subgroup_id', 
        'backfill_new_position', 
        'days_req_recruiting_date', 
        'position_status', 
        'flex_field_2', 
        'requisition_status', 
        'flex_field_4', 
        'job_title', 
        'approved_to_hire_id', 
        'flex_field_3', 
        'replacement_for', 
        'recruiting_start_date', 
        'hiring_manager', 
        'contractor_hourly_rates', 
        'contractor_annual_rates', 
        'at_manual_profile', 
        'manual_std_hours', 
        'job_level', 
        'fte', 
        'purchase_order', 
        'hr_cost_center', 
        'at_cost_center', 
        'flex_field_1', 
        'requisition_id', 
        'action_to_take'
      ], 
      'Name', 
      'Value', 
      [
        'organization', 
        'empl_status', 
        'user_group', 
        'targeted_hire_date', 
        'cost_center_desc', 
        'fte', 
        'physical_site', 
        'hiring_manager', 
        'requisition_id', 
        'at_access', 
        'vendor_id', 
        'requisition_status', 
        'line', 
        'cost_center', 
        'job_title', 
        'vendor_name', 
        'supervisor_name', 
        'subgroup_id', 
        'hr_cost_center', 
        'domain', 
        'flex_field_1', 
        'backfill_new_position', 
        'budgeted_id', 
        'additional_information', 
        'approved_to_hire_id', 
        'hr_std_hours', 
        'variablefunction', 
        'recruiting_start_date', 
        'days_req_recruiting_date', 
        'ntid', 
        'division', 
        'flex_field_4', 
        'country_code', 
        'flex_field_2', 
        'at_cost_center', 
        'function_id', 
        'job_level', 
        'management_level', 
        'std_hours', 
        'contractor_hourly_rates', 
        'modified_by', 
        'division_id', 
        'action_to_take', 
        'position_status', 
        'user_type', 
        'funding_id', 
        'zone_id', 
        'contractor_annual_rates', 
        'purchase_order', 
        'flex_field_3', 
        'manual_std_hours', 
        'compensation_level', 
        'line_id', 
        'zone', 
        'modified_date', 
        'user_name', 
        'at_manual_profile', 
        'org_id', 
        'period', 
        'replacement_for', 
        'position_number'
      ], 
      true
    )
  }}

),

Join_219_right AS (

  SELECT in0.*
  
  FROM Transpose_218 AS in0
  ANTI JOIN Transpose_217 AS in1
     ON ((in1.position_number = in0.position_number) AND (in1.Name = in0.Name))

),

Filter_232 AS (

  SELECT * 
  
  FROM Join_219_right AS in0
  
  WHERE (Name = 'user_name')

),

Formula_230_0 AS (

  SELECT 
    CAST('position_number' AS string) AS primary_key,
    *
  
  FROM Filter_232 AS in0

),

FindReplace_231_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period,
    in0.position_number AS position_number
  
  FROM Formula_230_0 AS in0
  FULL JOIN FindReplace_231_allRules AS in1
     ON TRUE

),

FindReplace_231_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`position_number`, rule['position_number'], 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_231_join AS in0

),

FindReplace_231_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`, `position_number`)
  
  FROM FindReplace_231_0 AS in0

),

Join_219_inner AS (

  SELECT 
    in0.period AS current_period,
    in1.period AS previous_period,
    in0.* EXCEPT (`period`, `Name`, `Value`, `position_number`),
    in1.* EXCEPT (`period`)
  
  FROM Transpose_217 AS in0
  INNER JOIN Transpose_218 AS in1
     ON ((in0.position_number = in1.position_number) AND (in0.Name = in1.Name))

),

AlteryxSelect_233 AS (

  SELECT current_period AS current_period
  
  FROM Join_219_inner AS in0

),

Unique_234 AS (

  SELECT * 
  
  FROM AlteryxSelect_233 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY current_period ORDER BY current_period) = 1

),

AppendFields_229 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Unique_234 AS in0
  INNER JOIN FindReplace_231_reorg_0 AS in1
     ON TRUE

),

Formula_237_0 AS (

  SELECT 
    CAST((CONCAT(user_name, ' (', position_number, ')')) AS string) AS user_name,
    CAST('Open Position Delete' AS string) AS Comments,
    CAST('Delete' AS string) AS transaction_type,
    * EXCEPT (`user_name`)
  
  FROM AppendFields_229 AS in0

),

FindReplace_163_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

Filter_158 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_151 AS in0
  
  WHERE (NOT(empl_id IS NULL))

),

Transpose_153_schemaTransform_0 AS (

  SELECT * EXCEPT (`loaded_date`)
  
  FROM Filter_158 AS in0

),

Transpose_153 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_153_schemaTransform_0'], 
      ['period', 'empl_id'], 
      [
        'user_group', 
        'domain', 
        'ntid', 
        'user_name', 
        'std_hours', 
        'division_id', 
        'division', 
        'zone_id', 
        'zone', 
        'line_id', 
        'line', 
        'function_id', 
        'variablefunction', 
        'org_id', 
        'organization', 
        'cost_center', 
        'cost_center_desc', 
        'user_type', 
        'at_access', 
        'empl_status', 
        'physical_site', 
        'country_code', 
        'supervisor_name', 
        'vendor_id', 
        'vendor_name', 
        'position_number', 
        'funding_id', 
        'additional_information', 
        'modified_by', 
        'modified_date', 
        'targeted_hire_date', 
        'management_level', 
        'budgeted_id', 
        'hr_std_hours', 
        'compensation_level', 
        'subgroup_id', 
        'backfill_new_position', 
        'days_req_recruiting_date', 
        'position_status', 
        'flex_field_2', 
        'requisition_status', 
        'flex_field_4', 
        'job_title', 
        'approved_to_hire_id', 
        'flex_field_3', 
        'replacement_for', 
        'recruiting_start_date', 
        'hiring_manager', 
        'contractor_hourly_rates', 
        'contractor_annual_rates', 
        'at_manual_profile', 
        'manual_std_hours', 
        'job_level', 
        'fte', 
        'purchase_order', 
        'hr_cost_center', 
        'at_cost_center', 
        'flex_field_1', 
        'requisition_id', 
        'action_to_take'
      ], 
      'Name', 
      'Value', 
      [
        'organization', 
        'empl_status', 
        'user_group', 
        'targeted_hire_date', 
        'cost_center_desc', 
        'fte', 
        'physical_site', 
        'hiring_manager', 
        'requisition_id', 
        'at_access', 
        'vendor_id', 
        'requisition_status', 
        'line', 
        'cost_center', 
        'job_title', 
        'vendor_name', 
        'supervisor_name', 
        'subgroup_id', 
        'hr_cost_center', 
        'domain', 
        'flex_field_1', 
        'backfill_new_position', 
        'budgeted_id', 
        'additional_information', 
        'approved_to_hire_id', 
        'hr_std_hours', 
        'variablefunction', 
        'recruiting_start_date', 
        'days_req_recruiting_date', 
        'ntid', 
        'division', 
        'flex_field_4', 
        'country_code', 
        'flex_field_2', 
        'at_cost_center', 
        'function_id', 
        'job_level', 
        'management_level', 
        'std_hours', 
        'contractor_hourly_rates', 
        'modified_by', 
        'division_id', 
        'action_to_take', 
        'position_status', 
        'user_type', 
        'funding_id', 
        'zone_id', 
        'contractor_annual_rates', 
        'purchase_order', 
        'flex_field_3', 
        'manual_std_hours', 
        'compensation_level', 
        'empl_id', 
        'line_id', 
        'zone', 
        'modified_date', 
        'user_name', 
        'at_manual_profile', 
        'org_id', 
        'period', 
        'replacement_for', 
        'position_number'
      ], 
      true
    )
  }}

),

Filter_157 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_150 AS in0
  
  WHERE (NOT(empl_id IS NULL))

),

Transpose_152_schemaTransform_0 AS (

  SELECT * EXCEPT (`loaded_date`)
  
  FROM Filter_157 AS in0

),

Transpose_152 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_152_schemaTransform_0'], 
      ['period', 'empl_id'], 
      [
        'user_group', 
        'domain', 
        'ntid', 
        'user_name', 
        'std_hours', 
        'division_id', 
        'division', 
        'zone_id', 
        'zone', 
        'line_id', 
        'line', 
        'function_id', 
        'variablefunction', 
        'org_id', 
        'organization', 
        'cost_center', 
        'cost_center_desc', 
        'user_type', 
        'at_access', 
        'empl_status', 
        'physical_site', 
        'country_code', 
        'supervisor_name', 
        'vendor_id', 
        'vendor_name', 
        'position_number', 
        'funding_id', 
        'subgroup_id', 
        'additional_information', 
        'job_level', 
        'purchase_order', 
        'contractor_hourly_rates', 
        'contractor_annual_rates', 
        'flex_field_1', 
        'flex_field_2', 
        'flex_field_3', 
        'flex_field_4', 
        'action_to_take', 
        'modified_by', 
        'modified_date', 
        'fte', 
        'at_cost_center', 
        'at_manual_profile', 
        'hr_cost_center', 
        'targeted_hire_date', 
        'management_level', 
        'position_name', 
        'budgeted_id', 
        'vol_invol', 
        'hr_std_hours', 
        'compensation_level', 
        'hire_date', 
        'backfill_new_position', 
        'days_req_recruiting_date', 
        'position_status', 
        'requisition_status', 
        'supervisor_id', 
        'job_title', 
        'approved_to_hire_id', 
        'position_worker_type', 
        'position_fill_date', 
        'category', 
        'replacement_for', 
        'recruiting_start_date', 
        'hiring_manager', 
        'manual_std_hours', 
        'requisition_id'
      ], 
      'Name', 
      'Value', 
      [
        'organization', 
        'empl_status', 
        'user_group', 
        'targeted_hire_date', 
        'cost_center_desc', 
        'hire_date', 
        'position_fill_date', 
        'fte', 
        'physical_site', 
        'position_worker_type', 
        'hiring_manager', 
        'requisition_id', 
        'at_access', 
        'vendor_id', 
        'requisition_status', 
        'line', 
        'cost_center', 
        'job_title', 
        'vendor_name', 
        'supervisor_name', 
        'subgroup_id', 
        'hr_cost_center', 
        'domain', 
        'flex_field_1', 
        'supervisor_id', 
        'backfill_new_position', 
        'budgeted_id', 
        'additional_information', 
        'approved_to_hire_id', 
        'hr_std_hours', 
        'variablefunction', 
        'recruiting_start_date', 
        'days_req_recruiting_date', 
        'ntid', 
        'division', 
        'flex_field_4', 
        'country_code', 
        'flex_field_2', 
        'at_cost_center', 
        'function_id', 
        'job_level', 
        'management_level', 
        'std_hours', 
        'contractor_hourly_rates', 
        'modified_by', 
        'division_id', 
        'action_to_take', 
        'position_status', 
        'user_type', 
        'funding_id', 
        'zone_id', 
        'contractor_annual_rates', 
        'purchase_order', 
        'position_name', 
        'category', 
        'flex_field_3', 
        'manual_std_hours', 
        'compensation_level', 
        'empl_id', 
        'vol_invol', 
        'line_id', 
        'zone', 
        'modified_date', 
        'user_name', 
        'at_manual_profile', 
        'org_id', 
        'period', 
        'replacement_for', 
        'position_number'
      ], 
      true
    )
  }}

),

Join_154_left AS (

  SELECT in0.*
  
  FROM Transpose_152 AS in0
  ANTI JOIN Transpose_153 AS in1
     ON ((in0.empl_id = in1.empl_id) AND (in0.Name = in1.Name))

),

Filter_193 AS (

  SELECT * 
  
  FROM Join_154_left AS in0
  
  WHERE (Name = 'user_name')

),

Formula_168_0 AS (

  SELECT 
    CAST('empl_id' AS string) AS primary_key,
    CAST('User Onboarded' AS string) AS Comments,
    *
  
  FROM Filter_193 AS in0

),

FindReplace_163_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.empl_id AS empl_id,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period,
    in0.Comments AS Comments
  
  FROM Formula_168_0 AS in0
  FULL JOIN FindReplace_163_allRules AS in1
     ON TRUE

),

FindReplace_163_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`empl_id`, concat('^', rule['empl_id'], '$'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_163_join AS in0

),

FindReplace_163_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_163_0 AS in0

),

AlteryxSelect_169 AS (

  SELECT 
    period AS current_period,
    empl_id AS key_value,
    * EXCEPT (`period`, `empl_id`)
  
  FROM FindReplace_163_reorg_0 AS in0

),

Join_154_inner AS (

  SELECT 
    in0.period AS current_period,
    in1.period AS previous_period,
    in0.* EXCEPT (`period`, `Name`, `Value`, `empl_id`),
    in1.* EXCEPT (`period`)
  
  FROM Transpose_152 AS in0
  INNER JOIN Transpose_153 AS in1
     ON ((in0.empl_id = in1.empl_id) AND (in0.Name = in1.Name))

),

Formula_167_0 AS (

  SELECT 
    CAST('empl_id' AS string) AS primary_key,
    CAST('User Update' AS string) AS Comments,
    *
  
  FROM Join_154_inner AS in0

),

Filter_155 AS (

  SELECT * 
  
  FROM Formula_167_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  Value = Right_Value)
              ) OR isnull(Value)
            ) OR isnull(Right_Value)
          )
          AND NOT ((isnull(Value) AND isnull(Right_Value)))
        )

),

FindReplace_156_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

FindReplace_156_join AS (

  SELECT 
    in0.previous_period AS previous_period,
    in0.Name AS Name,
    in0.current_period AS current_period,
    in1._rules AS _rules,
    in0.empl_id AS empl_id,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.Comments AS Comments
  
  FROM Filter_155 AS in0
  FULL JOIN FindReplace_156_allRules AS in1
     ON TRUE

),

FindReplace_156_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`empl_id`, concat('^', concat('(?i)', rule['empl_id']), '$'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_156_join AS in0

),

FindReplace_156_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_156_0 AS in0

),

AlteryxSelect_170 AS (

  SELECT 
    empl_id AS key_value,
    * EXCEPT (`empl_id`)
  
  FROM FindReplace_156_reorg_0 AS in0

),

FindReplace_221_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

Formula_223_0 AS (

  SELECT 
    CAST('position_number' AS string) AS primary_key,
    *
  
  FROM Join_219_inner AS in0

),

Filter_220 AS (

  {#Identifies records where value comparisons and null checks indicate mismatched or incomplete data for further review.#}
  SELECT * 
  
  FROM Formula_223_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  Value = Right_Value)
              ) OR isnull(Value)
            ) OR isnull(Right_Value)
          )
          AND NOT ((isnull(Value) AND isnull(Value)))
        )

),

FindReplace_221_join AS (

  SELECT 
    in0.previous_period AS previous_period,
    in0.Name AS Name,
    in0.current_period AS current_period,
    in1._rules AS _rules,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.position_number AS position_number
  
  FROM Filter_220 AS in0
  FULL JOIN FindReplace_221_allRules AS in1
     ON TRUE

),

FindReplace_221_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`position_number`, concat('^', rule['position_number'], '$'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_221_join AS in0

),

FindReplace_221_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`, `position_number`)
  
  FROM FindReplace_221_0 AS in0

),

Formula_236_0 AS (

  SELECT 
    CAST((CONCAT(user_name, ' (', position_number, ')')) AS string) AS user_name,
    CAST('Open Position Update' AS string) AS Comments,
    * EXCEPT (`user_name`)
  
  FROM FindReplace_221_reorg_0 AS in0

),

AlteryxSelect_226 AS (

  SELECT 
    position_number AS key_value,
    * EXCEPT (`position_number`)
  
  FROM Formula_236_0 AS in0

),

Union_161 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_170', 'AlteryxSelect_226'], 
      [
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_162_0 AS (

  SELECT 
    CAST('Update' AS string) AS transaction_type,
    *
  
  FROM Union_161 AS in0

),

FindReplace_222_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

Join_219_left AS (

  SELECT in0.*
  
  FROM Transpose_217 AS in0
  ANTI JOIN Transpose_218 AS in1
     ON ((in0.position_number = in1.position_number) AND (in0.Name = in1.Name))

),

Filter_227 AS (

  SELECT * 
  
  FROM Join_219_left AS in0
  
  WHERE (Name = 'user_name')

),

Formula_224_0 AS (

  SELECT 
    CAST('position_number' AS string) AS primary_key,
    *
  
  FROM Filter_227 AS in0

),

FindReplace_222_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period,
    in0.position_number AS position_number
  
  FROM Formula_224_0 AS in0
  FULL JOIN FindReplace_222_allRules AS in1
     ON TRUE

),

FindReplace_222_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`position_number`, concat('^', rule['position_number'], '$'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_222_join AS in0

),

FindReplace_222_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`, `position_number`)
  
  FROM FindReplace_222_0 AS in0

),

Formula_235_0 AS (

  SELECT 
    CAST((CONCAT(user_name, ' (', position_number, ')')) AS string) AS user_name,
    CAST('Open Position Insert' AS string) AS Comments,
    * EXCEPT (`user_name`)
  
  FROM FindReplace_222_reorg_0 AS in0

),

AlteryxSelect_225 AS (

  SELECT 
    period AS current_period,
    position_number AS key_value,
    * EXCEPT (`period`, `position_number`)
  
  FROM Formula_235_0 AS in0

),

Union_165 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_169', 'AlteryxSelect_225'], 
      [
        '[{"name": "current_period", "dataType": "Timestamp"}, {"name": "key_value", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "organization", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "position_number", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "Comments", "dataType": "String"}]', 
        '[{"name": "current_period", "dataType": "Timestamp"}, {"name": "key_value", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "Comments", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "organization", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_164_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (primary_key = 'empl_id')
          THEN 'Onboarded'
        ELSE 'Insert'
      END
    ) AS string) AS transaction_type,
    *
  
  FROM Union_165 AS in0

),

Union_166 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_162_0', 'Formula_164_0'], 
      [
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_159_0 AS (

  SELECT 
    CAST('System' AS string) AS created_by_system,
    (TO_TIMESTAMP((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')))) AS created_date,
    CAST('gpdip_uddl_hcdb.hc_all_users' AS string) AS source_table,
    *
  
  FROM Union_166 AS in0

),

AlteryxSelect_149 AS (

  SELECT 
    previous_period AS previous_period,
    current_period AS current_period,
    primary_key AS primary_key,
    key_value AS key_value,
    Name AS field_name,
    Value AS current_value,
    transaction_type AS TRANSACTION_TYPE,
    Comments AS Comments,
    created_by_system AS created_by,
    created_date AS created_date,
    source_table AS source_table,
    user_group AS user_group,
    user_name AS user_name,
    supervisor_name AS supervisor_name,
    line_id AS line_id,
    line AS line,
    variablefunction AS variablefunction,
    organization AS organization,
    subgroup_id AS subgroup_id,
    CAST(NULL AS string) AS previous_value,
    * EXCEPT (`loaded_date`, 
    `previous_period`, 
    `current_period`, 
    `primary_key`, 
    `key_value`, 
    `TRANSACTION_TYPE`, 
    `Comments`, 
    `created_date`, 
    `source_table`, 
    `user_group`, 
    `user_name`, 
    `supervisor_name`, 
    `line_id`, 
    `line`, 
    `variablefunction`, 
    `organization`, 
    `subgroup_id`, 
    `Name`, 
    `Value`, 
    `created_by_system`)
  
  FROM Formula_159_0 AS in0

),

Union_189_reformat_0 AS (

  SELECT 
    Comments AS Comments,
    TRANSACTION_TYPE AS TRANSACTION_TYPE,
    created_by AS created_by,
    CAST(created_date AS string) AS created_date,
    current_period AS current_period,
    CAST(current_value AS string) AS current_value,
    field_name AS field_name,
    variablefunction AS variablefunction,
    key_value AS key_value,
    line AS line,
    line_id AS line_id,
    organization AS organization,
    position_number AS position_number,
    previous_period AS previous_period,
    previous_value AS previous_value,
    primary_key AS primary_key,
    source_table AS source_table,
    subgroup_id AS subgroup_id,
    supervisor_name AS supervisor_name,
    user_group AS user_group,
    user_name AS user_name
  
  FROM AlteryxSelect_149 AS in0

),

aka_GPDIP_EDLUD_172 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4_CRIS_Period_Comparison', 'aka_GPDIP_EDLUD_172') }}

),

Filter_173 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_172 AS in0
  
  WHERE (NOT(empl_id IS NULL))

),

Formula_191_0 AS (

  SELECT 
    CAST((CONCAT(primary_rwt, '/', empl_id)) AS string) AS BK,
    *
  
  FROM Filter_173 AS in0

),

Transpose_175_schemaTransform_0 AS (

  SELECT * EXCEPT (`empl_id`, `primary_rwt`, `loaded_date`)
  
  FROM Formula_191_0 AS in0

),

Transpose_175 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_175_schemaTransform_0'], 
      ['period', 'BK'], 
      [
        'domain', 
        'ntid', 
        'rwt_id', 
        'rwt_desc', 
        'fte', 
        'percentage', 
        'skill_onb', 
        'manual_ind', 
        'modified_date', 
        'modified_by'
      ], 
      'Name', 
      'Value', 
      [
        'BK', 
        'percentage', 
        'fte', 
        'skill_onb', 
        'rwt_id', 
        'rwt_desc', 
        'domain', 
        'rwt_classification', 
        'ntid', 
        'modified_by', 
        'manual_ind', 
        'modified_date', 
        'period'
      ], 
      true
    )
  }}

),

aka_GPDIP_EDLUD_171 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4_CRIS_Period_Comparison', 'aka_GPDIP_EDLUD_171') }}

),

Filter_174 AS (

  SELECT * 
  
  FROM aka_GPDIP_EDLUD_171 AS in0
  
  WHERE (NOT(empl_id IS NULL))

),

Formula_190_0 AS (

  SELECT 
    CAST((CONCAT(primary_rwt, '/', empl_id)) AS string) AS BK,
    *
  
  FROM Filter_174 AS in0

),

Transpose_176_schemaTransform_0 AS (

  SELECT * EXCEPT (`empl_id`, `primary_rwt`, `loaded_date`, `hc_users_rwt_id`)
  
  FROM Formula_190_0 AS in0

),

Transpose_176 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_176_schemaTransform_0'], 
      ['period', 'BK'], 
      ['domain', 'ntid', 'rwt_id', 'fte', 'percentage', 'manual_ind', 'modified_date', 'modified_by'], 
      'Name', 
      'Value', 
      [
        'BK', 
        'percentage', 
        'fte', 
        'rwt_id', 
        'domain', 
        'ntid', 
        'modified_by', 
        'manual_ind', 
        'modified_date', 
        'period'
      ], 
      true
    )
  }}

),

Join_177_inner AS (

  SELECT 
    in0.period AS current_period,
    in1.period AS previous_period,
    in0.* EXCEPT (`period`, `Name`, `Value`, `BK`),
    in1.* EXCEPT (`period`)
  
  FROM Transpose_176 AS in0
  INNER JOIN Transpose_175 AS in1
     ON ((in0.Name = in1.Name) AND (in0.BK = in1.BK))

),

FindReplace_195_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

FindReplace_180_allRules AS (

  SELECT collect_list(struct(empl_id AS empl_id, loaded_date AS loaded_date, BK AS BK)) AS _rules
  
  FROM Formula_190_0 AS in0

),

Join_177_left AS (

  SELECT in0.*
  
  FROM Transpose_176 AS in0
  ANTI JOIN Transpose_175 AS in1
     ON ((in0.Name = in1.Name) AND (in0.BK = in1.BK))

),

Filter_194 AS (

  SELECT * 
  
  FROM Join_177_left AS in0
  
  WHERE (Name = 'rwt_id')

),

Formula_179_0 AS (

  SELECT 
    CAST('empl_id+primary_rwt' AS string) AS primary_key,
    *
  
  FROM Filter_194 AS in0

),

FindReplace_180_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period
  
  FROM Formula_179_0 AS in0
  FULL JOIN FindReplace_180_allRules AS in1
     ON TRUE

),

FindReplace_180_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`BK`, concat('^', rule['BK'], '$'), 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_180_join AS in0

),

FindReplace_180_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.empl_id')) AS empl_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_180_0 AS in0

),

FindReplace_195_join AS (

  SELECT 
    in0.Name AS Name,
    in0.loaded_date AS loaded_date,
    in1._rules AS _rules,
    in0.empl_id AS empl_id,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period
  
  FROM FindReplace_180_reorg_0 AS in0
  FULL JOIN FindReplace_195_allRules AS in1
     ON TRUE

),

FindReplace_195_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`empl_id`, rule['empl_id'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_195_join AS in0

),

FindReplace_195_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_195_0 AS in0

),

AlteryxSelect_183 AS (

  SELECT 
    period AS current_period,
    BK AS key_value,
    * EXCEPT (`empl_id`, `period`, `BK`)
  
  FROM FindReplace_195_reorg_0 AS in0

),

Formula_185_0 AS (

  SELECT 
    CAST('System' AS string) AS created_by_system,
    (TO_TIMESTAMP((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')))) AS created_date,
    CAST('gpdip_uddl_hcdb.hc_users_rwt' AS string) AS source_table,
    CAST('Insert' AS string) AS transaction_type,
    CAST('RWT Insert' AS string) AS Comments,
    *
  
  FROM AlteryxSelect_183 AS in0

),

Join_177_right AS (

  SELECT in0.*
  
  FROM Transpose_175 AS in0
  ANTI JOIN Transpose_176 AS in1
     ON ((in1.Name = in0.Name) AND (in1.BK = in0.BK))

),

Filter_215 AS (

  SELECT * 
  
  FROM Join_177_right AS in0
  
  WHERE (Name = 'rwt_id')

),

Formula_207_0 AS (

  SELECT 
    CAST('empl_id+primary_rwt' AS string) AS primary_key,
    *
  
  FROM Filter_215 AS in0

),

FindReplace_198_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             loaded_date AS loaded_date, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_151 AS in0

),

Formula_178_0 AS (

  SELECT 
    CAST('empl_id+primary_rwt' AS string) AS primary_key,
    *
  
  FROM Join_177_inner AS in0

),

Filter_181 AS (

  SELECT * 
  
  FROM Formula_178_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  Value = Right_Value)
              ) OR isnull(Value)
            ) OR isnull(Right_Value)
          )
          AND NOT ((isnull(Value) AND isnull(Right_Value)))
        )

),

Join_154_right AS (

  SELECT in0.*
  
  FROM Transpose_153 AS in0
  ANTI JOIN Transpose_152 AS in1
     ON ((in1.empl_id = in0.empl_id) AND (in1.Name = in0.Name))

),

Filter_201 AS (

  SELECT * 
  
  FROM Join_154_right AS in0
  
  WHERE (Name = 'user_name')

),

FindReplace_182_allRules AS (

  SELECT collect_list(struct(empl_id AS empl_id, loaded_date AS loaded_date, BK AS BK)) AS _rules
  
  FROM Formula_190_0 AS in0

),

FindReplace_182_join AS (

  SELECT 
    in0.previous_period AS previous_period,
    in0.Name AS Name,
    in0.current_period AS current_period,
    in1._rules AS _rules,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key
  
  FROM Filter_181 AS in0
  FULL JOIN FindReplace_182_allRules AS in1
     ON TRUE

),

FindReplace_182_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`BK`, concat('^', rule['BK'], '$'), 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_182_join AS in0

),

FindReplace_182_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.empl_id')) AS empl_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_182_0 AS in0

),

FindReplace_196_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_150 AS in0

),

FindReplace_196_join AS (

  SELECT 
    in0.previous_period AS previous_period,
    in0.Name AS Name,
    in0.current_period AS current_period,
    in0.loaded_date AS loaded_date,
    in1._rules AS _rules,
    in0.empl_id AS empl_id,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key
  
  FROM FindReplace_182_reorg_0 AS in0
  FULL JOIN FindReplace_196_allRules AS in1
     ON TRUE

),

FindReplace_196_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`empl_id`, rule['empl_id'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_196_join AS in0

),

FindReplace_196_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_196_0 AS in0

),

AlteryxSelect_184 AS (

  SELECT 
    BK AS key_value,
    * EXCEPT (`empl_id`, `BK`)
  
  FROM FindReplace_196_reorg_0 AS in0

),

Formula_186_0 AS (

  SELECT 
    CAST('System' AS string) AS created_by_system,
    (TO_TIMESTAMP((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')))) AS created_date,
    CAST('gpdip_uddl_hcdb.hc_users_rwt' AS string) AS source_table,
    CAST('Update' AS string) AS transaction_type,
    CAST('RWT Update' AS string) AS Comments,
    *
  
  FROM AlteryxSelect_184 AS in0

),

FindReplace_208_allRules AS (

  SELECT collect_list(struct(empl_id AS empl_id, loaded_date AS loaded_date, BK AS BK)) AS _rules
  
  FROM Formula_191_0 AS in0

),

AlteryxSelect_212 AS (

  SELECT current_period AS current_period
  
  FROM Join_177_inner AS in0

),

FindReplace_216_allRules AS (

  SELECT collect_list(
           struct(
             line_id AS line_id, 
             user_group AS user_group, 
             subgroup_id AS subgroup_id, 
             supervisor_name AS supervisor_name, 
             variablefunction AS variablefunction, 
             empl_id AS empl_id, 
             line AS line, 
             organization AS organization, 
             position_number AS position_number, 
             user_name AS user_name)) AS _rules
  
  FROM aka_GPDIP_EDLUD_151 AS in0

),

AlteryxSelect_228 AS (

  SELECT 
    period AS previous_period,
    position_number AS key_value,
    * EXCEPT (`period`, `position_number`)
  
  FROM Formula_237_0 AS in0

),

AlteryxSelect_202 AS (

  SELECT current_period AS current_period
  
  FROM Join_154_inner AS in0

),

Unique_203 AS (

  SELECT * 
  
  FROM AlteryxSelect_202 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY current_period ORDER BY current_period) = 1

),

Formula_197_0 AS (

  SELECT 
    CAST('empl_id' AS string) AS primary_key,
    CAST('User Offboarded' AS string) AS Comments,
    CAST('Offboarded' AS string) AS transaction_type,
    *
  
  FROM Filter_201 AS in0

),

FindReplace_198_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.transaction_type AS transaction_type,
    in0.empl_id AS empl_id,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period,
    in0.Comments AS Comments
  
  FROM Formula_197_0 AS in0
  FULL JOIN FindReplace_198_allRules AS in1
     ON TRUE

),

FindReplace_198_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`empl_id`, rule['empl_id'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_198_join AS in0

),

FindReplace_198_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_198_0 AS in0

),

AppendFields_199 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Unique_203 AS in0
  INNER JOIN FindReplace_198_reorg_0 AS in1
     ON TRUE

),

AlteryxSelect_200 AS (

  SELECT 
    period AS previous_period,
    empl_id AS key_value,
    * EXCEPT (`period`, `empl_id`)
  
  FROM AppendFields_199 AS in0

),

Union_204 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_200', 'AlteryxSelect_228'], 
      [
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "key_value", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "user_group", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "organization", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "position_number", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "Comments", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "key_value", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "Comments", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "user_group", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "organization", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_205_0 AS (

  SELECT 
    CAST('System' AS string) AS created_by_system,
    (TO_TIMESTAMP((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')))) AS created_date,
    CAST('gpdip_uddl_hcdb.hc_all_users_archive' AS string) AS source_table,
    *
  
  FROM Union_204 AS in0

),

AlteryxSelect_206 AS (

  SELECT 
    Name AS field_name,
    Value AS previous_value,
    created_by_system AS created_by,
    * EXCEPT (`loaded_date`, `Name`, `Value`, `created_by_system`)
  
  FROM Formula_205_0 AS in0

),

FindReplace_208_join AS (

  SELECT 
    in0.Name AS Name,
    in1._rules AS _rules,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period
  
  FROM Formula_207_0 AS in0
  FULL JOIN FindReplace_208_allRules AS in1
     ON TRUE

),

FindReplace_208_0 AS (

  SELECT 
    coalesce(
      to_json(element_at(filter(
        _rules, 
        rule -> length(regexp_extract(`BK`, rule['BK'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_208_join AS in0

),

FindReplace_208_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.empl_id')) AS empl_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.loaded_date')) AS loaded_date,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_208_0 AS in0

),

Unique_214 AS (

  SELECT * 
  
  FROM AlteryxSelect_212 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY current_period ORDER BY current_period) = 1

),

FindReplace_216_join AS (

  SELECT 
    in0.Name AS Name,
    in0.loaded_date AS loaded_date,
    in1._rules AS _rules,
    in0.empl_id AS empl_id,
    in0.BK AS BK,
    in0.Value AS `Value`,
    in0.primary_key AS primary_key,
    in0.period AS period
  
  FROM FindReplace_208_reorg_0 AS in0
  FULL JOIN FindReplace_216_allRules AS in1
     ON TRUE

),

FindReplace_216_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`empl_id`, rule['empl_id'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_216_join AS in0

),

FindReplace_216_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.user_group')) AS user_group,
    (GET_JSON_OBJECT(_extracted_rule, '$.user_name')) AS user_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.line_id')) AS line_id,
    (GET_JSON_OBJECT(_extracted_rule, '$.line')) AS line,
    (GET_JSON_OBJECT(_extracted_rule, '$.function')) AS variablefunction,
    (GET_JSON_OBJECT(_extracted_rule, '$.organization')) AS organization,
    (GET_JSON_OBJECT(_extracted_rule, '$.supervisor_name')) AS supervisor_name,
    (GET_JSON_OBJECT(_extracted_rule, '$.position_number')) AS position_number,
    (GET_JSON_OBJECT(_extracted_rule, '$.subgroup_id')) AS subgroup_id,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_216_0 AS in0

),

AppendFields_213 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Unique_214 AS in0
  INNER JOIN FindReplace_216_reorg_0 AS in1
     ON TRUE

),

AlteryxSelect_210 AS (

  SELECT 
    period AS previous_period,
    BK AS key_value,
    * EXCEPT (`empl_id`, `period`, `BK`)
  
  FROM AppendFields_213 AS in0

),

Formula_209_0 AS (

  SELECT 
    CAST('System' AS string) AS created_by_system,
    CAST((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')) AS string) AS created_date,
    CAST('gpdip_uddl_hcdb.hc_users_rwt_archive' AS string) AS source_table,
    CAST('Delete' AS string) AS transaction_type,
    CAST('RWT Delete' AS string) AS Comments,
    *
  
  FROM AlteryxSelect_210 AS in0

),

AlteryxSelect_211 AS (

  SELECT 
    Name AS field_name,
    Value AS previous_value,
    created_by_system AS created_by,
    * EXCEPT (`loaded_date`, `Name`, `Value`, `created_by_system`)
  
  FROM Formula_209_0 AS in0

),

Union_189_reformat_3 AS (

  SELECT 
    Comments AS Comments,
    created_by AS created_by,
    CAST(created_date AS string) AS created_date,
    current_period AS current_period,
    field_name AS field_name,
    variablefunction AS variablefunction,
    key_value AS key_value,
    line AS line,
    line_id AS line_id,
    organization AS organization,
    position_number AS position_number,
    previous_period AS previous_period,
    previous_value AS previous_value,
    primary_key AS primary_key,
    source_table AS source_table,
    subgroup_id AS subgroup_id,
    supervisor_name AS supervisor_name,
    user_group AS user_group,
    user_name AS user_name
  
  FROM AlteryxSelect_211 AS in0

),

Union_187 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_185_0', 'Formula_186_0'], 
      [
        '[{"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "created_date", "dataType": "Timestamp"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "created_by_system", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "created_date", "dataType": "Timestamp"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "Value", "dataType": "String"}, {"name": "loaded_date", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "transaction_type", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "created_by_system", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_188 AS (

  SELECT 
    previous_period AS previous_period,
    current_period AS current_period,
    primary_key AS primary_key,
    key_value AS key_value,
    Name AS field_name,
    Value AS current_value,
    transaction_type AS transaction_type,
    Comments AS Comments,
    created_by_system AS created_by,
    created_date AS created_date,
    source_table AS source_table,
    user_group AS user_group,
    user_name AS user_name,
    supervisor_name AS supervisor_name,
    line AS line,
    variablefunction AS variablefunction,
    organization AS organization,
    subgroup_id AS subgroup_id,
    CAST(NULL AS string) AS previous_value,
    * EXCEPT (`loaded_date`, 
    `previous_period`, 
    `current_period`, 
    `primary_key`, 
    `key_value`, 
    `transaction_type`, 
    `Comments`, 
    `created_date`, 
    `source_table`, 
    `user_group`, 
    `user_name`, 
    `supervisor_name`, 
    `line`, 
    `variablefunction`, 
    `organization`, 
    `subgroup_id`, 
    `Name`, 
    `Value`, 
    `created_by_system`)
  
  FROM Union_187 AS in0

),

Union_189_reformat_1 AS (

  SELECT 
    Comments AS Comments,
    created_by AS created_by,
    CAST(created_date AS string) AS created_date,
    current_period AS current_period,
    CAST(current_value AS string) AS current_value,
    field_name AS field_name,
    variablefunction AS variablefunction,
    key_value AS key_value,
    line AS line,
    line_id AS line_id,
    organization AS organization,
    position_number AS position_number,
    previous_period AS previous_period,
    previous_value AS previous_value,
    primary_key AS primary_key,
    source_table AS source_table,
    subgroup_id AS subgroup_id,
    supervisor_name AS supervisor_name,
    user_group AS user_group,
    user_name AS user_name
  
  FROM AlteryxSelect_188 AS in0

),

Union_189_reformat_2 AS (

  SELECT 
    Comments AS Comments,
    created_by AS created_by,
    CAST(created_date AS string) AS created_date,
    current_period AS current_period,
    field_name AS field_name,
    variablefunction AS variablefunction,
    key_value AS key_value,
    line AS line,
    line_id AS line_id,
    organization AS organization,
    position_number AS position_number,
    previous_period AS previous_period,
    previous_value AS previous_value,
    primary_key AS primary_key,
    source_table AS source_table,
    subgroup_id AS subgroup_id,
    supervisor_name AS supervisor_name,
    user_group AS user_group,
    user_name AS user_name
  
  FROM AlteryxSelect_206 AS in0

),

Union_189 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_189_reformat_0', 'Union_189_reformat_1', 'Union_189_reformat_2', 'Union_189_reformat_3'], 
      [
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "created_by", "dataType": "String"}, {"name": "previous_value", "dataType": "String"}, {"name": "created_date", "dataType": "Timestamp"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "current_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "field_name", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "TRANSACTION_TYPE", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "created_by", "dataType": "String"}, {"name": "previous_value", "dataType": "String"}, {"name": "created_date", "dataType": "Timestamp"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "current_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "field_name", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "created_by", "dataType": "String"}, {"name": "previous_value", "dataType": "String"}, {"name": "created_date", "dataType": "Timestamp"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "field_name", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]', 
        '[{"name": "previous_period", "dataType": "Timestamp"}, {"name": "organization", "dataType": "String"}, {"name": "user_group", "dataType": "String"}, {"name": "created_by", "dataType": "String"}, {"name": "previous_value", "dataType": "String"}, {"name": "created_date", "dataType": "String"}, {"name": "line", "dataType": "String"}, {"name": "current_period", "dataType": "Timestamp"}, {"name": "Comments", "dataType": "String"}, {"name": "supervisor_name", "dataType": "String"}, {"name": "subgroup_id", "dataType": "String"}, {"name": "variablefunction", "dataType": "String"}, {"name": "source_table", "dataType": "String"}, {"name": "key_value", "dataType": "String"}, {"name": "line_id", "dataType": "String"}, {"name": "field_name", "dataType": "String"}, {"name": "user_name", "dataType": "String"}, {"name": "primary_key", "dataType": "String"}, {"name": "position_number", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_192 AS (

  SELECT * 
  
  FROM Union_189 AS in0
  
  WHERE (
          (NOT((previous_value IS NULL) AND (current_value IS NULL)))
          OR (((previous_value IS NULL) AND (current_value IS NULL)) IS NULL)
        )

)

SELECT *

FROM Filter_192
