{{
  config({    
    "materialized": "ephemeral",
    "database": "rohit",
    "schema": "default"
  })
}}

WITH aka_test_oracle_356 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_356') }}

),

Filter_405 AS (

  SELECT * 
  
  FROM aka_test_oracle_356 AS in0
  
  WHERE (variabledate <= CAST('2025-04-18' AS timestamp))

),

Filter_575 AS (

  SELECT * 
  
  FROM Filter_405 AS in0
  
  WHERE ((TO_DATE(variabledate)) <= (DATE_ADD(CURRENT_DATE, CAST(-90 AS INTEGER))))

),

Summarize_574 AS (

  SELECT 
    SUM(count) AS `count`,
    study_id AS study_id,
    curve_type AS curve_type,
    standard_visit_name AS standard_visit_name
  
  FROM Filter_575 AS in0
  
  GROUP BY 
    study_id, curve_type, standard_visit_name

),

Formula_577_0 AS (

  SELECT 
    CAST((CONCAT(curve_type, '_', standard_visit_name)) AS string) AS column_name,
    *
  
  FROM Summarize_574 AS in0

),

CrossTab_576 AS (

  SELECT *
  
  FROM (
    SELECT 
      study_id,
      column_name,
      count
    
    FROM Formula_577_0 AS in0
  )
  PIVOT (
    SUM(count) AS Sum
    FOR column_name
    IN (
      'Dvso_Baseline_Site_Activated', 
      'Latest_Projection_Randomization', 
      'Planned_Site_Activated', 
      'Actuals_Site_Activated', 
      'Baseline_Site_Activated', 
      'Actuals_Randomization', 
      'Baseline_Plan_Randomization', 
      'RAPID_Baseline_Plan_Randomization'
    )
  )

),

AlteryxSelect_578 AS (

  SELECT 
    Actuals_Randomization AS rand_actuals_90d,
    Baseline_Plan_Randomization AS rand_dvso_baseline_to_date_90d,
    Latest_Projection_Randomization AS rand_latest_estimate_to_date_90d,
    RAPID_Baseline_Plan_Randomization AS rand_rapid_baseline_to_date_90d,
    * EXCEPT (`Actuals_Site_Activated`, 
    `Baseline_Site_Activated`, 
    `Dvso_Baseline_Site_Activated`, 
    `Planned_Site_Activated`, 
    `Actuals_Randomization`, 
    `Baseline_Plan_Randomization`, 
    `Latest_Projection_Randomization`, 
    `RAPID_Baseline_Plan_Randomization`)
  
  FROM CrossTab_576 AS in0

),

Filter_570 AS (

  SELECT * 
  
  FROM Filter_405 AS in0
  
  WHERE ((TO_DATE(variabledate)) <= (DATE_ADD(CURRENT_DATE, CAST(-30 AS INTEGER))))

),

Summarize_569 AS (

  SELECT 
    SUM(count) AS `count`,
    study_id AS study_id,
    curve_type AS curve_type,
    standard_visit_name AS standard_visit_name
  
  FROM Filter_570 AS in0
  
  GROUP BY 
    study_id, curve_type, standard_visit_name

),

Formula_572_0 AS (

  SELECT 
    CAST((CONCAT(curve_type, '_', standard_visit_name)) AS string) AS column_name,
    *
  
  FROM Summarize_569 AS in0

),

CrossTab_571 AS (

  SELECT *
  
  FROM (
    SELECT 
      study_id,
      column_name,
      count
    
    FROM Formula_572_0 AS in0
  )
  PIVOT (
    SUM(count) AS Sum
    FOR column_name
    IN (
      'Dvso_Baseline_Site_Activated', 
      'Latest_Projection_Randomization', 
      'Planned_Site_Activated', 
      'Actuals_Site_Activated', 
      'Baseline_Site_Activated', 
      'Actuals_Randomization', 
      'Baseline_Plan_Randomization', 
      'RAPID_Baseline_Plan_Randomization'
    )
  )

),

AlteryxSelect_573 AS (

  SELECT 
    Actuals_Randomization AS rand_actuals_30d,
    Baseline_Plan_Randomization AS rand_dvso_baseline_to_date_30d,
    Latest_Projection_Randomization AS rand_latest_estimate_to_date_30d,
    RAPID_Baseline_Plan_Randomization AS rand_rapid_baseline_to_date_30d,
    * EXCEPT (`Actuals_Site_Activated`, 
    `Baseline_Site_Activated`, 
    `Dvso_Baseline_Site_Activated`, 
    `Planned_Site_Activated`, 
    `Actuals_Randomization`, 
    `Baseline_Plan_Randomization`, 
    `Latest_Projection_Randomization`, 
    `RAPID_Baseline_Plan_Randomization`)
  
  FROM CrossTab_571 AS in0

),

Join_579_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN NULL
        ELSE in1.study_id
      END
    ) AS study_id,
    in0.* EXCEPT (`study_id`),
    in1.* EXCEPT (`study_id`)
  
  FROM AlteryxSelect_578 AS in0
  FULL JOIN AlteryxSelect_573 AS in1
     ON (in0.study_id = in1.study_id)

),

Summarize_404 AS (

  SELECT 
    SUM(count) AS `count`,
    study_id AS study_id,
    curve_type AS curve_type,
    standard_visit_name AS standard_visit_name
  
  FROM Filter_405 AS in0
  
  GROUP BY 
    study_id, curve_type, standard_visit_name

),

Formula_407_0 AS (

  SELECT 
    CAST((CONCAT(curve_type, '_', standard_visit_name)) AS string) AS column_name,
    *
  
  FROM Summarize_404 AS in0

),

CrossTab_406 AS (

  SELECT *
  
  FROM (
    SELECT 
      study_id,
      column_name,
      count
    
    FROM Formula_407_0 AS in0
  )
  PIVOT (
    SUM(count) AS Sum
    FOR column_name
    IN (
      'Dvso_Baseline_Site_Activated', 
      'Latest_Projection_Randomization', 
      'Planned_Site_Activated', 
      'Actuals_Site_Activated', 
      'Baseline_Site_Activated', 
      'Actuals_Randomization', 
      'Baseline_Plan_Randomization', 
      'RAPID_Baseline_Plan_Randomization'
    )
  )

),

AlteryxSelect_408 AS (

  SELECT 
    Planned_Site_Activated AS activation_mtp_plan_to_date,
    Actuals_Randomization AS rand_actuals,
    Actuals_Site_Activated AS activation_actuals,
    Baseline_Plan_Randomization AS rand_dvso_baseline_to_date,
    Baseline_Site_Activated AS activation_mtp_baseline_to_date,
    Dvso_Baseline_Site_Activated AS activation_dvso_baseline_to_date,
    Latest_Projection_Randomization AS rand_latest_estimate_to_date,
    RAPID_Baseline_Plan_Randomization AS rand_rapid_baseline_to_date,
    * EXCEPT (`Planned_Site_Activated`, 
    `Actuals_Randomization`, 
    `Actuals_Site_Activated`, 
    `Baseline_Plan_Randomization`, 
    `Baseline_Site_Activated`, 
    `Dvso_Baseline_Site_Activated`, 
    `Latest_Projection_Randomization`, 
    `RAPID_Baseline_Plan_Randomization`)
  
  FROM CrossTab_406 AS in0

),

Summarize_409 AS (

  SELECT 
    SUM(count) AS `count`,
    study_id AS study_id,
    curve_type AS curve_type,
    standard_visit_name AS standard_visit_name
  
  FROM aka_test_oracle_356 AS in0
  
  GROUP BY 
    study_id, curve_type, standard_visit_name

),

Formula_411_0 AS (

  SELECT 
    CAST((CONCAT(curve_type, '_', standard_visit_name)) AS string) AS column_name,
    *
  
  FROM Summarize_409 AS in0

),

CrossTab_410 AS (

  SELECT *
  
  FROM (
    SELECT 
      study_id,
      column_name,
      count
    
    FROM Formula_411_0 AS in0
  )
  PIVOT (
    SUM(count) AS Sum
    FOR column_name
    IN (
      'Dvso_Baseline_Site_Activated', 
      'Latest_Projection_Randomization', 
      'Planned_Site_Activated', 
      'Actuals_Site_Activated', 
      'Baseline_Site_Activated', 
      'Actuals_Randomization', 
      'Baseline_Plan_Randomization', 
      'RAPID_Baseline_Plan_Randomization'
    )
  )

),

AlteryxSelect_412 AS (

  SELECT 
    Baseline_Plan_Randomization AS rand_dvso_baseline_total,
    Baseline_Site_Activated AS activation_mtp_baseline_total,
    Dvso_Baseline_Site_Activated AS activation_dvso_baseline_total,
    Latest_Projection_Randomization AS rand_latest_estimate_total,
    Planned_Site_Activated AS activation_mtp_plan_total,
    RAPID_Baseline_Plan_Randomization AS rand_rapid_baseline_total,
    * EXCEPT (`Actuals_Randomization`, 
    `Actuals_Site_Activated`, 
    `Baseline_Plan_Randomization`, 
    `Baseline_Site_Activated`, 
    `Dvso_Baseline_Site_Activated`, 
    `Latest_Projection_Randomization`, 
    `Planned_Site_Activated`, 
    `RAPID_Baseline_Plan_Randomization`)
  
  FROM CrossTab_410 AS in0

),

Join_413_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN NULL
        ELSE in1.study_id
      END
    ) AS study_id,
    in0.* EXCEPT (`study_id`),
    in1.* EXCEPT (`study_id`)
  
  FROM AlteryxSelect_408 AS in0
  FULL JOIN AlteryxSelect_412 AS in1
     ON (in0.study_id = in1.study_id)

),

Join_581_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN NULL
        ELSE in1.study_id
      END
    ) AS study_id,
    in0.* EXCEPT (`study_id`),
    in1.* EXCEPT (`study_id`)
  
  FROM Join_579_left_UnionFullOuter AS in0
  FULL JOIN Join_413_left_UnionFullOuter AS in1
     ON (in0.study_id = in1.study_id)

),

Cleanse_415 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_581_left_UnionFullOuter'], 
      [
        { "name": "study_id", "dataType": "String" }, 
        { "name": "rand_actuals_90d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_actuals_30d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_to_date", "dataType": "Bigint" }, 
        { "name": "rand_actuals", "dataType": "Bigint" }, 
        { "name": "activation_actuals", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_total", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_total", "dataType": "Bigint" }
      ], 
      'keepOriginal', 
      [], 
      false, 
      '', 
      true, 
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

DynamicInput_417 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'DynamicInput_417') }}

),

AlteryxSelect_418 AS (

  SELECT 
    study_id AS study_id,
    study_site_number AS study_site_number,
    subj_entrd_cnt AS subj_entrd_cnt
  
  FROM DynamicInput_417 AS in0

),

Summarize_419 AS (

  SELECT 
    SUM(subj_entrd_cnt) AS site_file_rand_count,
    study_id AS study_id
  
  FROM AlteryxSelect_418 AS in0
  
  GROUP BY study_id

),

Join_357_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN NULL
        ELSE in1.study_id
      END
    ) AS study_id,
    in0.* EXCEPT (`study_id`),
    in1.* EXCEPT (`study_id`)
  
  FROM Cleanse_415 AS in0
  RIGHT JOIN Summarize_419 AS in1
     ON (in0.study_id = in1.study_id)

),

aka_test_oracle_586 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_586') }}

),

AlteryxSelect_441 AS (

  SELECT 
    study_number_pfe AS study_number_pfe,
    protocol_id AS protocol_id,
    study_id AS study_id,
    drug_program_code AS drug_program_code,
    candidate_code AS candidate_code,
    protocol_description_working AS protocol_description_working,
    protocol_description_planning AS protocol_description_planning,
    study_title AS study_title,
    study_status AS study_status,
    study_phase AS study_phase,
    study_phase_planned AS study_phase_planned,
    study_type AS study_type,
    methodology_study AS methodology_study,
    compassionate_use_study AS compassionate_use_study,
    post_author_safety_study AS post_author_safety_study,
    ccs_clinical_placement AS ccs_clinical_placement,
    project_plan_type AS project_plan_type,
    study_execution_state AS study_execution_state,
    subject_type AS subject_type,
    study_status_planning AS study_status_planning,
    transfer_status AS transfer_status,
    model AS model,
    front_end AS front_end,
    back_end AS back_end,
    study_closed AS study_closed,
    commercial_bu AS commercial_bu,
    study_type_plan AS study_type_plan,
    assigned_ctms AS assigned_ctms,
    patient_database AS patient_database,
    sponsor_product AS sponsor_product,
    business_group AS business_group,
    medical_responsibility AS medical_responsibility,
    sponsoring_division AS sponsoring_division,
    sponsoring_unit AS sponsoring_unit,
    study_therapeutic_area AS study_therapeutic_area,
    study_design AS study_design,
    business_rationale AS business_rationale,
    pediatric_study AS pediatric_study,
    primary_data_collection AS primary_data_collection,
    secondary_data_collection AS secondary_data_collection,
    funding_source AS funding_source,
    subj_max_age_der AS subj_max_age_der,
    subj_min_age_der AS subj_min_age_der,
    study_post_reg_commitment AS study_post_reg_commitment,
    CAST(planned_sites AS DOUBLE) AS activation_rapid_plan_total,
    study_finance_code AS study_finance_code,
    study_alias AS study_alias,
    study_nctid AS study_nctid,
    manual_milestone_change AS manual_milestone_change,
    study_subjects_randomized_reg AS study_subjects_randomized,
    sites_ongoing AS sites_active,
    non_cancelled_sites_total_der AS sites_total_der,
    asset AS asset,
    product_der AS product_der,
    business_category_der AS business_category_der,
    business_category AS business_category,
    study_status_indicator AS study_status_indicator,
    study_phase_lifecycle AS study_phase_lifecycle,
    lead_clinician AS lead_clinician,
    extension_study AS extension_study,
    indication_preferred_term_list AS indication_preferred_term_list,
    study_end_date_source AS study_end_date_source,
    partner_binned AS partner_binned,
    partner_allocated AS partner_allocated,
    opco_study_start AS opco_study_start,
    study_next_milestone AS study_next_milestone,
    goal_detail_list AS goal_detail_list,
    mop_tl_previous AS mop_tl_previous,
    mop_study_status AS mop_study_status,
    candidate_division AS candidate_division,
    candidate_status AS candidate_status,
    candidate_sub_division AS candidate_sub_division,
    candidate_sub_unit AS candidate_sub_unit,
    compound_type_der AS compound_type_der,
    mechanism_of_action AS mechanism_of_action,
    override_summary_list AS override_summary_list,
    candidate_disease_area AS candidate_disease_area,
    candidate_finance_code AS candidate_finance_code,
    candidate_phase AS candidate_phase,
    candidate_priority AS candidate_priority,
    candidate_therapeutic_area AS candidate_therapeutic_area,
    candidate_type AS candidate_type,
    primary_indication_list AS primary_indication_list,
    compound_number AS compound_number,
    compound_type AS compound_type,
    compound_type_binned AS compound_type_binned,
    * EXCEPT (`study_number_pfe`, 
    `protocol_id`, 
    `study_id`, 
    `drug_program_code`, 
    `candidate_code`, 
    `protocol_description_working`, 
    `protocol_description_planning`, 
    `study_title`, 
    `study_status`, 
    `study_phase`, 
    `study_phase_planned`, 
    `study_type`, 
    `methodology_study`, 
    `compassionate_use_study`, 
    `post_author_safety_study`, 
    `ccs_clinical_placement`, 
    `project_plan_type`, 
    `study_execution_state`, 
    `subject_type`, 
    `study_status_planning`, 
    `transfer_status`, 
    `model`, 
    `front_end`, 
    `back_end`, 
    `study_closed`, 
    `commercial_bu`, 
    `study_type_plan`, 
    `assigned_ctms`, 
    `patient_database`, 
    `sponsor_product`, 
    `business_group`, 
    `medical_responsibility`, 
    `sponsoring_division`, 
    `sponsoring_unit`, 
    `study_therapeutic_area`, 
    `study_design`, 
    `business_rationale`, 
    `pediatric_study`, 
    `primary_data_collection`, 
    `secondary_data_collection`, 
    `funding_source`, 
    `subj_max_age_der`, 
    `subj_min_age_der`, 
    `study_post_reg_commitment`, 
    `study_finance_code`, 
    `study_alias`, 
    `study_nctid`, 
    `manual_milestone_change`, 
    `asset`, 
    `product_der`, 
    `business_category_der`, 
    `business_category`, 
    `study_status_indicator`, 
    `study_phase_lifecycle`, 
    `lead_clinician`, 
    `extension_study`, 
    `indication_preferred_term_list`, 
    `study_end_date_source`, 
    `partner_binned`, 
    `partner_allocated`, 
    `opco_study_start`, 
    `study_next_milestone`, 
    `goal_detail_list`, 
    `mop_tl_previous`, 
    `mop_study_status`, 
    `candidate_division`, 
    `candidate_status`, 
    `candidate_sub_division`, 
    `candidate_sub_unit`, 
    `compound_type_der`, 
    `mechanism_of_action`, 
    `override_summary_list`, 
    `candidate_disease_area`, 
    `candidate_finance_code`, 
    `candidate_phase`, 
    `candidate_priority`, 
    `candidate_therapeutic_area`, 
    `candidate_type`, 
    `primary_indication_list`, 
    `compound_number`, 
    `compound_type`, 
    `compound_type_binned`, 
    `planned_sites`, 
    `study_subjects_randomized_reg`, 
    `sites_ongoing`, 
    `non_cancelled_sites_total_der`)
  
  FROM aka_test_oracle_586 AS in0

),

aka_test_oracle_448 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_448') }}

),

Join_363_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`study_number_pfe`)
  
  FROM AlteryxSelect_441 AS in0
  LEFT JOIN aka_test_oracle_448 AS in1
     ON (in0.study_number_pfe = in1.study_number_pfe)

),

Join_358_right_UnionRightOuter AS (

  SELECT 
    in1.protocol_id AS protocol_id,
    in1.study_status AS study_status,
    in1.study_type AS study_type,
    in1.planned_patients AS planned_patients,
    in1.asset AS asset,
    in1.business_category_der AS business_category_der,
    in1.priority_level AS priority_level,
    in1.candidate_priority AS candidate_priority,
    in1.rapid_model AS rapid_model,
    in0.rand_actuals AS rand_actuals,
    in0.activation_actuals AS activation_actuals,
    in0.rand_dvso_baseline_to_date AS rand_dvso_baseline_to_date,
    in0.activation_mtp_baseline_to_date AS activation_mtp_baseline_to_date,
    in0.activation_dvso_baseline_to_date AS activation_dvso_baseline_to_date,
    in0.rand_latest_estimate_to_date AS rand_latest_estimate_to_date,
    in0.activation_mtp_plan_to_date AS activation_mtp_plan_to_date,
    in0.rand_rapid_baseline_to_date AS rand_rapid_baseline_to_date,
    in0.rand_dvso_baseline_total AS rand_dvso_baseline_total,
    in0.activation_mtp_baseline_total AS activation_mtp_baseline_total,
    in0.activation_dvso_baseline_total AS activation_dvso_baseline_total,
    in0.rand_latest_estimate_total AS rand_latest_estimate_total,
    in0.activation_mtp_plan_total AS activation_mtp_plan_total,
    in0.rand_rapid_baseline_total AS rand_rapid_baseline_total,
    in1.amendments_prior_to_fsfv AS amendments_prior_to_fsfv,
    in1.max_amendment_date_prior_to_fsfv AS max_amendment_date_prior_to_fsfv,
    in1.amendments_during_enrollment AS amendments_during_enrollment,
    in1.max_amendment_date_during_enrollment AS max_amendment_date_during_enrollment,
    in1.total_number_amendments AS total_number_amendments,
    in0.rand_actuals_90d AS rand_actuals_90d,
    in0.rand_dvso_baseline_to_date_90d AS rand_dvso_baseline_to_date_90d,
    in0.rand_latest_estimate_to_date_90d AS rand_latest_estimate_to_date_90d,
    in0.rand_rapid_baseline_to_date_90d AS rand_rapid_baseline_to_date_90d,
    in0.rand_actuals_30d AS rand_actuals_30d,
    in0.rand_dvso_baseline_to_date_30d AS rand_dvso_baseline_to_date_30d,
    in0.rand_latest_estimate_to_date_30d AS rand_latest_estimate_to_date_30d,
    in0.rand_rapid_baseline_to_date_30d AS rand_rapid_baseline_to_date_30d,
    in0.site_file_rand_count AS site_file_rand_count,
    in1.study_number_pfe AS study_number_pfe,
    in1.activation_rapid_plan_total AS activation_rapid_plan_total,
    in1.fsfv_date AS fsfv_date,
    in1.fsfv_pct_cmp AS fsfv_pct_cmp,
    in1.lsfv_date AS lsfv_date,
    in1.lsfv_pct_cmp AS lsfv_pct_cmp,
    in1.sites100_date AS sites100_date,
    in1.sites100_pct_cmp AS sites100_pct_cmp,
    in1.study_phase_bin AS study_phase_bin,
    in1.mop_tl_current AS mop_tl_current,
    in1.mop_study_status AS mop_study_status,
    in1.cd_patient_population_age_der AS cd_patient_population_age_der,
    in1.candidate_portfolio_priority AS candidate_portfolio_priority,
    in1.goal_study AS goal_study,
    in1.recruitment_review_scope_flag AS recruitment_review_scope_flag,
    in1.drug_program_code AS drug_program_code,
    in1.candidate_code AS candidate_code,
    in1.protocol_description_working AS protocol_description_working,
    in1.protocol_description_planning AS protocol_description_planning,
    in1.study_title AS study_title,
    in1.study_phase AS study_phase,
    in1.study_phase_planned AS study_phase_planned,
    in1.study_sub_type AS study_sub_type,
    in1.methodology_study AS methodology_study,
    in1.compassionate_use_study AS compassionate_use_study,
    in1.post_author_safety_study AS post_author_safety_study,
    in1.post_author_effective_study AS post_author_effective_study,
    in1.ccs_clinical_placement AS ccs_clinical_placement,
    in1.project_plan_type AS project_plan_type,
    in1.study_execution_state AS study_execution_state,
    in1.subject_type AS subject_type,
    in1.study_status_planning AS study_status_planning,
    in1.transfer_status AS transfer_status,
    in1.model AS model,
    in1.front_end AS front_end,
    in1.study_termination_date AS study_termination_date,
    in1.back_end AS back_end,
    in1.study_closed AS study_closed,
    in1.commercial_bu AS commercial_bu,
    in1.study_type_plan AS study_type_plan,
    in1.assigned_ctms AS assigned_ctms,
    in1.patient_database AS patient_database,
    in1.sponsor_product AS sponsor_product,
    in1.business_group AS business_group,
    in1.medical_responsibility AS medical_responsibility,
    in1.terminate_decision_date AS terminate_decision_date,
    in1.sponsoring_division AS sponsoring_division,
    in1.sponsoring_unit AS sponsoring_unit,
    in1.study_therapeutic_area AS study_therapeutic_area,
    in1.study_design AS study_design,
    in1.business_rationale AS business_rationale,
    in1.pediatric_study AS pediatric_study,
    in1.primary_data_collection AS primary_data_collection,
    in1.secondary_data_collection AS secondary_data_collection,
    in1.study_project_planner_planning AS study_project_planner_planning,
    in1.funding_source AS funding_source,
    in1.subj_max_age_der AS subj_max_age_der,
    in1.subj_min_age_der AS subj_min_age_der,
    in1.study_post_reg_commitment AS study_post_reg_commitment,
    in1.eudract_num AS eudract_num,
    in1.study_status_assessment_date AS study_status_assessment_date,
    in1.study_finance_code AS study_finance_code,
    in1.load_ts_cdm AS load_ts_cdm,
    in1.study_alias AS study_alias,
    in1.study_nctid AS study_nctid,
    in1.bl137t_date AS bl137t_date,
    in1.bl137t_pct_cmp AS bl137t_pct_cmp,
    in1.bl137p_date AS bl137p_date,
    in1.bl137p_pct_cmp AS bl137p_pct_cmp,
    in1.arp_finish_date AS arp_finish_date,
    in1.arp_finish_pct_cmp AS arp_finish_pct_cmp,
    in1.arp_start_date AS arp_start_date,
    in1.bdrprep_date AS bdrprep_date,
    in1.bdrprep_pct_cmp AS bdrprep_pct_cmp,
    in1.bdrprep_start_date AS bdrprep_start_date,
    in1.prc_date AS prc_date,
    in1.prc_pct_cmp AS prc_pct_cmp,
    in1.ps_date AS ps_date,
    in1.ps_pct_cmp AS ps_pct_cmp,
    in1.ep3_date AS ep3_date,
    in1.ep3_pct_cmp AS ep3_pct_cmp,
    in1.fap_date AS fap_date,
    in1.fap_pct_cmp AS fap_pct_cmp,
    in1.fap_source AS fap_source,
    in1.fap_plw_date AS fap_plw_date,
    in1.fap_plw_pct_cmp AS fap_plw_pct_cmp,
    in1.dba_date AS dba_date,
    in1.dba_pct_cmp AS dba_pct_cmp,
    in1.siv_date AS siv_date,
    in1.siv_pct_cmp AS siv_pct_cmp,
    in1.siv_source AS siv_source,
    in1.siv_us_date AS siv_us_date,
    in1.siv_us_pct_cmp AS siv_us_pct_cmp,
    in1.siv_exus_date AS siv_exus_date,
    in1.siv_exus_pct_cmp AS siv_exus_pct_cmp,
    in1.crfdata_date AS crfdata_date,
    in1.crfdata_pct_cmp AS crfdata_pct_cmp,
    in1.csr_max_date AS csr_max_date,
    in1.csr_min_date AS csr_min_date,
    in1.csr_pct_cmp AS csr_pct_cmp,
    in1.csr_plw_date AS csr_plw_date,
    in1.csr_plw_pct_cmp AS csr_plw_pct_cmp,
    in1.csr_sem2 AS csr_sem2,
    in1.csr_source AS csr_source,
    in1.dataready227t_date AS dataready227t_date,
    in1.dataready227t_pct_cmp AS dataready227t_pct_cmp,
    in1.dbr_date AS dbr_date,
    in1.dbr_pct_cmp AS dbr_pct_cmp,
    in1.dbr_sem2 AS dbr_sem2,
    in1.der_csr_max_date AS der_csr_max_date,
    in1.der_csr_max_pct_cmp AS der_csr_max_pct_cmp,
    in1.der_csr_max_source AS der_csr_max_source,
    in1.der_dbr_max_date AS der_dbr_max_date,
    in1.der_dbr_max_pct_cmp AS der_dbr_max_pct_cmp,
    in1.der_dbr_max_source AS der_dbr_max_source,
    in1.der_lslv_max_date AS der_lslv_max_date,
    in1.der_lslv_max_pct_cmp AS der_lslv_max_pct_cmp,
    in1.der_lslv_max_source AS der_lslv_max_source,
    in1.fsfd_date AS fsfd_date,
    in1.fsfd_pct_cmp AS fsfd_pct_cmp,
    in1.fsfd_sem2 AS fsfd_sem2,
    in1.fsfv_plw_date AS fsfv_plw_date,
    in1.fsfv_plw_pct_cmp AS fsfv_plw_pct_cmp,
    in1.fsfv_sem2 AS fsfv_sem2,
    in1.fsfv_source AS fsfv_source,
    in1.ft_date AS ft_date,
    in1.ft_pct_cmp AS ft_pct_cmp,
    in1.ft_sem2 AS ft_sem2,
    in1.labdata_date AS labdata_date,
    in1.labdata_pct_cmp AS labdata_pct_cmp,
    in1.lastdata_date AS lastdata_date,
    in1.lastdata_pct_cmp AS lastdata_pct_cmp,
    in1.lastdata_source AS lastdata_source,
    in1.ldi_date AS ldi_date,
    in1.ldi_pct_cmp AS ldi_pct_cmp,
    in1.ldis_date AS ldis_date,
    in1.ldis_pct_cmp AS ldis_pct_cmp,
    in1.lsfv_sem2 AS lsfv_sem2,
    in1.lslv_date AS lslv_date,
    in1.lslv_pcd_date AS lslv_pcd_date,
    in1.lslv_pcd_pct_cmp AS lslv_pcd_pct_cmp,
    in1.lslv_pcd_sem2 AS lslv_pcd_sem2,
    in1.lslv_pcd_source AS lslv_pcd_source,
    in1.lslv_pct_cmp AS lslv_pct_cmp,
    in1.lslv_plw_date AS lslv_plw_date,
    in1.lslv_plw_pct_cmp AS lslv_plw_pct_cmp,
    in1.lslv_sem2 AS lslv_sem2,
    in1.lslv_source AS lslv_source,
    in1.pcd_date AS pcd_date,
    in1.pcd_pct_cmp AS pcd_pct_cmp,
    in1.pcd_sem2 AS pcd_sem2,
    in1.pkdata_date AS pkdata_date,
    in1.pkdata_pct_cmp AS pkdata_pct_cmp,
    in1.scd_date AS scd_date,
    in1.scd_pct_cmp AS scd_pct_cmp,
    in1.scd_sem2 AS scd_sem2,
    in1.scsr_date AS scsr_date,
    in1.scsr_pct_cmp AS scsr_pct_cmp,
    in1.scsr_sem2 AS scsr_sem2,
    in1.sdbr_date AS sdbr_date,
    in1.sdbr_pct_cmp AS sdbr_pct_cmp,
    in1.sdbr_sem2 AS sdbr_sem2,
    in1.serology_date AS serology_date,
    in1.serology_pct_cmp AS serology_pct_cmp,
    in1.sites100_sem2 AS sites100_sem2,
    in1.sites50_date AS sites50_date,
    in1.sites50_pct_cmp AS sites50_pct_cmp,
    in1.sites50_sem2 AS sites50_sem2,
    in1.slock353p_date AS slock353p_date,
    in1.slock353p_pct_cmp AS slock353p_pct_cmp,
    in1.stlftlr_date AS stlftlr_date,
    in1.stlftlr_pct_cmp AS stlftlr_pct_cmp,
    in1.stlr_date AS stlr_date,
    in1.stlr_pct_cmp AS stlr_pct_cmp,
    in1.subjects25_date AS subjects25_date,
    in1.subjects25_pct_cmp AS subjects25_pct_cmp,
    in1.subjects25_sem2 AS subjects25_sem2,
    in1.subjects50_date AS subjects50_date,
    in1.subjects50_pct_cmp AS subjects50_pct_cmp,
    in1.subjects50_sem2 AS subjects50_sem2,
    in1.subjects75_date AS subjects75_date,
    in1.subjects75_pct_cmp AS subjects75_pct_cmp,
    in1.subjects75_sem2 AS subjects75_sem2,
    in1.tlftlr_date AS tlftlr_date,
    in1.tlftlr_pct_cmp AS tlftlr_pct_cmp,
    in1.tlftlr_start_date AS tlftlr_start_date,
    in1.tlr_date AS tlr_date,
    in1.tlr_pct_cmp AS tlr_pct_cmp,
    in1.tlr_sem2 AS tlr_sem2,
    in1.fih_date AS fih_date,
    in1.fih_pct AS fih_pct,
    in1.pom_date AS pom_date,
    in1.pom_pct AS pom_pct,
    in1.poc_ss_date AS poc_ss_date,
    in1.poc_ss_pct AS poc_ss_pct,
    in1.poc_date AS poc_date,
    in1.poc_pct AS poc_pct,
    in1.phase_3_start_date AS phase_3_start_date,
    in1.phase_3_start_pct AS phase_3_start_pct,
    in1.nda_submission_date AS nda_submission_date,
    in1.nda_submission_pct AS nda_submission_pct,
    in1.maa_submission_date AS maa_submission_date,
    in1.maa_submission_pct AS maa_submission_pct,
    in1.jnda_submission_date AS jnda_submission_date,
    in1.jnda_submission_pct AS jnda_submission_pct,
    in1.nda_approval_date AS nda_approval_date,
    in1.nda_approval_pct AS nda_approval_pct,
    in1.maa_approval_date AS maa_approval_date,
    in1.maa_approval_pct AS maa_approval_pct,
    in1.jnda_approval_date AS jnda_approval_date,
    in1.jnda_approval_pct AS jnda_approval_pct,
    in1.poc_ss_gem_date AS poc_ss_gem_date,
    in1.poc_gem_date AS poc_gem_date,
    in1.phase_3_start_gem_date AS phase_3_start_gem_date,
    in1.nda_submission_gem_date AS nda_submission_gem_date,
    in1.maa_submission_gem_date AS maa_submission_gem_date,
    in1.jnda_submission_gem_date AS jnda_submission_gem_date,
    in1.nda_approval_gem_date AS nda_approval_gem_date,
    in1.maa_approval_gem_date AS maa_approval_gem_date,
    in1.jnda_approval_gem_date AS jnda_approval_gem_date,
    in1.manual_milestone_change AS manual_milestone_change,
    in1.multi_csr_flag AS multi_csr_flag,
    in1.study_subjects_active_reg AS study_subjects_active_reg,
    in1.study_subjects_screened_reg AS study_subjects_screened_reg,
    in1.study_subjects_randomized AS study_subjects_randomized,
    in1.study_subjects_completed_reg AS study_subjects_completed_reg,
    in1.study_subjects_discontinued_reg AS study_subjects_discontinued_reg,
    in1.study_subjects_screen_failed_reg AS study_subjects_screen_failed_reg,
    in1.retention_rate AS retention_rate,
    in1.screen_failure_rate AS screen_failure_rate,
    in1.sites_completed AS sites_completed,
    in1.sites_cancelled AS sites_cancelled,
    in1.sites_active AS sites_active,
    in1.sites_proposed AS sites_proposed,
    in1.sites_planned AS sites_planned,
    in1.sites_total_der AS sites_total_der,
    in1.product_der AS product_der,
    in1.business_category AS business_category,
    in1.study_status_indicator AS study_status_indicator,
    in1.study_phase_lifecycle AS study_phase_lifecycle,
    in1.lead_clinician AS lead_clinician,
    in1.study_point_of_contact AS study_point_of_contact,
    in1.extension_study AS extension_study,
    in1.internalized_study AS internalized_study,
    in1.indication_preferred_term_list AS indication_preferred_term_list,
    in1.acquisition_date AS acquisition_date,
    in1.study_end_date_source AS study_end_date_source,
    in1.partner_binned AS partner_binned,
    in1.partner_allocated AS partner_allocated,
    in1.msa_vendor_name AS msa_vendor_name,
    in1.tier1_provider AS tier1_provider,
    in1.opco_agmt_min_date AS opco_agmt_min_date,
    in1.opco_agmt_max_date AS opco_agmt_max_date,
    in1.opco_study_start AS opco_study_start,
    in1.cost_division AS cost_division,
    in1.study_next_milestone AS study_next_milestone,
    in1.study_next_milestone_date AS study_next_milestone_date,
    in1.gov_tracked_asset AS gov_tracked_asset,
    in1.goal_detail_list AS goal_detail_list,
    in1.bic_scope AS bic_scope,
    in1.bic_scope_working AS bic_scope_working,
    in1.mop_tl_previous AS mop_tl_previous,
    in1.mop_lastsaved_dt AS mop_lastsaved_dt,
    in1.dvso_approved_plan AS dvso_approved_plan,
    in1.candidate_division AS candidate_division,
    in1.candidate_status AS candidate_status,
    in1.candidate_sub_division AS candidate_sub_division,
    in1.candidate_sub_unit AS candidate_sub_unit,
    in1.compound_type_der AS compound_type_der,
    in1.mechanism_of_action AS mechanism_of_action,
    in1.pacd AS pacd,
    in1.cd_subj_min_age_yr_der AS cd_subj_min_age_yr_der,
    in1.cd_subj_max_age_yr_der AS cd_subj_max_age_yr_der,
    in1.critical_flag AS critical_flag,
    in1.critical_flag_detail AS critical_flag_detail,
    in1.recruitment_review_meeting AS recruitment_review_meeting,
    in1.study_goal AS study_goal,
    in1.ssr_study AS ssr_study,
    in1.cd_primary_exclude_der AS cd_primary_exclude_der,
    in1.cd_trial_type_final_der AS cd_trial_type_final_der,
    in1.cd_clinpharm_study_der AS cd_clinpharm_study_der,
    in1.cd_dev_japan_study_flag AS cd_dev_japan_study_flag,
    in1.cd_dev_china_study_flag AS cd_dev_china_study_flag,
    in1.cd_pcru_study_flag AS cd_pcru_study_flag,
    in1.cd_concat_pcru_named_site_flag AS cd_concat_pcru_named_site_flag,
    in1.cd_pcrc_study_flag AS cd_pcrc_study_flag,
    in1.cd_concat_pcrc_named_site_flag AS cd_concat_pcrc_named_site_flag,
    in1.override_summary_list AS override_summary_list,
    in1.country_list_terminated AS country_list_terminated,
    in1.planned_country_list AS planned_country_list,
    in1.country_list_active AS country_list_active,
    in1.country_list_cancelled AS country_list_cancelled,
    in1.country_list_completed AS country_list_completed,
    in1.country_list_planned AS country_list_planned,
    in1.country_list_proposed AS country_list_proposed,
    in1.candidate_disease_area AS candidate_disease_area,
    in1.candidate_finance_code AS candidate_finance_code,
    in1.candidate_phase AS candidate_phase,
    in1.candidate_therapeutic_area AS candidate_therapeutic_area,
    in1.candidate_type AS candidate_type,
    in1.primary_indication_list AS primary_indication_list,
    in1.compound_source AS compound_source,
    in1.compound_number AS compound_number,
    in1.compound_type AS compound_type,
    in1.compound_acquired_company_name AS compound_acquired_company_name,
    in1.compound_type_binned AS compound_type_binned,
    in1.snapshot_date AS snapshot_date,
    in1.study_end_date AS study_end_date,
    in1.mytrial_model AS mytrial_model,
    in1.sites_terminated AS sites_terminated,
    in1.lslb_pct_cmp AS lslb_pct_cmp,
    in1.lslb_date AS lslb_date,
    in1.ps_sem2 AS ps_sem2,
    in1.fap_sem2 AS fap_sem2,
    in1.lslv_nda_submission_date AS lslv_nda_submission_date,
    in1.lslv_nda_submission_pct AS lslv_nda_submission_pct,
    in1.lslv_maa_submission_date AS lslv_maa_submission_date,
    in1.lslv_maa_submission_pct AS lslv_maa_submission_pct,
    in1.study_next_milestone_sem2 AS study_next_milestone_sem2,
    in1.study_next_milestone_variance AS study_next_milestone_variance,
    in1.candidate_investment_category AS candidate_investment_category,
    in1.sites_activated AS sites_activated,
    in1.sites_selected AS sites_selected,
    in1.last_study_milestone AS last_study_milestone,
    in1.last_study_milestone_date AS last_study_milestone_date,
    in1.milestones_forecasted_12_months AS milestones_forecasted_12_months,
    in1.rationale_for_mop_traffic_light AS rationale_for_mop_traffic_light,
    in1.mop_lastsaved_by AS mop_lastsaved_by,
    in1.country_list_selected AS country_list_selected,
    in1.country_list_activated AS country_list_activated,
    in1.country_list_unknown_legacy_status AS country_list_unknown_legacy_status,
    in1.study_min_site_activated_dt AS study_min_site_activated_dt,
    in1.study_min_derived_site_activated_dt AS study_min_derived_site_activated_dt,
    in1.study_min_derived_site_activated_dt_source AS study_min_derived_site_activated_dt_source,
    in1.poc_ss_target_bl AS poc_ss_target_bl,
    in1.poc_target_bl AS poc_target_bl,
    in1.phase_3_start_target_bl AS phase_3_start_target_bl,
    in1.nda_submission_target_bl AS nda_submission_target_bl,
    in1.maa_submission_target_bl AS maa_submission_target_bl,
    in1.jnda_submission_target_bl AS jnda_submission_target_bl,
    in1.nda_approval_target_bl AS nda_approval_target_bl,
    in1.maa_approval_target_bl AS maa_approval_target_bl,
    in1.jnda_approval_target_bl AS jnda_approval_target_bl,
    in1.csr_target_bl AS csr_target_bl,
    in1.dbr_target_bl AS dbr_target_bl,
    in1.fsfd_target_bl AS fsfd_target_bl,
    in1.fsfv_target_bl AS fsfv_target_bl,
    in1.ft_target_bl AS ft_target_bl,
    in1.lsfv_target_bl AS lsfv_target_bl,
    in1.lslv_pcd_target_bl AS lslv_pcd_target_bl,
    in1.lslv_target_bl AS lslv_target_bl,
    in1.pcd_target_bl AS pcd_target_bl,
    in1.scd_target_bl AS scd_target_bl,
    in1.scsr_target_bl AS scsr_target_bl,
    in1.sdbr_target_bl AS sdbr_target_bl,
    in1.sites100_target_bl AS sites100_target_bl,
    in1.sites50_target_bl AS sites50_target_bl,
    in1.subjects25_target_bl AS subjects25_target_bl,
    in1.subjects50_target_bl AS subjects50_target_bl,
    in1.subjects75_target_bl AS subjects75_target_bl,
    in1.tlr_target_bl AS tlr_target_bl,
    in1.ps_target_bl AS ps_target_bl,
    in1.fap_target_bl AS fap_target_bl,
    in1.study_next_milestone_target_bl AS study_next_milestone_target_bl,
    in1.study_baseline_event AS study_baseline_event,
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN NULL
        ELSE in1.study_id
      END
    ) AS study_id,
    in0.* EXCEPT (`rand_actuals`, 
    `activation_actuals`, 
    `rand_dvso_baseline_to_date`, 
    `activation_mtp_baseline_to_date`, 
    `activation_dvso_baseline_to_date`, 
    `rand_latest_estimate_to_date`, 
    `activation_mtp_plan_to_date`, 
    `rand_rapid_baseline_to_date`, 
    `rand_dvso_baseline_total`, 
    `activation_mtp_baseline_total`, 
    `activation_dvso_baseline_total`, 
    `rand_latest_estimate_total`, 
    `activation_mtp_plan_total`, 
    `rand_rapid_baseline_total`, 
    `rand_actuals_90d`, 
    `rand_dvso_baseline_to_date_90d`, 
    `rand_latest_estimate_to_date_90d`, 
    `rand_rapid_baseline_to_date_90d`, 
    `rand_actuals_30d`, 
    `rand_dvso_baseline_to_date_30d`, 
    `rand_latest_estimate_to_date_30d`, 
    `rand_rapid_baseline_to_date_30d`, 
    `site_file_rand_count`, 
    `study_id`),
    in1.* EXCEPT (`protocol_id`, 
    `study_id`, 
    `study_status`, 
    `study_type`, 
    `planned_patients`, 
    `asset`, 
    `business_category_der`, 
    `priority_level`, 
    `candidate_priority`, 
    `rapid_model`, 
    `amendments_prior_to_fsfv`, 
    `max_amendment_date_prior_to_fsfv`, 
    `amendments_during_enrollment`, 
    `max_amendment_date_during_enrollment`, 
    `total_number_amendments`, 
    `study_number_pfe`, 
    `activation_rapid_plan_total`, 
    `fsfv_date`, 
    `fsfv_pct_cmp`, 
    `lsfv_date`, 
    `lsfv_pct_cmp`, 
    `sites100_date`, 
    `sites100_pct_cmp`, 
    `study_phase_bin`, 
    `mop_tl_current`, 
    `mop_study_status`, 
    `cd_patient_population_age_der`, 
    `candidate_portfolio_priority`, 
    `goal_study`, 
    `recruitment_review_scope_flag`, 
    `drug_program_code`, 
    `candidate_code`, 
    `protocol_description_working`, 
    `protocol_description_planning`, 
    `study_title`, 
    `study_phase`, 
    `study_phase_planned`, 
    `study_sub_type`, 
    `methodology_study`, 
    `compassionate_use_study`, 
    `post_author_safety_study`, 
    `post_author_effective_study`, 
    `ccs_clinical_placement`, 
    `project_plan_type`, 
    `study_execution_state`, 
    `subject_type`, 
    `study_status_planning`, 
    `transfer_status`, 
    `model`, 
    `front_end`, 
    `study_termination_date`, 
    `back_end`, 
    `study_closed`, 
    `commercial_bu`, 
    `study_type_plan`, 
    `assigned_ctms`, 
    `patient_database`, 
    `sponsor_product`, 
    `business_group`, 
    `medical_responsibility`, 
    `terminate_decision_date`, 
    `sponsoring_division`, 
    `sponsoring_unit`, 
    `study_therapeutic_area`, 
    `study_design`, 
    `business_rationale`, 
    `pediatric_study`, 
    `primary_data_collection`, 
    `secondary_data_collection`, 
    `study_project_planner_planning`, 
    `funding_source`, 
    `subj_max_age_der`, 
    `subj_min_age_der`, 
    `study_post_reg_commitment`, 
    `eudract_num`, 
    `study_status_assessment_date`, 
    `study_finance_code`, 
    `load_ts_cdm`, 
    `study_alias`, 
    `study_nctid`, 
    `bl137t_date`, 
    `bl137t_pct_cmp`, 
    `bl137p_date`, 
    `bl137p_pct_cmp`, 
    `arp_finish_date`, 
    `arp_finish_pct_cmp`, 
    `arp_start_date`, 
    `bdrprep_date`, 
    `bdrprep_pct_cmp`, 
    `bdrprep_start_date`, 
    `prc_date`, 
    `prc_pct_cmp`, 
    `ps_date`, 
    `ps_pct_cmp`, 
    `ep3_date`, 
    `ep3_pct_cmp`, 
    `fap_date`, 
    `fap_pct_cmp`, 
    `fap_source`, 
    `fap_plw_date`, 
    `fap_plw_pct_cmp`, 
    `dba_date`, 
    `dba_pct_cmp`, 
    `siv_date`, 
    `siv_pct_cmp`, 
    `siv_source`, 
    `siv_us_date`, 
    `siv_us_pct_cmp`, 
    `siv_exus_date`, 
    `siv_exus_pct_cmp`, 
    `crfdata_date`, 
    `crfdata_pct_cmp`, 
    `csr_max_date`, 
    `csr_min_date`, 
    `csr_pct_cmp`, 
    `csr_plw_date`, 
    `csr_plw_pct_cmp`, 
    `csr_sem2`, 
    `csr_source`, 
    `dataready227t_date`, 
    `dataready227t_pct_cmp`, 
    `dbr_date`, 
    `dbr_pct_cmp`, 
    `dbr_sem2`, 
    `der_csr_max_date`, 
    `der_csr_max_pct_cmp`, 
    `der_csr_max_source`, 
    `der_dbr_max_date`, 
    `der_dbr_max_pct_cmp`, 
    `der_dbr_max_source`, 
    `der_lslv_max_date`, 
    `der_lslv_max_pct_cmp`, 
    `der_lslv_max_source`, 
    `fsfd_date`, 
    `fsfd_pct_cmp`, 
    `fsfd_sem2`, 
    `fsfv_plw_date`, 
    `fsfv_plw_pct_cmp`, 
    `fsfv_sem2`, 
    `fsfv_source`, 
    `ft_date`, 
    `ft_pct_cmp`, 
    `ft_sem2`, 
    `labdata_date`, 
    `labdata_pct_cmp`, 
    `lastdata_date`, 
    `lastdata_pct_cmp`, 
    `lastdata_source`, 
    `ldi_date`, 
    `ldi_pct_cmp`, 
    `ldis_date`, 
    `ldis_pct_cmp`, 
    `lsfv_sem2`, 
    `lslv_date`, 
    `lslv_pcd_date`, 
    `lslv_pcd_pct_cmp`, 
    `lslv_pcd_sem2`, 
    `lslv_pcd_source`, 
    `lslv_pct_cmp`, 
    `lslv_plw_date`, 
    `lslv_plw_pct_cmp`, 
    `lslv_sem2`, 
    `lslv_source`, 
    `pcd_date`, 
    `pcd_pct_cmp`, 
    `pcd_sem2`, 
    `pkdata_date`, 
    `pkdata_pct_cmp`, 
    `scd_date`, 
    `scd_pct_cmp`, 
    `scd_sem2`, 
    `scsr_date`, 
    `scsr_pct_cmp`, 
    `scsr_sem2`, 
    `sdbr_date`, 
    `sdbr_pct_cmp`, 
    `sdbr_sem2`, 
    `serology_date`, 
    `serology_pct_cmp`, 
    `sites100_sem2`, 
    `sites50_date`, 
    `sites50_pct_cmp`, 
    `sites50_sem2`, 
    `slock353p_date`, 
    `slock353p_pct_cmp`, 
    `stlftlr_date`, 
    `stlftlr_pct_cmp`, 
    `stlr_date`, 
    `stlr_pct_cmp`, 
    `subjects25_date`, 
    `subjects25_pct_cmp`, 
    `subjects25_sem2`, 
    `subjects50_date`, 
    `subjects50_pct_cmp`, 
    `subjects50_sem2`, 
    `subjects75_date`, 
    `subjects75_pct_cmp`, 
    `subjects75_sem2`, 
    `tlftlr_date`, 
    `tlftlr_pct_cmp`, 
    `tlftlr_start_date`, 
    `tlr_date`, 
    `tlr_pct_cmp`, 
    `tlr_sem2`, 
    `fih_date`, 
    `fih_pct`, 
    `pom_date`, 
    `pom_pct`, 
    `poc_ss_date`, 
    `poc_ss_pct`, 
    `poc_date`, 
    `poc_pct`, 
    `phase_3_start_date`, 
    `phase_3_start_pct`, 
    `nda_submission_date`, 
    `nda_submission_pct`, 
    `maa_submission_date`, 
    `maa_submission_pct`, 
    `jnda_submission_date`, 
    `jnda_submission_pct`, 
    `nda_approval_date`, 
    `nda_approval_pct`, 
    `maa_approval_date`, 
    `maa_approval_pct`, 
    `jnda_approval_date`, 
    `jnda_approval_pct`, 
    `poc_ss_gem_date`, 
    `poc_gem_date`, 
    `phase_3_start_gem_date`, 
    `nda_submission_gem_date`, 
    `maa_submission_gem_date`, 
    `jnda_submission_gem_date`, 
    `nda_approval_gem_date`, 
    `maa_approval_gem_date`, 
    `jnda_approval_gem_date`, 
    `manual_milestone_change`, 
    `multi_csr_flag`, 
    `study_subjects_active_reg`, 
    `study_subjects_screened_reg`, 
    `study_subjects_randomized`, 
    `study_subjects_completed_reg`, 
    `study_subjects_discontinued_reg`, 
    `study_subjects_screen_failed_reg`, 
    `retention_rate`, 
    `screen_failure_rate`, 
    `sites_completed`, 
    `sites_cancelled`, 
    `sites_active`, 
    `sites_proposed`, 
    `sites_planned`, 
    `sites_total_der`, 
    `product_der`, 
    `business_category`, 
    `study_status_indicator`, 
    `study_phase_lifecycle`, 
    `lead_clinician`, 
    `study_point_of_contact`, 
    `extension_study`, 
    `internalized_study`, 
    `indication_preferred_term_list`, 
    `acquisition_date`, 
    `study_end_date_source`, 
    `partner_binned`, 
    `partner_allocated`, 
    `msa_vendor_name`, 
    `tier1_provider`, 
    `opco_agmt_min_date`, 
    `opco_agmt_max_date`, 
    `opco_study_start`, 
    `cost_division`, 
    `study_next_milestone`, 
    `study_next_milestone_date`, 
    `gov_tracked_asset`, 
    `goal_detail_list`, 
    `bic_scope`, 
    `bic_scope_working`, 
    `mop_tl_previous`, 
    `mop_lastsaved_dt`, 
    `dvso_approved_plan`, 
    `candidate_division`, 
    `candidate_status`, 
    `candidate_sub_division`, 
    `candidate_sub_unit`, 
    `compound_type_der`, 
    `mechanism_of_action`, 
    `pacd`, 
    `cd_subj_min_age_yr_der`, 
    `cd_subj_max_age_yr_der`, 
    `critical_flag`, 
    `critical_flag_detail`, 
    `recruitment_review_meeting`, 
    `study_goal`, 
    `ssr_study`, 
    `cd_primary_exclude_der`, 
    `cd_trial_type_final_der`, 
    `cd_clinpharm_study_der`, 
    `cd_dev_japan_study_flag`, 
    `cd_dev_china_study_flag`, 
    `cd_pcru_study_flag`, 
    `cd_concat_pcru_named_site_flag`, 
    `cd_pcrc_study_flag`, 
    `cd_concat_pcrc_named_site_flag`, 
    `override_summary_list`, 
    `country_list_terminated`, 
    `planned_country_list`, 
    `country_list_active`, 
    `country_list_cancelled`, 
    `country_list_completed`, 
    `country_list_planned`, 
    `country_list_proposed`, 
    `candidate_disease_area`, 
    `candidate_finance_code`, 
    `candidate_phase`, 
    `candidate_therapeutic_area`, 
    `candidate_type`, 
    `primary_indication_list`, 
    `compound_source`, 
    `compound_number`, 
    `compound_type`, 
    `compound_acquired_company_name`, 
    `compound_type_binned`, 
    `snapshot_date`, 
    `study_end_date`, 
    `mytrial_model`, 
    `sites_terminated`, 
    `lslb_pct_cmp`, 
    `lslb_date`, 
    `ps_sem2`, 
    `fap_sem2`, 
    `lslv_nda_submission_date`, 
    `lslv_nda_submission_pct`, 
    `lslv_maa_submission_date`, 
    `lslv_maa_submission_pct`, 
    `study_next_milestone_sem2`, 
    `study_next_milestone_variance`, 
    `candidate_investment_category`, 
    `sites_activated`, 
    `sites_selected`, 
    `last_study_milestone`, 
    `last_study_milestone_date`, 
    `milestones_forecasted_12_months`, 
    `rationale_for_mop_traffic_light`, 
    `mop_lastsaved_by`, 
    `country_list_selected`, 
    `country_list_activated`, 
    `country_list_unknown_legacy_status`, 
    `study_min_site_activated_dt`, 
    `study_min_derived_site_activated_dt`, 
    `study_min_derived_site_activated_dt_source`, 
    `poc_ss_target_bl`, 
    `poc_target_bl`, 
    `phase_3_start_target_bl`, 
    `nda_submission_target_bl`, 
    `maa_submission_target_bl`, 
    `jnda_submission_target_bl`, 
    `nda_approval_target_bl`, 
    `maa_approval_target_bl`, 
    `jnda_approval_target_bl`, 
    `csr_target_bl`, 
    `dbr_target_bl`, 
    `fsfd_target_bl`, 
    `fsfv_target_bl`, 
    `ft_target_bl`, 
    `lsfv_target_bl`, 
    `lslv_pcd_target_bl`, 
    `lslv_target_bl`, 
    `pcd_target_bl`, 
    `scd_target_bl`, 
    `scsr_target_bl`, 
    `sdbr_target_bl`, 
    `sites100_target_bl`, 
    `sites50_target_bl`, 
    `subjects25_target_bl`, 
    `subjects50_target_bl`, 
    `subjects75_target_bl`, 
    `tlr_target_bl`, 
    `ps_target_bl`, 
    `fap_target_bl`, 
    `study_next_milestone_target_bl`, 
    `study_baseline_event`)
  
  FROM Join_357_right_UnionRightOuter AS in0
  RIGHT JOIN Join_363_left_UnionLeftOuter AS in1
     ON (in0.study_id = in1.study_id)

),

DynamicInput_446 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'DynamicInput_446') }}

),

AlteryxSelect_444 AS (

  SELECT 
    study_id AS study_id,
    partial_plan AS dvso_partial_plan_flag,
    partial_plan_desc AS dvso_partial_plan_desc
  
  FROM DynamicInput_446 AS in0

),

Formula_445_0 AS (

  SELECT 
    CAST('All Participants' AS string) AS cohort_type,
    CAST('All Participants' AS string) AS cohort_name,
    *
  
  FROM AlteryxSelect_444 AS in0

),

Join_361_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.study_id = in1.study_id)
          THEN in1.study_id
        ELSE NULL
      END
    ) AS Right_STUDY_ID,
    in0.*,
    in1.* EXCEPT (`study_id`, `STUDY_ID`)
  
  FROM Join_358_right_UnionRightOuter AS in0
  LEFT JOIN Formula_445_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Filter_476 AS (

  SELECT * 
  
  FROM Join_361_left_UnionLeftOuter AS in0
  
  WHERE ((rand_actuals = 0) AND (site_file_rand_count > 0))

),

Filter_476_reject AS (

  SELECT * 
  
  FROM Join_361_left_UnionLeftOuter AS in0
  
  WHERE (
          (NOT((rand_actuals = 0) AND (site_file_rand_count > 0)))
          OR (((rand_actuals = 0) AND (site_file_rand_count > 0)) IS NULL)
        )

),

AlteryxSelect_478 AS (

  SELECT * EXCEPT (`site_file_rand_count`)
  
  FROM Filter_476_reject AS in0

),

Union_479 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_478', 'Filter_476'], 
      [
        '[{"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}]', 
        '[{"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "site_file_rand_count", "dataType": "Double"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_455_reject AS (

  SELECT * 
  
  FROM Union_479 AS in0
  
  WHERE (
          (NOT((rand_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL)))
          OR (((rand_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL)) IS NULL)
        )

),

Formula_457_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (rand_rapid_baseline_total > 0)
          THEN rand_rapid_baseline_to_date
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_plan_to_date,
    *
  
  FROM Filter_455_reject AS in0

),

Filter_455 AS (

  SELECT * 
  
  FROM Union_479 AS in0
  
  WHERE ((rand_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL))

),

Formula_456_0 AS (

  SELECT 
    CAST(rand_dvso_baseline_to_date AS DOUBLE) AS rand_plan_to_date,
    CAST(rand_dvso_baseline_to_date_90d AS DOUBLE) AS rand_plan_to_date_90d,
    CAST(rand_dvso_baseline_to_date_30d AS DOUBLE) AS rand_plan_to_date_30d,
    CAST('DVSO Baseline' AS string) AS rand_plan_to_date_src,
    *
  
  FROM Filter_455 AS in0

),

Union_460 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_456_0', 'Formula_457_0'], 
      [
        '[{"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]', 
        '[{"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_482 AS (

  SELECT * 
  
  FROM Union_460 AS in0
  
  WHERE (rand_plan_to_date_src = 'DVSO Baseline')

),

Formula_483_0 AS (

  SELECT 
    CAST(rand_dvso_baseline_total AS DOUBLE) AS rand_plan,
    CAST('DVSO Baseline' AS string) AS rand_plan_src,
    *
  
  FROM Filter_482 AS in0

),

Filter_482_reject AS (

  SELECT * 
  
  FROM Union_460 AS in0
  
  WHERE (
          (
            (
              NOT(
                rand_plan_to_date_src = 'DVSO Baseline')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          OR ((rand_plan_to_date_src = 'DVSO Baseline') IS NULL)
        )

),

Formula_484_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (rand_rapid_baseline_total > 0)
          THEN rand_rapid_baseline_total
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_plan,
    *
  
  FROM Filter_482_reject AS in0

),

Union_487 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_483_0', 'Formula_484_0'], 
      [
        '[{"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_src", "dataType": "String"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]', 
        '[{"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Cleanse_451 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_487'], 
      [
        { "name": "rand_plan", "dataType": "Double" }, 
        { "name": "rand_plan_src", "dataType": "String" }, 
        { "name": "rand_plan_to_date", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_90d", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_30d", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_src", "dataType": "String" }, 
        { "name": "Right_STUDY_ID", "dataType": "String" }, 
        { "name": "protocol_id", "dataType": "String" }, 
        { "name": "study_status", "dataType": "String" }, 
        { "name": "study_type", "dataType": "String" }, 
        { "name": "planned_patients", "dataType": "Double" }, 
        { "name": "asset", "dataType": "String" }, 
        { "name": "business_category_der", "dataType": "String" }, 
        { "name": "priority_level", "dataType": "String" }, 
        { "name": "candidate_priority", "dataType": "String" }, 
        { "name": "rapid_model", "dataType": "String" }, 
        { "name": "rand_actuals", "dataType": "Bigint" }, 
        { "name": "activation_actuals", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_to_date", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_total", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_total", "dataType": "Bigint" }, 
        { "name": "amendments_prior_to_fsfv", "dataType": "Integer" }, 
        { "name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp" }, 
        { "name": "amendments_during_enrollment", "dataType": "Integer" }, 
        { "name": "max_amendment_date_during_enrollment", "dataType": "Timestamp" }, 
        { "name": "total_number_amendments", "dataType": "Integer" }, 
        { "name": "rand_actuals_90d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_actuals_30d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "study_number_pfe", "dataType": "String" }, 
        { "name": "activation_rapid_plan_total", "dataType": "Double" }, 
        { "name": "fsfv_date", "dataType": "Timestamp" }, 
        { "name": "fsfv_pct_cmp", "dataType": "Integer" }, 
        { "name": "lsfv_date", "dataType": "Timestamp" }, 
        { "name": "lsfv_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites100_date", "dataType": "Timestamp" }, 
        { "name": "sites100_pct_cmp", "dataType": "Integer" }, 
        { "name": "study_phase_bin", "dataType": "String" }, 
        { "name": "mop_tl_current", "dataType": "String" }, 
        { "name": "mop_study_status", "dataType": "String" }, 
        { "name": "cd_patient_population_age_der", "dataType": "String" }, 
        { "name": "candidate_portfolio_priority", "dataType": "String" }, 
        { "name": "goal_study", "dataType": "String" }, 
        { "name": "recruitment_review_scope_flag", "dataType": "String" }, 
        { "name": "drug_program_code", "dataType": "String" }, 
        { "name": "candidate_code", "dataType": "String" }, 
        { "name": "protocol_description_working", "dataType": "String" }, 
        { "name": "protocol_description_planning", "dataType": "String" }, 
        { "name": "study_title", "dataType": "String" }, 
        { "name": "study_phase", "dataType": "String" }, 
        { "name": "study_phase_planned", "dataType": "String" }, 
        { "name": "study_sub_type", "dataType": "String" }, 
        { "name": "methodology_study", "dataType": "String" }, 
        { "name": "compassionate_use_study", "dataType": "String" }, 
        { "name": "post_author_safety_study", "dataType": "String" }, 
        { "name": "post_author_effective_study", "dataType": "String" }, 
        { "name": "ccs_clinical_placement", "dataType": "String" }, 
        { "name": "project_plan_type", "dataType": "String" }, 
        { "name": "study_execution_state", "dataType": "String" }, 
        { "name": "subject_type", "dataType": "String" }, 
        { "name": "study_status_planning", "dataType": "String" }, 
        { "name": "transfer_status", "dataType": "String" }, 
        { "name": "model", "dataType": "String" }, 
        { "name": "front_end", "dataType": "String" }, 
        { "name": "study_termination_date", "dataType": "Timestamp" }, 
        { "name": "back_end", "dataType": "String" }, 
        { "name": "study_closed", "dataType": "String" }, 
        { "name": "commercial_bu", "dataType": "String" }, 
        { "name": "study_type_plan", "dataType": "String" }, 
        { "name": "assigned_ctms", "dataType": "String" }, 
        { "name": "patient_database", "dataType": "String" }, 
        { "name": "sponsor_product", "dataType": "String" }, 
        { "name": "business_group", "dataType": "String" }, 
        { "name": "medical_responsibility", "dataType": "String" }, 
        { "name": "terminate_decision_date", "dataType": "Timestamp" }, 
        { "name": "sponsoring_division", "dataType": "String" }, 
        { "name": "sponsoring_unit", "dataType": "String" }, 
        { "name": "study_therapeutic_area", "dataType": "String" }, 
        { "name": "study_design", "dataType": "String" }, 
        { "name": "business_rationale", "dataType": "String" }, 
        { "name": "pediatric_study", "dataType": "String" }, 
        { "name": "primary_data_collection", "dataType": "String" }, 
        { "name": "secondary_data_collection", "dataType": "String" }, 
        { "name": "study_project_planner_planning", "dataType": "String" }, 
        { "name": "funding_source", "dataType": "String" }, 
        { "name": "subj_max_age_der", "dataType": "String" }, 
        { "name": "subj_min_age_der", "dataType": "String" }, 
        { "name": "study_post_reg_commitment", "dataType": "String" }, 
        { "name": "eudract_num", "dataType": "String" }, 
        { "name": "study_status_assessment_date", "dataType": "Timestamp" }, 
        { "name": "study_finance_code", "dataType": "String" }, 
        { "name": "load_ts_cdm", "dataType": "Timestamp" }, 
        { "name": "study_alias", "dataType": "String" }, 
        { "name": "study_nctid", "dataType": "String" }, 
        { "name": "bl137t_date", "dataType": "Timestamp" }, 
        { "name": "bl137t_pct_cmp", "dataType": "Integer" }, 
        { "name": "bl137p_date", "dataType": "Timestamp" }, 
        { "name": "bl137p_pct_cmp", "dataType": "Integer" }, 
        { "name": "arp_finish_date", "dataType": "Timestamp" }, 
        { "name": "arp_finish_pct_cmp", "dataType": "Integer" }, 
        { "name": "arp_start_date", "dataType": "Timestamp" }, 
        { "name": "bdrprep_date", "dataType": "Timestamp" }, 
        { "name": "bdrprep_pct_cmp", "dataType": "Integer" }, 
        { "name": "bdrprep_start_date", "dataType": "Timestamp" }, 
        { "name": "prc_date", "dataType": "Timestamp" }, 
        { "name": "prc_pct_cmp", "dataType": "Integer" }, 
        { "name": "ps_date", "dataType": "Timestamp" }, 
        { "name": "ps_pct_cmp", "dataType": "Integer" }, 
        { "name": "ep3_date", "dataType": "Timestamp" }, 
        { "name": "ep3_pct_cmp", "dataType": "Integer" }, 
        { "name": "fap_date", "dataType": "Timestamp" }, 
        { "name": "fap_pct_cmp", "dataType": "Integer" }, 
        { "name": "fap_source", "dataType": "String" }, 
        { "name": "fap_plw_date", "dataType": "Timestamp" }, 
        { "name": "fap_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "dba_date", "dataType": "Timestamp" }, 
        { "name": "dba_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_date", "dataType": "Timestamp" }, 
        { "name": "siv_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_source", "dataType": "String" }, 
        { "name": "siv_us_date", "dataType": "Timestamp" }, 
        { "name": "siv_us_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_exus_date", "dataType": "Timestamp" }, 
        { "name": "siv_exus_pct_cmp", "dataType": "Integer" }, 
        { "name": "crfdata_date", "dataType": "Timestamp" }, 
        { "name": "crfdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_max_date", "dataType": "Timestamp" }, 
        { "name": "csr_min_date", "dataType": "Timestamp" }, 
        { "name": "csr_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_plw_date", "dataType": "Timestamp" }, 
        { "name": "csr_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_sem2", "dataType": "Timestamp" }, 
        { "name": "csr_source", "dataType": "String" }, 
        { "name": "dataready227t_date", "dataType": "Timestamp" }, 
        { "name": "dataready227t_pct_cmp", "dataType": "Integer" }, 
        { "name": "dbr_date", "dataType": "Timestamp" }, 
        { "name": "dbr_pct_cmp", "dataType": "Integer" }, 
        { "name": "dbr_sem2", "dataType": "Timestamp" }, 
        { "name": "der_csr_max_date", "dataType": "Timestamp" }, 
        { "name": "der_csr_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_csr_max_source", "dataType": "String" }, 
        { "name": "der_dbr_max_date", "dataType": "Timestamp" }, 
        { "name": "der_dbr_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_dbr_max_source", "dataType": "String" }, 
        { "name": "der_lslv_max_date", "dataType": "Timestamp" }, 
        { "name": "der_lslv_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_lslv_max_source", "dataType": "String" }, 
        { "name": "fsfd_date", "dataType": "Timestamp" }, 
        { "name": "fsfd_pct_cmp", "dataType": "Integer" }, 
        { "name": "fsfd_sem2", "dataType": "Timestamp" }, 
        { "name": "fsfv_plw_date", "dataType": "Timestamp" }, 
        { "name": "fsfv_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "fsfv_sem2", "dataType": "Timestamp" }, 
        { "name": "fsfv_source", "dataType": "String" }, 
        { "name": "ft_date", "dataType": "Timestamp" }, 
        { "name": "ft_pct_cmp", "dataType": "Integer" }, 
        { "name": "ft_sem2", "dataType": "Timestamp" }, 
        { "name": "labdata_date", "dataType": "Timestamp" }, 
        { "name": "labdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "lastdata_date", "dataType": "Timestamp" }, 
        { "name": "lastdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "lastdata_source", "dataType": "String" }, 
        { "name": "ldi_date", "dataType": "Timestamp" }, 
        { "name": "ldi_pct_cmp", "dataType": "Integer" }, 
        { "name": "ldis_date", "dataType": "Timestamp" }, 
        { "name": "ldis_pct_cmp", "dataType": "Integer" }, 
        { "name": "lsfv_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_date", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_date", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_pcd_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_source", "dataType": "String" }, 
        { "name": "lslv_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_plw_date", "dataType": "Timestamp" }, 
        { "name": "lslv_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_source", "dataType": "String" }, 
        { "name": "pcd_date", "dataType": "Timestamp" }, 
        { "name": "pcd_pct_cmp", "dataType": "Integer" }, 
        { "name": "pcd_sem2", "dataType": "Timestamp" }, 
        { "name": "pkdata_date", "dataType": "Timestamp" }, 
        { "name": "pkdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "scd_date", "dataType": "Timestamp" }, 
        { "name": "scd_pct_cmp", "dataType": "Integer" }, 
        { "name": "scd_sem2", "dataType": "Timestamp" }, 
        { "name": "scsr_date", "dataType": "Timestamp" }, 
        { "name": "scsr_pct_cmp", "dataType": "Integer" }, 
        { "name": "scsr_sem2", "dataType": "Timestamp" }, 
        { "name": "sdbr_date", "dataType": "Timestamp" }, 
        { "name": "sdbr_pct_cmp", "dataType": "Integer" }, 
        { "name": "sdbr_sem2", "dataType": "Timestamp" }, 
        { "name": "serology_date", "dataType": "Timestamp" }, 
        { "name": "serology_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites100_sem2", "dataType": "Timestamp" }, 
        { "name": "sites50_date", "dataType": "Timestamp" }, 
        { "name": "sites50_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites50_sem2", "dataType": "Timestamp" }, 
        { "name": "slock353p_date", "dataType": "Timestamp" }, 
        { "name": "slock353p_pct_cmp", "dataType": "Integer" }, 
        { "name": "stlftlr_date", "dataType": "Timestamp" }, 
        { "name": "stlftlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "stlr_date", "dataType": "Timestamp" }, 
        { "name": "stlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects25_date", "dataType": "Timestamp" }, 
        { "name": "subjects25_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects25_sem2", "dataType": "Timestamp" }, 
        { "name": "subjects50_date", "dataType": "Timestamp" }, 
        { "name": "subjects50_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects50_sem2", "dataType": "Timestamp" }, 
        { "name": "subjects75_date", "dataType": "Timestamp" }, 
        { "name": "subjects75_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects75_sem2", "dataType": "Timestamp" }, 
        { "name": "tlftlr_date", "dataType": "Timestamp" }, 
        { "name": "tlftlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "tlftlr_start_date", "dataType": "Timestamp" }, 
        { "name": "tlr_date", "dataType": "Timestamp" }, 
        { "name": "tlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "tlr_sem2", "dataType": "Timestamp" }, 
        { "name": "fih_date", "dataType": "Timestamp" }, 
        { "name": "fih_pct", "dataType": "Integer" }, 
        { "name": "pom_date", "dataType": "Timestamp" }, 
        { "name": "pom_pct", "dataType": "Integer" }, 
        { "name": "poc_ss_date", "dataType": "Timestamp" }, 
        { "name": "poc_ss_pct", "dataType": "Integer" }, 
        { "name": "poc_date", "dataType": "Timestamp" }, 
        { "name": "poc_pct", "dataType": "Integer" }, 
        { "name": "phase_3_start_date", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_pct", "dataType": "Integer" }, 
        { "name": "nda_submission_date", "dataType": "Timestamp" }, 
        { "name": "nda_submission_pct", "dataType": "Integer" }, 
        { "name": "maa_submission_date", "dataType": "Timestamp" }, 
        { "name": "maa_submission_pct", "dataType": "Integer" }, 
        { "name": "jnda_submission_date", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_pct", "dataType": "Integer" }, 
        { "name": "nda_approval_date", "dataType": "Timestamp" }, 
        { "name": "nda_approval_pct", "dataType": "Integer" }, 
        { "name": "maa_approval_date", "dataType": "Timestamp" }, 
        { "name": "maa_approval_pct", "dataType": "Integer" }, 
        { "name": "jnda_approval_date", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_pct", "dataType": "Integer" }, 
        { "name": "poc_ss_gem_date", "dataType": "Timestamp" }, 
        { "name": "poc_gem_date", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_gem_date", "dataType": "Timestamp" }, 
        { "name": "nda_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "maa_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "nda_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "maa_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "manual_milestone_change", "dataType": "String" }, 
        { "name": "multi_csr_flag", "dataType": "String" }, 
        { "name": "study_subjects_active_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_screened_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_randomized", "dataType": "Integer" }, 
        { "name": "study_subjects_completed_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_discontinued_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_screen_failed_reg", "dataType": "Integer" }, 
        { "name": "retention_rate", "dataType": "Double" }, 
        { "name": "screen_failure_rate", "dataType": "Double" }, 
        { "name": "sites_completed", "dataType": "Integer" }, 
        { "name": "sites_cancelled", "dataType": "Integer" }, 
        { "name": "sites_active", "dataType": "Integer" }, 
        { "name": "sites_proposed", "dataType": "Integer" }, 
        { "name": "sites_planned", "dataType": "Integer" }, 
        { "name": "sites_total_der", "dataType": "Integer" }, 
        { "name": "product_der", "dataType": "String" }, 
        { "name": "business_category", "dataType": "String" }, 
        { "name": "study_status_indicator", "dataType": "String" }, 
        { "name": "study_phase_lifecycle", "dataType": "String" }, 
        { "name": "lead_clinician", "dataType": "String" }, 
        { "name": "study_point_of_contact", "dataType": "String" }, 
        { "name": "extension_study", "dataType": "String" }, 
        { "name": "internalized_study", "dataType": "String" }, 
        { "name": "indication_preferred_term_list", "dataType": "String" }, 
        { "name": "acquisition_date", "dataType": "Timestamp" }, 
        { "name": "study_end_date_source", "dataType": "String" }, 
        { "name": "partner_binned", "dataType": "String" }, 
        { "name": "partner_allocated", "dataType": "String" }, 
        { "name": "msa_vendor_name", "dataType": "String" }, 
        { "name": "tier1_provider", "dataType": "String" }, 
        { "name": "opco_agmt_min_date", "dataType": "Timestamp" }, 
        { "name": "opco_agmt_max_date", "dataType": "Timestamp" }, 
        { "name": "opco_study_start", "dataType": "String" }, 
        { "name": "cost_division", "dataType": "String" }, 
        { "name": "study_next_milestone", "dataType": "String" }, 
        { "name": "study_next_milestone_date", "dataType": "Timestamp" }, 
        { "name": "gov_tracked_asset", "dataType": "String" }, 
        { "name": "goal_detail_list", "dataType": "String" }, 
        { "name": "bic_scope", "dataType": "String" }, 
        { "name": "bic_scope_working", "dataType": "String" }, 
        { "name": "mop_tl_previous", "dataType": "String" }, 
        { "name": "mop_lastsaved_dt", "dataType": "Timestamp" }, 
        { "name": "dvso_approved_plan", "dataType": "String" }, 
        { "name": "candidate_division", "dataType": "String" }, 
        { "name": "candidate_status", "dataType": "String" }, 
        { "name": "candidate_sub_division", "dataType": "String" }, 
        { "name": "candidate_sub_unit", "dataType": "String" }, 
        { "name": "compound_type_der", "dataType": "String" }, 
        { "name": "mechanism_of_action", "dataType": "String" }, 
        { "name": "pacd", "dataType": "String" }, 
        { "name": "cd_subj_min_age_yr_der", "dataType": "Double" }, 
        { "name": "cd_subj_max_age_yr_der", "dataType": "Double" }, 
        { "name": "critical_flag", "dataType": "String" }, 
        { "name": "critical_flag_detail", "dataType": "String" }, 
        { "name": "recruitment_review_meeting", "dataType": "String" }, 
        { "name": "study_goal", "dataType": "String" }, 
        { "name": "ssr_study", "dataType": "String" }, 
        { "name": "cd_primary_exclude_der", "dataType": "String" }, 
        { "name": "cd_trial_type_final_der", "dataType": "String" }, 
        { "name": "cd_clinpharm_study_der", "dataType": "String" }, 
        { "name": "cd_dev_japan_study_flag", "dataType": "String" }, 
        { "name": "cd_dev_china_study_flag", "dataType": "String" }, 
        { "name": "cd_pcru_study_flag", "dataType": "String" }, 
        { "name": "cd_concat_pcru_named_site_flag", "dataType": "String" }, 
        { "name": "cd_pcrc_study_flag", "dataType": "String" }, 
        { "name": "cd_concat_pcrc_named_site_flag", "dataType": "String" }, 
        { "name": "override_summary_list", "dataType": "String" }, 
        { "name": "country_list_terminated", "dataType": "String" }, 
        { "name": "planned_country_list", "dataType": "String" }, 
        { "name": "country_list_active", "dataType": "String" }, 
        { "name": "country_list_cancelled", "dataType": "String" }, 
        { "name": "country_list_completed", "dataType": "String" }, 
        { "name": "country_list_planned", "dataType": "String" }, 
        { "name": "country_list_proposed", "dataType": "String" }, 
        { "name": "candidate_disease_area", "dataType": "String" }, 
        { "name": "candidate_finance_code", "dataType": "String" }, 
        { "name": "candidate_phase", "dataType": "String" }, 
        { "name": "candidate_therapeutic_area", "dataType": "String" }, 
        { "name": "candidate_type", "dataType": "String" }, 
        { "name": "primary_indication_list", "dataType": "String" }, 
        { "name": "compound_source", "dataType": "String" }, 
        { "name": "compound_number", "dataType": "String" }, 
        { "name": "compound_type", "dataType": "String" }, 
        { "name": "compound_acquired_company_name", "dataType": "String" }, 
        { "name": "compound_type_binned", "dataType": "String" }, 
        { "name": "snapshot_date", "dataType": "Timestamp" }, 
        { "name": "study_end_date", "dataType": "Timestamp" }, 
        { "name": "mytrial_model", "dataType": "String" }, 
        { "name": "sites_terminated", "dataType": "Integer" }, 
        { "name": "lslb_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslb_date", "dataType": "Timestamp" }, 
        { "name": "ps_sem2", "dataType": "Timestamp" }, 
        { "name": "fap_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_nda_submission_date", "dataType": "Timestamp" }, 
        { "name": "lslv_nda_submission_pct", "dataType": "Integer" }, 
        { "name": "lslv_maa_submission_date", "dataType": "Timestamp" }, 
        { "name": "lslv_maa_submission_pct", "dataType": "Integer" }, 
        { "name": "study_next_milestone_sem2", "dataType": "Timestamp" }, 
        { "name": "study_next_milestone_variance", "dataType": "Integer" }, 
        { "name": "candidate_investment_category", "dataType": "String" }, 
        { "name": "sites_activated", "dataType": "Integer" }, 
        { "name": "sites_selected", "dataType": "Integer" }, 
        { "name": "last_study_milestone", "dataType": "String" }, 
        { "name": "last_study_milestone_date", "dataType": "Timestamp" }, 
        { "name": "milestones_forecasted_12_months", "dataType": "String" }, 
        { "name": "rationale_for_mop_traffic_light", "dataType": "String" }, 
        { "name": "mop_lastsaved_by", "dataType": "String" }, 
        { "name": "country_list_selected", "dataType": "String" }, 
        { "name": "country_list_activated", "dataType": "String" }, 
        { "name": "country_list_unknown_legacy_status", "dataType": "String" }, 
        { "name": "study_min_site_activated_dt", "dataType": "Timestamp" }, 
        { "name": "study_min_derived_site_activated_dt", "dataType": "Timestamp" }, 
        { "name": "study_min_derived_site_activated_dt_source", "dataType": "String" }, 
        { "name": "poc_ss_target_bl", "dataType": "Timestamp" }, 
        { "name": "poc_target_bl", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_target_bl", "dataType": "Timestamp" }, 
        { "name": "nda_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "maa_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "nda_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "maa_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "csr_target_bl", "dataType": "Timestamp" }, 
        { "name": "dbr_target_bl", "dataType": "Timestamp" }, 
        { "name": "fsfd_target_bl", "dataType": "Timestamp" }, 
        { "name": "fsfv_target_bl", "dataType": "Timestamp" }, 
        { "name": "ft_target_bl", "dataType": "Timestamp" }, 
        { "name": "lsfv_target_bl", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_target_bl", "dataType": "Timestamp" }, 
        { "name": "lslv_target_bl", "dataType": "Timestamp" }, 
        { "name": "pcd_target_bl", "dataType": "Timestamp" }, 
        { "name": "scd_target_bl", "dataType": "Timestamp" }, 
        { "name": "scsr_target_bl", "dataType": "Timestamp" }, 
        { "name": "sdbr_target_bl", "dataType": "Timestamp" }, 
        { "name": "sites100_target_bl", "dataType": "Timestamp" }, 
        { "name": "sites50_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects25_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects50_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects75_target_bl", "dataType": "Timestamp" }, 
        { "name": "tlr_target_bl", "dataType": "Timestamp" }, 
        { "name": "ps_target_bl", "dataType": "Timestamp" }, 
        { "name": "fap_target_bl", "dataType": "Timestamp" }, 
        { "name": "study_next_milestone_target_bl", "dataType": "Timestamp" }, 
        { "name": "study_baseline_event", "dataType": "String" }, 
        { "name": "study_id", "dataType": "String" }, 
        { "name": "cohort_type", "dataType": "String" }, 
        { "name": "cohort_name", "dataType": "String" }, 
        { "name": "dvso_partial_plan_flag", "dataType": "String" }, 
        { "name": "dvso_partial_plan_desc", "dataType": "String" }, 
        { "name": "site_file_rand_count", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['rand_actuals', 'rand_plan_to_date', 'rand_plan_to_date_90d', 'rand_plan_to_date_30d', 'rand_plan'], 
      false, 
      '', 
      true, 
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

Formula_452_to_Formula_467_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date > 0)
        )
          THEN (rand_actuals / rand_plan_to_date)
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date = 0)
        )
          THEN (rand_actuals / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_to_date_variance,
    *
  
  FROM Cleanse_451 AS in0

),

Formula_452_to_Formula_467_1 AS (

  SELECT 
    CAST((1 - rand_to_date_variance) AS DOUBLE) AS rand_to_date_fraction,
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                rand_plan_src = 'Unavailable')
            ) OR (rand_plan_src IS NULL)
          )
          AND (rand_plan > 0)
        )
          THEN (rand_actuals / rand_plan)
        WHEN (
          (
            (
              NOT(
                rand_plan_src = 'Unavailable')
            ) OR (rand_plan_src IS NULL)
          )
          AND (rand_plan = 0)
        )
          THEN (rand_actuals / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_total_variance,
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date_90d > 0)
        )
          THEN (rand_actuals_90d / rand_plan_to_date_90d)
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date_90d = 0)
        )
          THEN (rand_actuals_90d / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_to_date_variance_90d,
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date_30d > 0)
        )
          THEN (rand_actuals_30d / rand_plan_to_date_90d)
        WHEN (
          (
            (
              NOT(
                rand_plan_to_date_src = 'Unavailable')
            ) OR (rand_plan_to_date_src IS NULL)
          )
          AND (rand_plan_to_date_30d = 0)
        )
          THEN (rand_actuals_30d / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS rand_to_date_variance_30d,
    *
  
  FROM Formula_452_to_Formula_467_0 AS in0

),

Formula_452_to_Formula_467_2 AS (

  SELECT 
    CAST((
      CASE
        WHEN (rand_plan_to_date_src = 'Unavailable')
          THEN 'Plan Unavailable'
        WHEN (lsfv_pct_cmp = 100)
          THEN 'Enrollment Complete'
        WHEN (
          (CAST(rand_plan_to_date AS DOUBLE) IN (CAST(0 AS DOUBLE)))
          AND (CAST(rand_actuals AS DOUBLE) IN (CAST(0 AS DOUBLE)))
        )
          THEN 'Not Started'
        WHEN (ABS(rand_to_date_fraction) >= 0.05)
          THEN (
            CASE
              WHEN (rand_to_date_fraction < 0)
                THEN 'Ahead'
              ELSE 'Behind'
            END
          )
        ELSE 'On Plan'
      END
    ) AS string) AS enrollment_indicator,
    *
  
  FROM Formula_452_to_Formula_467_1 AS in0

),

Filter_381 AS (

  SELECT * 
  
  FROM Formula_452_to_Formula_467_2 AS in0
  
  WHERE (activation_mtp_baseline_total > 0)

),

Formula_376_0 AS (

  SELECT 
    CAST(activation_mtp_baseline_to_date AS DOUBLE) AS activation_plan_to_date,
    CAST('MyTrial Baseline' AS string) AS activation_plan_to_date_src,
    *
  
  FROM Filter_381 AS in0

),

Filter_381_reject AS (

  SELECT * 
  
  FROM Formula_452_to_Formula_467_2 AS in0
  
  WHERE (
          (
            NOT(
              activation_mtp_baseline_total > 0)
          ) OR ((activation_mtp_baseline_total > 0) IS NULL)
        )

),

Formula_379_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (activation_dvso_baseline_to_date > 0)
          THEN activation_dvso_baseline_to_date
        ELSE NULL
      END
    ) AS DOUBLE) AS activation_plan_to_date,
    *
  
  FROM Filter_381_reject AS in0

),

Union_378 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_376_0', 'Formula_379_0'], 
      [
        '[{"name": "activation_plan_to_date", "dataType": "Double"}, {"name": "activation_plan_to_date_src", "dataType": "String"}, {"name": "enrollment_indicator", "dataType": "String"}, {"name": "rand_to_date_fraction", "dataType": "Double"}, {"name": "rand_total_variance", "dataType": "Double"}, {"name": "rand_to_date_variance_90d", "dataType": "Double"}, {"name": "rand_to_date_variance_30d", "dataType": "Double"}, {"name": "rand_to_date_variance", "dataType": "Double"}, {"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_src", "dataType": "String"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]', 
        '[{"name": "activation_plan_to_date", "dataType": "Double"}, {"name": "enrollment_indicator", "dataType": "String"}, {"name": "rand_to_date_fraction", "dataType": "Double"}, {"name": "rand_total_variance", "dataType": "Double"}, {"name": "rand_to_date_variance_90d", "dataType": "Double"}, {"name": "rand_to_date_variance_30d", "dataType": "Double"}, {"name": "rand_to_date_variance", "dataType": "Double"}, {"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_src", "dataType": "String"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_383_reject AS (

  SELECT * 
  
  FROM Union_378 AS in0
  
  WHERE (
          (NOT((activation_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL)))
          OR (((activation_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL)) IS NULL)
        )

),

Formula_385_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (activation_rapid_plan_total > 0)
          THEN activation_rapid_plan_total
        ELSE NULL
      END
    ) AS DOUBLE) AS activation_plan,
    *
  
  FROM Filter_383_reject AS in0

),

Filter_383 AS (

  SELECT * 
  
  FROM Union_378 AS in0
  
  WHERE ((activation_dvso_baseline_total > 0) AND (dvso_partial_plan_flag = NULL))

),

Formula_384_0 AS (

  SELECT 
    CAST(activation_dvso_baseline_total AS DOUBLE) AS activation_plan,
    CAST('DVSO Baseline' AS string) AS activation_plan_src,
    *
  
  FROM Filter_383 AS in0

),

Union_388 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_384_0', 'Formula_385_0'], 
      [
        '[{"name": "activation_plan", "dataType": "Double"}, {"name": "activation_plan_src", "dataType": "String"}, {"name": "activation_plan_to_date", "dataType": "Double"}, {"name": "activation_plan_to_date_src", "dataType": "String"}, {"name": "enrollment_indicator", "dataType": "String"}, {"name": "rand_to_date_fraction", "dataType": "Double"}, {"name": "rand_total_variance", "dataType": "Double"}, {"name": "rand_to_date_variance_90d", "dataType": "Double"}, {"name": "rand_to_date_variance_30d", "dataType": "Double"}, {"name": "rand_to_date_variance", "dataType": "Double"}, {"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_src", "dataType": "String"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]', 
        '[{"name": "activation_plan", "dataType": "Double"}, {"name": "activation_plan_to_date", "dataType": "Double"}, {"name": "activation_plan_to_date_src", "dataType": "String"}, {"name": "enrollment_indicator", "dataType": "String"}, {"name": "rand_to_date_fraction", "dataType": "Double"}, {"name": "rand_total_variance", "dataType": "Double"}, {"name": "rand_to_date_variance_90d", "dataType": "Double"}, {"name": "rand_to_date_variance_30d", "dataType": "Double"}, {"name": "rand_to_date_variance", "dataType": "Double"}, {"name": "rand_plan", "dataType": "Double"}, {"name": "rand_plan_src", "dataType": "String"}, {"name": "rand_plan_to_date", "dataType": "Double"}, {"name": "rand_plan_to_date_90d", "dataType": "Double"}, {"name": "rand_plan_to_date_30d", "dataType": "Double"}, {"name": "rand_plan_to_date_src", "dataType": "String"}, {"name": "Right_STUDY_ID", "dataType": "String"}, {"name": "protocol_id", "dataType": "String"}, {"name": "study_status", "dataType": "String"}, {"name": "study_type", "dataType": "String"}, {"name": "planned_patients", "dataType": "Double"}, {"name": "asset", "dataType": "String"}, {"name": "business_category_der", "dataType": "String"}, {"name": "priority_level", "dataType": "String"}, {"name": "candidate_priority", "dataType": "String"}, {"name": "rapid_model", "dataType": "String"}, {"name": "rand_actuals", "dataType": "Bigint"}, {"name": "activation_actuals", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_to_date", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date", "dataType": "Bigint"}, {"name": "activation_mtp_plan_to_date", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_total", "dataType": "Bigint"}, {"name": "activation_mtp_baseline_total", "dataType": "Bigint"}, {"name": "activation_dvso_baseline_total", "dataType": "Bigint"}, {"name": "rand_latest_estimate_total", "dataType": "Bigint"}, {"name": "activation_mtp_plan_total", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_total", "dataType": "Bigint"}, {"name": "amendments_prior_to_fsfv", "dataType": "Integer"}, {"name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp"}, {"name": "amendments_during_enrollment", "dataType": "Integer"}, {"name": "max_amendment_date_during_enrollment", "dataType": "Timestamp"}, {"name": "total_number_amendments", "dataType": "Integer"}, {"name": "rand_actuals_90d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint"}, {"name": "rand_actuals_30d", "dataType": "Bigint"}, {"name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint"}, {"name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint"}, {"name": "study_number_pfe", "dataType": "String"}, {"name": "activation_rapid_plan_total", "dataType": "Double"}, {"name": "fsfv_date", "dataType": "Timestamp"}, {"name": "fsfv_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_date", "dataType": "Timestamp"}, {"name": "lsfv_pct_cmp", "dataType": "Integer"}, {"name": "sites100_date", "dataType": "Timestamp"}, {"name": "sites100_pct_cmp", "dataType": "Integer"}, {"name": "study_phase_bin", "dataType": "String"}, {"name": "mop_tl_current", "dataType": "String"}, {"name": "mop_study_status", "dataType": "String"}, {"name": "cd_patient_population_age_der", "dataType": "String"}, {"name": "candidate_portfolio_priority", "dataType": "String"}, {"name": "goal_study", "dataType": "String"}, {"name": "recruitment_review_scope_flag", "dataType": "String"}, {"name": "drug_program_code", "dataType": "String"}, {"name": "candidate_code", "dataType": "String"}, {"name": "protocol_description_working", "dataType": "String"}, {"name": "protocol_description_planning", "dataType": "String"}, {"name": "study_title", "dataType": "String"}, {"name": "study_phase", "dataType": "String"}, {"name": "study_phase_planned", "dataType": "String"}, {"name": "study_sub_type", "dataType": "String"}, {"name": "methodology_study", "dataType": "String"}, {"name": "compassionate_use_study", "dataType": "String"}, {"name": "post_author_safety_study", "dataType": "String"}, {"name": "post_author_effective_study", "dataType": "String"}, {"name": "ccs_clinical_placement", "dataType": "String"}, {"name": "project_plan_type", "dataType": "String"}, {"name": "study_execution_state", "dataType": "String"}, {"name": "subject_type", "dataType": "String"}, {"name": "study_status_planning", "dataType": "String"}, {"name": "transfer_status", "dataType": "String"}, {"name": "model", "dataType": "String"}, {"name": "front_end", "dataType": "String"}, {"name": "study_termination_date", "dataType": "Timestamp"}, {"name": "back_end", "dataType": "String"}, {"name": "study_closed", "dataType": "String"}, {"name": "commercial_bu", "dataType": "String"}, {"name": "study_type_plan", "dataType": "String"}, {"name": "assigned_ctms", "dataType": "String"}, {"name": "patient_database", "dataType": "String"}, {"name": "sponsor_product", "dataType": "String"}, {"name": "business_group", "dataType": "String"}, {"name": "medical_responsibility", "dataType": "String"}, {"name": "terminate_decision_date", "dataType": "Timestamp"}, {"name": "sponsoring_division", "dataType": "String"}, {"name": "sponsoring_unit", "dataType": "String"}, {"name": "study_therapeutic_area", "dataType": "String"}, {"name": "study_design", "dataType": "String"}, {"name": "business_rationale", "dataType": "String"}, {"name": "pediatric_study", "dataType": "String"}, {"name": "primary_data_collection", "dataType": "String"}, {"name": "secondary_data_collection", "dataType": "String"}, {"name": "study_project_planner_planning", "dataType": "String"}, {"name": "funding_source", "dataType": "String"}, {"name": "subj_max_age_der", "dataType": "String"}, {"name": "subj_min_age_der", "dataType": "String"}, {"name": "study_post_reg_commitment", "dataType": "String"}, {"name": "eudract_num", "dataType": "String"}, {"name": "study_status_assessment_date", "dataType": "Timestamp"}, {"name": "study_finance_code", "dataType": "String"}, {"name": "load_ts_cdm", "dataType": "Timestamp"}, {"name": "study_alias", "dataType": "String"}, {"name": "study_nctid", "dataType": "String"}, {"name": "bl137t_date", "dataType": "Timestamp"}, {"name": "bl137t_pct_cmp", "dataType": "Integer"}, {"name": "bl137p_date", "dataType": "Timestamp"}, {"name": "bl137p_pct_cmp", "dataType": "Integer"}, {"name": "arp_finish_date", "dataType": "Timestamp"}, {"name": "arp_finish_pct_cmp", "dataType": "Integer"}, {"name": "arp_start_date", "dataType": "Timestamp"}, {"name": "bdrprep_date", "dataType": "Timestamp"}, {"name": "bdrprep_pct_cmp", "dataType": "Integer"}, {"name": "bdrprep_start_date", "dataType": "Timestamp"}, {"name": "prc_date", "dataType": "Timestamp"}, {"name": "prc_pct_cmp", "dataType": "Integer"}, {"name": "ps_date", "dataType": "Timestamp"}, {"name": "ps_pct_cmp", "dataType": "Integer"}, {"name": "ep3_date", "dataType": "Timestamp"}, {"name": "ep3_pct_cmp", "dataType": "Integer"}, {"name": "fap_date", "dataType": "Timestamp"}, {"name": "fap_pct_cmp", "dataType": "Integer"}, {"name": "fap_source", "dataType": "String"}, {"name": "fap_plw_date", "dataType": "Timestamp"}, {"name": "fap_plw_pct_cmp", "dataType": "Integer"}, {"name": "dba_date", "dataType": "Timestamp"}, {"name": "dba_pct_cmp", "dataType": "Integer"}, {"name": "siv_date", "dataType": "Timestamp"}, {"name": "siv_pct_cmp", "dataType": "Integer"}, {"name": "siv_source", "dataType": "String"}, {"name": "siv_us_date", "dataType": "Timestamp"}, {"name": "siv_us_pct_cmp", "dataType": "Integer"}, {"name": "siv_exus_date", "dataType": "Timestamp"}, {"name": "siv_exus_pct_cmp", "dataType": "Integer"}, {"name": "crfdata_date", "dataType": "Timestamp"}, {"name": "crfdata_pct_cmp", "dataType": "Integer"}, {"name": "csr_max_date", "dataType": "Timestamp"}, {"name": "csr_min_date", "dataType": "Timestamp"}, {"name": "csr_pct_cmp", "dataType": "Integer"}, {"name": "csr_plw_date", "dataType": "Timestamp"}, {"name": "csr_plw_pct_cmp", "dataType": "Integer"}, {"name": "csr_sem2", "dataType": "Timestamp"}, {"name": "csr_source", "dataType": "String"}, {"name": "dataready227t_date", "dataType": "Timestamp"}, {"name": "dataready227t_pct_cmp", "dataType": "Integer"}, {"name": "dbr_date", "dataType": "Timestamp"}, {"name": "dbr_pct_cmp", "dataType": "Integer"}, {"name": "dbr_sem2", "dataType": "Timestamp"}, {"name": "der_csr_max_date", "dataType": "Timestamp"}, {"name": "der_csr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_csr_max_source", "dataType": "String"}, {"name": "der_dbr_max_date", "dataType": "Timestamp"}, {"name": "der_dbr_max_pct_cmp", "dataType": "Integer"}, {"name": "der_dbr_max_source", "dataType": "String"}, {"name": "der_lslv_max_date", "dataType": "Timestamp"}, {"name": "der_lslv_max_pct_cmp", "dataType": "Integer"}, {"name": "der_lslv_max_source", "dataType": "String"}, {"name": "fsfd_date", "dataType": "Timestamp"}, {"name": "fsfd_pct_cmp", "dataType": "Integer"}, {"name": "fsfd_sem2", "dataType": "Timestamp"}, {"name": "fsfv_plw_date", "dataType": "Timestamp"}, {"name": "fsfv_plw_pct_cmp", "dataType": "Integer"}, {"name": "fsfv_sem2", "dataType": "Timestamp"}, {"name": "fsfv_source", "dataType": "String"}, {"name": "ft_date", "dataType": "Timestamp"}, {"name": "ft_pct_cmp", "dataType": "Integer"}, {"name": "ft_sem2", "dataType": "Timestamp"}, {"name": "labdata_date", "dataType": "Timestamp"}, {"name": "labdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_date", "dataType": "Timestamp"}, {"name": "lastdata_pct_cmp", "dataType": "Integer"}, {"name": "lastdata_source", "dataType": "String"}, {"name": "ldi_date", "dataType": "Timestamp"}, {"name": "ldi_pct_cmp", "dataType": "Integer"}, {"name": "ldis_date", "dataType": "Timestamp"}, {"name": "ldis_pct_cmp", "dataType": "Integer"}, {"name": "lsfv_sem2", "dataType": "Timestamp"}, {"name": "lslv_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_date", "dataType": "Timestamp"}, {"name": "lslv_pcd_pct_cmp", "dataType": "Integer"}, {"name": "lslv_pcd_sem2", "dataType": "Timestamp"}, {"name": "lslv_pcd_source", "dataType": "String"}, {"name": "lslv_pct_cmp", "dataType": "Integer"}, {"name": "lslv_plw_date", "dataType": "Timestamp"}, {"name": "lslv_plw_pct_cmp", "dataType": "Integer"}, {"name": "lslv_sem2", "dataType": "Timestamp"}, {"name": "lslv_source", "dataType": "String"}, {"name": "pcd_date", "dataType": "Timestamp"}, {"name": "pcd_pct_cmp", "dataType": "Integer"}, {"name": "pcd_sem2", "dataType": "Timestamp"}, {"name": "pkdata_date", "dataType": "Timestamp"}, {"name": "pkdata_pct_cmp", "dataType": "Integer"}, {"name": "scd_date", "dataType": "Timestamp"}, {"name": "scd_pct_cmp", "dataType": "Integer"}, {"name": "scd_sem2", "dataType": "Timestamp"}, {"name": "scsr_date", "dataType": "Timestamp"}, {"name": "scsr_pct_cmp", "dataType": "Integer"}, {"name": "scsr_sem2", "dataType": "Timestamp"}, {"name": "sdbr_date", "dataType": "Timestamp"}, {"name": "sdbr_pct_cmp", "dataType": "Integer"}, {"name": "sdbr_sem2", "dataType": "Timestamp"}, {"name": "serology_date", "dataType": "Timestamp"}, {"name": "serology_pct_cmp", "dataType": "Integer"}, {"name": "sites100_sem2", "dataType": "Timestamp"}, {"name": "sites50_date", "dataType": "Timestamp"}, {"name": "sites50_pct_cmp", "dataType": "Integer"}, {"name": "sites50_sem2", "dataType": "Timestamp"}, {"name": "slock353p_date", "dataType": "Timestamp"}, {"name": "slock353p_pct_cmp", "dataType": "Integer"}, {"name": "stlftlr_date", "dataType": "Timestamp"}, {"name": "stlftlr_pct_cmp", "dataType": "Integer"}, {"name": "stlr_date", "dataType": "Timestamp"}, {"name": "stlr_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_date", "dataType": "Timestamp"}, {"name": "subjects25_pct_cmp", "dataType": "Integer"}, {"name": "subjects25_sem2", "dataType": "Timestamp"}, {"name": "subjects50_date", "dataType": "Timestamp"}, {"name": "subjects50_pct_cmp", "dataType": "Integer"}, {"name": "subjects50_sem2", "dataType": "Timestamp"}, {"name": "subjects75_date", "dataType": "Timestamp"}, {"name": "subjects75_pct_cmp", "dataType": "Integer"}, {"name": "subjects75_sem2", "dataType": "Timestamp"}, {"name": "tlftlr_date", "dataType": "Timestamp"}, {"name": "tlftlr_pct_cmp", "dataType": "Integer"}, {"name": "tlftlr_start_date", "dataType": "Timestamp"}, {"name": "tlr_date", "dataType": "Timestamp"}, {"name": "tlr_pct_cmp", "dataType": "Integer"}, {"name": "tlr_sem2", "dataType": "Timestamp"}, {"name": "fih_date", "dataType": "Timestamp"}, {"name": "fih_pct", "dataType": "Integer"}, {"name": "pom_date", "dataType": "Timestamp"}, {"name": "pom_pct", "dataType": "Integer"}, {"name": "poc_ss_date", "dataType": "Timestamp"}, {"name": "poc_ss_pct", "dataType": "Integer"}, {"name": "poc_date", "dataType": "Timestamp"}, {"name": "poc_pct", "dataType": "Integer"}, {"name": "phase_3_start_date", "dataType": "Timestamp"}, {"name": "phase_3_start_pct", "dataType": "Integer"}, {"name": "nda_submission_date", "dataType": "Timestamp"}, {"name": "nda_submission_pct", "dataType": "Integer"}, {"name": "maa_submission_date", "dataType": "Timestamp"}, {"name": "maa_submission_pct", "dataType": "Integer"}, {"name": "jnda_submission_date", "dataType": "Timestamp"}, {"name": "jnda_submission_pct", "dataType": "Integer"}, {"name": "nda_approval_date", "dataType": "Timestamp"}, {"name": "nda_approval_pct", "dataType": "Integer"}, {"name": "maa_approval_date", "dataType": "Timestamp"}, {"name": "maa_approval_pct", "dataType": "Integer"}, {"name": "jnda_approval_date", "dataType": "Timestamp"}, {"name": "jnda_approval_pct", "dataType": "Integer"}, {"name": "poc_ss_gem_date", "dataType": "Timestamp"}, {"name": "poc_gem_date", "dataType": "Timestamp"}, {"name": "phase_3_start_gem_date", "dataType": "Timestamp"}, {"name": "nda_submission_gem_date", "dataType": "Timestamp"}, {"name": "maa_submission_gem_date", "dataType": "Timestamp"}, {"name": "jnda_submission_gem_date", "dataType": "Timestamp"}, {"name": "nda_approval_gem_date", "dataType": "Timestamp"}, {"name": "maa_approval_gem_date", "dataType": "Timestamp"}, {"name": "jnda_approval_gem_date", "dataType": "Timestamp"}, {"name": "manual_milestone_change", "dataType": "String"}, {"name": "multi_csr_flag", "dataType": "String"}, {"name": "study_subjects_active_reg", "dataType": "Integer"}, {"name": "study_subjects_screened_reg", "dataType": "Integer"}, {"name": "study_subjects_randomized", "dataType": "Integer"}, {"name": "study_subjects_completed_reg", "dataType": "Integer"}, {"name": "study_subjects_discontinued_reg", "dataType": "Integer"}, {"name": "study_subjects_screen_failed_reg", "dataType": "Integer"}, {"name": "retention_rate", "dataType": "Double"}, {"name": "screen_failure_rate", "dataType": "Double"}, {"name": "sites_completed", "dataType": "Integer"}, {"name": "sites_cancelled", "dataType": "Integer"}, {"name": "sites_active", "dataType": "Integer"}, {"name": "sites_proposed", "dataType": "Integer"}, {"name": "sites_planned", "dataType": "Integer"}, {"name": "sites_total_der", "dataType": "Integer"}, {"name": "product_der", "dataType": "String"}, {"name": "business_category", "dataType": "String"}, {"name": "study_status_indicator", "dataType": "String"}, {"name": "study_phase_lifecycle", "dataType": "String"}, {"name": "lead_clinician", "dataType": "String"}, {"name": "study_point_of_contact", "dataType": "String"}, {"name": "extension_study", "dataType": "String"}, {"name": "internalized_study", "dataType": "String"}, {"name": "indication_preferred_term_list", "dataType": "String"}, {"name": "acquisition_date", "dataType": "Timestamp"}, {"name": "study_end_date_source", "dataType": "String"}, {"name": "partner_binned", "dataType": "String"}, {"name": "partner_allocated", "dataType": "String"}, {"name": "msa_vendor_name", "dataType": "String"}, {"name": "tier1_provider", "dataType": "String"}, {"name": "opco_agmt_min_date", "dataType": "Timestamp"}, {"name": "opco_agmt_max_date", "dataType": "Timestamp"}, {"name": "opco_study_start", "dataType": "String"}, {"name": "cost_division", "dataType": "String"}, {"name": "study_next_milestone", "dataType": "String"}, {"name": "study_next_milestone_date", "dataType": "Timestamp"}, {"name": "gov_tracked_asset", "dataType": "String"}, {"name": "goal_detail_list", "dataType": "String"}, {"name": "bic_scope", "dataType": "String"}, {"name": "bic_scope_working", "dataType": "String"}, {"name": "mop_tl_previous", "dataType": "String"}, {"name": "mop_lastsaved_dt", "dataType": "Timestamp"}, {"name": "dvso_approved_plan", "dataType": "String"}, {"name": "candidate_division", "dataType": "String"}, {"name": "candidate_status", "dataType": "String"}, {"name": "candidate_sub_division", "dataType": "String"}, {"name": "candidate_sub_unit", "dataType": "String"}, {"name": "compound_type_der", "dataType": "String"}, {"name": "mechanism_of_action", "dataType": "String"}, {"name": "pacd", "dataType": "String"}, {"name": "cd_subj_min_age_yr_der", "dataType": "Double"}, {"name": "cd_subj_max_age_yr_der", "dataType": "Double"}, {"name": "critical_flag", "dataType": "String"}, {"name": "critical_flag_detail", "dataType": "String"}, {"name": "recruitment_review_meeting", "dataType": "String"}, {"name": "study_goal", "dataType": "String"}, {"name": "ssr_study", "dataType": "String"}, {"name": "cd_primary_exclude_der", "dataType": "String"}, {"name": "cd_trial_type_final_der", "dataType": "String"}, {"name": "cd_clinpharm_study_der", "dataType": "String"}, {"name": "cd_dev_japan_study_flag", "dataType": "String"}, {"name": "cd_dev_china_study_flag", "dataType": "String"}, {"name": "cd_pcru_study_flag", "dataType": "String"}, {"name": "cd_concat_pcru_named_site_flag", "dataType": "String"}, {"name": "cd_pcrc_study_flag", "dataType": "String"}, {"name": "cd_concat_pcrc_named_site_flag", "dataType": "String"}, {"name": "override_summary_list", "dataType": "String"}, {"name": "country_list_terminated", "dataType": "String"}, {"name": "planned_country_list", "dataType": "String"}, {"name": "country_list_active", "dataType": "String"}, {"name": "country_list_cancelled", "dataType": "String"}, {"name": "country_list_completed", "dataType": "String"}, {"name": "country_list_planned", "dataType": "String"}, {"name": "country_list_proposed", "dataType": "String"}, {"name": "candidate_disease_area", "dataType": "String"}, {"name": "candidate_finance_code", "dataType": "String"}, {"name": "candidate_phase", "dataType": "String"}, {"name": "candidate_therapeutic_area", "dataType": "String"}, {"name": "candidate_type", "dataType": "String"}, {"name": "primary_indication_list", "dataType": "String"}, {"name": "compound_source", "dataType": "String"}, {"name": "compound_number", "dataType": "String"}, {"name": "compound_type", "dataType": "String"}, {"name": "compound_acquired_company_name", "dataType": "String"}, {"name": "compound_type_binned", "dataType": "String"}, {"name": "snapshot_date", "dataType": "Timestamp"}, {"name": "study_end_date", "dataType": "Timestamp"}, {"name": "mytrial_model", "dataType": "String"}, {"name": "sites_terminated", "dataType": "Integer"}, {"name": "lslb_pct_cmp", "dataType": "Integer"}, {"name": "lslb_date", "dataType": "Timestamp"}, {"name": "ps_sem2", "dataType": "Timestamp"}, {"name": "fap_sem2", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_date", "dataType": "Timestamp"}, {"name": "lslv_nda_submission_pct", "dataType": "Integer"}, {"name": "lslv_maa_submission_date", "dataType": "Timestamp"}, {"name": "lslv_maa_submission_pct", "dataType": "Integer"}, {"name": "study_next_milestone_sem2", "dataType": "Timestamp"}, {"name": "study_next_milestone_variance", "dataType": "Integer"}, {"name": "candidate_investment_category", "dataType": "String"}, {"name": "sites_activated", "dataType": "Integer"}, {"name": "sites_selected", "dataType": "Integer"}, {"name": "last_study_milestone", "dataType": "String"}, {"name": "last_study_milestone_date", "dataType": "Timestamp"}, {"name": "milestones_forecasted_12_months", "dataType": "String"}, {"name": "rationale_for_mop_traffic_light", "dataType": "String"}, {"name": "mop_lastsaved_by", "dataType": "String"}, {"name": "country_list_selected", "dataType": "String"}, {"name": "country_list_activated", "dataType": "String"}, {"name": "country_list_unknown_legacy_status", "dataType": "String"}, {"name": "study_min_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt", "dataType": "Timestamp"}, {"name": "study_min_derived_site_activated_dt_source", "dataType": "String"}, {"name": "poc_ss_target_bl", "dataType": "Timestamp"}, {"name": "poc_target_bl", "dataType": "Timestamp"}, {"name": "phase_3_start_target_bl", "dataType": "Timestamp"}, {"name": "nda_submission_target_bl", "dataType": "Timestamp"}, {"name": "maa_submission_target_bl", "dataType": "Timestamp"}, {"name": "jnda_submission_target_bl", "dataType": "Timestamp"}, {"name": "nda_approval_target_bl", "dataType": "Timestamp"}, {"name": "maa_approval_target_bl", "dataType": "Timestamp"}, {"name": "jnda_approval_target_bl", "dataType": "Timestamp"}, {"name": "csr_target_bl", "dataType": "Timestamp"}, {"name": "dbr_target_bl", "dataType": "Timestamp"}, {"name": "fsfd_target_bl", "dataType": "Timestamp"}, {"name": "fsfv_target_bl", "dataType": "Timestamp"}, {"name": "ft_target_bl", "dataType": "Timestamp"}, {"name": "lsfv_target_bl", "dataType": "Timestamp"}, {"name": "lslv_pcd_target_bl", "dataType": "Timestamp"}, {"name": "lslv_target_bl", "dataType": "Timestamp"}, {"name": "pcd_target_bl", "dataType": "Timestamp"}, {"name": "scd_target_bl", "dataType": "Timestamp"}, {"name": "scsr_target_bl", "dataType": "Timestamp"}, {"name": "sdbr_target_bl", "dataType": "Timestamp"}, {"name": "sites100_target_bl", "dataType": "Timestamp"}, {"name": "sites50_target_bl", "dataType": "Timestamp"}, {"name": "subjects25_target_bl", "dataType": "Timestamp"}, {"name": "subjects50_target_bl", "dataType": "Timestamp"}, {"name": "subjects75_target_bl", "dataType": "Timestamp"}, {"name": "tlr_target_bl", "dataType": "Timestamp"}, {"name": "ps_target_bl", "dataType": "Timestamp"}, {"name": "fap_target_bl", "dataType": "Timestamp"}, {"name": "study_next_milestone_target_bl", "dataType": "Timestamp"}, {"name": "study_baseline_event", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "cohort_type", "dataType": "String"}, {"name": "cohort_name", "dataType": "String"}, {"name": "dvso_partial_plan_flag", "dataType": "String"}, {"name": "dvso_partial_plan_desc", "dataType": "String"}, {"name": "site_file_rand_count", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Cleanse_373 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_388'], 
      [
        { "name": "activation_plan", "dataType": "Double" }, 
        { "name": "activation_plan_src", "dataType": "String" }, 
        { "name": "activation_plan_to_date", "dataType": "Double" }, 
        { "name": "activation_plan_to_date_src", "dataType": "String" }, 
        { "name": "enrollment_indicator", "dataType": "String" }, 
        { "name": "rand_to_date_fraction", "dataType": "Double" }, 
        { "name": "rand_total_variance", "dataType": "Double" }, 
        { "name": "rand_to_date_variance_90d", "dataType": "Double" }, 
        { "name": "rand_to_date_variance_30d", "dataType": "Double" }, 
        { "name": "rand_to_date_variance", "dataType": "Double" }, 
        { "name": "rand_plan", "dataType": "Double" }, 
        { "name": "rand_plan_src", "dataType": "String" }, 
        { "name": "rand_plan_to_date", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_90d", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_30d", "dataType": "Double" }, 
        { "name": "rand_plan_to_date_src", "dataType": "String" }, 
        { "name": "Right_STUDY_ID", "dataType": "String" }, 
        { "name": "protocol_id", "dataType": "String" }, 
        { "name": "study_status", "dataType": "String" }, 
        { "name": "study_type", "dataType": "String" }, 
        { "name": "planned_patients", "dataType": "Double" }, 
        { "name": "asset", "dataType": "String" }, 
        { "name": "business_category_der", "dataType": "String" }, 
        { "name": "priority_level", "dataType": "String" }, 
        { "name": "candidate_priority", "dataType": "String" }, 
        { "name": "rapid_model", "dataType": "String" }, 
        { "name": "rand_actuals", "dataType": "Bigint" }, 
        { "name": "activation_actuals", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_to_date", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_baseline_total", "dataType": "Bigint" }, 
        { "name": "activation_dvso_baseline_total", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_total", "dataType": "Bigint" }, 
        { "name": "activation_mtp_plan_total", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_total", "dataType": "Bigint" }, 
        { "name": "amendments_prior_to_fsfv", "dataType": "Integer" }, 
        { "name": "max_amendment_date_prior_to_fsfv", "dataType": "Timestamp" }, 
        { "name": "amendments_during_enrollment", "dataType": "Integer" }, 
        { "name": "max_amendment_date_during_enrollment", "dataType": "Timestamp" }, 
        { "name": "total_number_amendments", "dataType": "Integer" }, 
        { "name": "rand_actuals_90d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_90d", "dataType": "Bigint" }, 
        { "name": "rand_actuals_30d", "dataType": "Bigint" }, 
        { "name": "rand_dvso_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_latest_estimate_to_date_30d", "dataType": "Bigint" }, 
        { "name": "rand_rapid_baseline_to_date_30d", "dataType": "Bigint" }, 
        { "name": "study_number_pfe", "dataType": "String" }, 
        { "name": "activation_rapid_plan_total", "dataType": "Double" }, 
        { "name": "fsfv_date", "dataType": "Timestamp" }, 
        { "name": "fsfv_pct_cmp", "dataType": "Integer" }, 
        { "name": "lsfv_date", "dataType": "Timestamp" }, 
        { "name": "lsfv_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites100_date", "dataType": "Timestamp" }, 
        { "name": "sites100_pct_cmp", "dataType": "Integer" }, 
        { "name": "study_phase_bin", "dataType": "String" }, 
        { "name": "mop_tl_current", "dataType": "String" }, 
        { "name": "mop_study_status", "dataType": "String" }, 
        { "name": "cd_patient_population_age_der", "dataType": "String" }, 
        { "name": "candidate_portfolio_priority", "dataType": "String" }, 
        { "name": "goal_study", "dataType": "String" }, 
        { "name": "recruitment_review_scope_flag", "dataType": "String" }, 
        { "name": "drug_program_code", "dataType": "String" }, 
        { "name": "candidate_code", "dataType": "String" }, 
        { "name": "protocol_description_working", "dataType": "String" }, 
        { "name": "protocol_description_planning", "dataType": "String" }, 
        { "name": "study_title", "dataType": "String" }, 
        { "name": "study_phase", "dataType": "String" }, 
        { "name": "study_phase_planned", "dataType": "String" }, 
        { "name": "study_sub_type", "dataType": "String" }, 
        { "name": "methodology_study", "dataType": "String" }, 
        { "name": "compassionate_use_study", "dataType": "String" }, 
        { "name": "post_author_safety_study", "dataType": "String" }, 
        { "name": "post_author_effective_study", "dataType": "String" }, 
        { "name": "ccs_clinical_placement", "dataType": "String" }, 
        { "name": "project_plan_type", "dataType": "String" }, 
        { "name": "study_execution_state", "dataType": "String" }, 
        { "name": "subject_type", "dataType": "String" }, 
        { "name": "study_status_planning", "dataType": "String" }, 
        { "name": "transfer_status", "dataType": "String" }, 
        { "name": "model", "dataType": "String" }, 
        { "name": "front_end", "dataType": "String" }, 
        { "name": "study_termination_date", "dataType": "Timestamp" }, 
        { "name": "back_end", "dataType": "String" }, 
        { "name": "study_closed", "dataType": "String" }, 
        { "name": "commercial_bu", "dataType": "String" }, 
        { "name": "study_type_plan", "dataType": "String" }, 
        { "name": "assigned_ctms", "dataType": "String" }, 
        { "name": "patient_database", "dataType": "String" }, 
        { "name": "sponsor_product", "dataType": "String" }, 
        { "name": "business_group", "dataType": "String" }, 
        { "name": "medical_responsibility", "dataType": "String" }, 
        { "name": "terminate_decision_date", "dataType": "Timestamp" }, 
        { "name": "sponsoring_division", "dataType": "String" }, 
        { "name": "sponsoring_unit", "dataType": "String" }, 
        { "name": "study_therapeutic_area", "dataType": "String" }, 
        { "name": "study_design", "dataType": "String" }, 
        { "name": "business_rationale", "dataType": "String" }, 
        { "name": "pediatric_study", "dataType": "String" }, 
        { "name": "primary_data_collection", "dataType": "String" }, 
        { "name": "secondary_data_collection", "dataType": "String" }, 
        { "name": "study_project_planner_planning", "dataType": "String" }, 
        { "name": "funding_source", "dataType": "String" }, 
        { "name": "subj_max_age_der", "dataType": "String" }, 
        { "name": "subj_min_age_der", "dataType": "String" }, 
        { "name": "study_post_reg_commitment", "dataType": "String" }, 
        { "name": "eudract_num", "dataType": "String" }, 
        { "name": "study_status_assessment_date", "dataType": "Timestamp" }, 
        { "name": "study_finance_code", "dataType": "String" }, 
        { "name": "load_ts_cdm", "dataType": "Timestamp" }, 
        { "name": "study_alias", "dataType": "String" }, 
        { "name": "study_nctid", "dataType": "String" }, 
        { "name": "bl137t_date", "dataType": "Timestamp" }, 
        { "name": "bl137t_pct_cmp", "dataType": "Integer" }, 
        { "name": "bl137p_date", "dataType": "Timestamp" }, 
        { "name": "bl137p_pct_cmp", "dataType": "Integer" }, 
        { "name": "arp_finish_date", "dataType": "Timestamp" }, 
        { "name": "arp_finish_pct_cmp", "dataType": "Integer" }, 
        { "name": "arp_start_date", "dataType": "Timestamp" }, 
        { "name": "bdrprep_date", "dataType": "Timestamp" }, 
        { "name": "bdrprep_pct_cmp", "dataType": "Integer" }, 
        { "name": "bdrprep_start_date", "dataType": "Timestamp" }, 
        { "name": "prc_date", "dataType": "Timestamp" }, 
        { "name": "prc_pct_cmp", "dataType": "Integer" }, 
        { "name": "ps_date", "dataType": "Timestamp" }, 
        { "name": "ps_pct_cmp", "dataType": "Integer" }, 
        { "name": "ep3_date", "dataType": "Timestamp" }, 
        { "name": "ep3_pct_cmp", "dataType": "Integer" }, 
        { "name": "fap_date", "dataType": "Timestamp" }, 
        { "name": "fap_pct_cmp", "dataType": "Integer" }, 
        { "name": "fap_source", "dataType": "String" }, 
        { "name": "fap_plw_date", "dataType": "Timestamp" }, 
        { "name": "fap_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "dba_date", "dataType": "Timestamp" }, 
        { "name": "dba_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_date", "dataType": "Timestamp" }, 
        { "name": "siv_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_source", "dataType": "String" }, 
        { "name": "siv_us_date", "dataType": "Timestamp" }, 
        { "name": "siv_us_pct_cmp", "dataType": "Integer" }, 
        { "name": "siv_exus_date", "dataType": "Timestamp" }, 
        { "name": "siv_exus_pct_cmp", "dataType": "Integer" }, 
        { "name": "crfdata_date", "dataType": "Timestamp" }, 
        { "name": "crfdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_max_date", "dataType": "Timestamp" }, 
        { "name": "csr_min_date", "dataType": "Timestamp" }, 
        { "name": "csr_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_plw_date", "dataType": "Timestamp" }, 
        { "name": "csr_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "csr_sem2", "dataType": "Timestamp" }, 
        { "name": "csr_source", "dataType": "String" }, 
        { "name": "dataready227t_date", "dataType": "Timestamp" }, 
        { "name": "dataready227t_pct_cmp", "dataType": "Integer" }, 
        { "name": "dbr_date", "dataType": "Timestamp" }, 
        { "name": "dbr_pct_cmp", "dataType": "Integer" }, 
        { "name": "dbr_sem2", "dataType": "Timestamp" }, 
        { "name": "der_csr_max_date", "dataType": "Timestamp" }, 
        { "name": "der_csr_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_csr_max_source", "dataType": "String" }, 
        { "name": "der_dbr_max_date", "dataType": "Timestamp" }, 
        { "name": "der_dbr_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_dbr_max_source", "dataType": "String" }, 
        { "name": "der_lslv_max_date", "dataType": "Timestamp" }, 
        { "name": "der_lslv_max_pct_cmp", "dataType": "Integer" }, 
        { "name": "der_lslv_max_source", "dataType": "String" }, 
        { "name": "fsfd_date", "dataType": "Timestamp" }, 
        { "name": "fsfd_pct_cmp", "dataType": "Integer" }, 
        { "name": "fsfd_sem2", "dataType": "Timestamp" }, 
        { "name": "fsfv_plw_date", "dataType": "Timestamp" }, 
        { "name": "fsfv_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "fsfv_sem2", "dataType": "Timestamp" }, 
        { "name": "fsfv_source", "dataType": "String" }, 
        { "name": "ft_date", "dataType": "Timestamp" }, 
        { "name": "ft_pct_cmp", "dataType": "Integer" }, 
        { "name": "ft_sem2", "dataType": "Timestamp" }, 
        { "name": "labdata_date", "dataType": "Timestamp" }, 
        { "name": "labdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "lastdata_date", "dataType": "Timestamp" }, 
        { "name": "lastdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "lastdata_source", "dataType": "String" }, 
        { "name": "ldi_date", "dataType": "Timestamp" }, 
        { "name": "ldi_pct_cmp", "dataType": "Integer" }, 
        { "name": "ldis_date", "dataType": "Timestamp" }, 
        { "name": "ldis_pct_cmp", "dataType": "Integer" }, 
        { "name": "lsfv_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_date", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_date", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_pcd_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_source", "dataType": "String" }, 
        { "name": "lslv_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_plw_date", "dataType": "Timestamp" }, 
        { "name": "lslv_plw_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslv_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_source", "dataType": "String" }, 
        { "name": "pcd_date", "dataType": "Timestamp" }, 
        { "name": "pcd_pct_cmp", "dataType": "Integer" }, 
        { "name": "pcd_sem2", "dataType": "Timestamp" }, 
        { "name": "pkdata_date", "dataType": "Timestamp" }, 
        { "name": "pkdata_pct_cmp", "dataType": "Integer" }, 
        { "name": "scd_date", "dataType": "Timestamp" }, 
        { "name": "scd_pct_cmp", "dataType": "Integer" }, 
        { "name": "scd_sem2", "dataType": "Timestamp" }, 
        { "name": "scsr_date", "dataType": "Timestamp" }, 
        { "name": "scsr_pct_cmp", "dataType": "Integer" }, 
        { "name": "scsr_sem2", "dataType": "Timestamp" }, 
        { "name": "sdbr_date", "dataType": "Timestamp" }, 
        { "name": "sdbr_pct_cmp", "dataType": "Integer" }, 
        { "name": "sdbr_sem2", "dataType": "Timestamp" }, 
        { "name": "serology_date", "dataType": "Timestamp" }, 
        { "name": "serology_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites100_sem2", "dataType": "Timestamp" }, 
        { "name": "sites50_date", "dataType": "Timestamp" }, 
        { "name": "sites50_pct_cmp", "dataType": "Integer" }, 
        { "name": "sites50_sem2", "dataType": "Timestamp" }, 
        { "name": "slock353p_date", "dataType": "Timestamp" }, 
        { "name": "slock353p_pct_cmp", "dataType": "Integer" }, 
        { "name": "stlftlr_date", "dataType": "Timestamp" }, 
        { "name": "stlftlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "stlr_date", "dataType": "Timestamp" }, 
        { "name": "stlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects25_date", "dataType": "Timestamp" }, 
        { "name": "subjects25_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects25_sem2", "dataType": "Timestamp" }, 
        { "name": "subjects50_date", "dataType": "Timestamp" }, 
        { "name": "subjects50_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects50_sem2", "dataType": "Timestamp" }, 
        { "name": "subjects75_date", "dataType": "Timestamp" }, 
        { "name": "subjects75_pct_cmp", "dataType": "Integer" }, 
        { "name": "subjects75_sem2", "dataType": "Timestamp" }, 
        { "name": "tlftlr_date", "dataType": "Timestamp" }, 
        { "name": "tlftlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "tlftlr_start_date", "dataType": "Timestamp" }, 
        { "name": "tlr_date", "dataType": "Timestamp" }, 
        { "name": "tlr_pct_cmp", "dataType": "Integer" }, 
        { "name": "tlr_sem2", "dataType": "Timestamp" }, 
        { "name": "fih_date", "dataType": "Timestamp" }, 
        { "name": "fih_pct", "dataType": "Integer" }, 
        { "name": "pom_date", "dataType": "Timestamp" }, 
        { "name": "pom_pct", "dataType": "Integer" }, 
        { "name": "poc_ss_date", "dataType": "Timestamp" }, 
        { "name": "poc_ss_pct", "dataType": "Integer" }, 
        { "name": "poc_date", "dataType": "Timestamp" }, 
        { "name": "poc_pct", "dataType": "Integer" }, 
        { "name": "phase_3_start_date", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_pct", "dataType": "Integer" }, 
        { "name": "nda_submission_date", "dataType": "Timestamp" }, 
        { "name": "nda_submission_pct", "dataType": "Integer" }, 
        { "name": "maa_submission_date", "dataType": "Timestamp" }, 
        { "name": "maa_submission_pct", "dataType": "Integer" }, 
        { "name": "jnda_submission_date", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_pct", "dataType": "Integer" }, 
        { "name": "nda_approval_date", "dataType": "Timestamp" }, 
        { "name": "nda_approval_pct", "dataType": "Integer" }, 
        { "name": "maa_approval_date", "dataType": "Timestamp" }, 
        { "name": "maa_approval_pct", "dataType": "Integer" }, 
        { "name": "jnda_approval_date", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_pct", "dataType": "Integer" }, 
        { "name": "poc_ss_gem_date", "dataType": "Timestamp" }, 
        { "name": "poc_gem_date", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_gem_date", "dataType": "Timestamp" }, 
        { "name": "nda_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "maa_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_gem_date", "dataType": "Timestamp" }, 
        { "name": "nda_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "maa_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_gem_date", "dataType": "Timestamp" }, 
        { "name": "manual_milestone_change", "dataType": "String" }, 
        { "name": "multi_csr_flag", "dataType": "String" }, 
        { "name": "study_subjects_active_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_screened_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_randomized", "dataType": "Integer" }, 
        { "name": "study_subjects_completed_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_discontinued_reg", "dataType": "Integer" }, 
        { "name": "study_subjects_screen_failed_reg", "dataType": "Integer" }, 
        { "name": "retention_rate", "dataType": "Double" }, 
        { "name": "screen_failure_rate", "dataType": "Double" }, 
        { "name": "sites_completed", "dataType": "Integer" }, 
        { "name": "sites_cancelled", "dataType": "Integer" }, 
        { "name": "sites_active", "dataType": "Integer" }, 
        { "name": "sites_proposed", "dataType": "Integer" }, 
        { "name": "sites_planned", "dataType": "Integer" }, 
        { "name": "sites_total_der", "dataType": "Integer" }, 
        { "name": "product_der", "dataType": "String" }, 
        { "name": "business_category", "dataType": "String" }, 
        { "name": "study_status_indicator", "dataType": "String" }, 
        { "name": "study_phase_lifecycle", "dataType": "String" }, 
        { "name": "lead_clinician", "dataType": "String" }, 
        { "name": "study_point_of_contact", "dataType": "String" }, 
        { "name": "extension_study", "dataType": "String" }, 
        { "name": "internalized_study", "dataType": "String" }, 
        { "name": "indication_preferred_term_list", "dataType": "String" }, 
        { "name": "acquisition_date", "dataType": "Timestamp" }, 
        { "name": "study_end_date_source", "dataType": "String" }, 
        { "name": "partner_binned", "dataType": "String" }, 
        { "name": "partner_allocated", "dataType": "String" }, 
        { "name": "msa_vendor_name", "dataType": "String" }, 
        { "name": "tier1_provider", "dataType": "String" }, 
        { "name": "opco_agmt_min_date", "dataType": "Timestamp" }, 
        { "name": "opco_agmt_max_date", "dataType": "Timestamp" }, 
        { "name": "opco_study_start", "dataType": "String" }, 
        { "name": "cost_division", "dataType": "String" }, 
        { "name": "study_next_milestone", "dataType": "String" }, 
        { "name": "study_next_milestone_date", "dataType": "Timestamp" }, 
        { "name": "gov_tracked_asset", "dataType": "String" }, 
        { "name": "goal_detail_list", "dataType": "String" }, 
        { "name": "bic_scope", "dataType": "String" }, 
        { "name": "bic_scope_working", "dataType": "String" }, 
        { "name": "mop_tl_previous", "dataType": "String" }, 
        { "name": "mop_lastsaved_dt", "dataType": "Timestamp" }, 
        { "name": "dvso_approved_plan", "dataType": "String" }, 
        { "name": "candidate_division", "dataType": "String" }, 
        { "name": "candidate_status", "dataType": "String" }, 
        { "name": "candidate_sub_division", "dataType": "String" }, 
        { "name": "candidate_sub_unit", "dataType": "String" }, 
        { "name": "compound_type_der", "dataType": "String" }, 
        { "name": "mechanism_of_action", "dataType": "String" }, 
        { "name": "pacd", "dataType": "String" }, 
        { "name": "cd_subj_min_age_yr_der", "dataType": "Double" }, 
        { "name": "cd_subj_max_age_yr_der", "dataType": "Double" }, 
        { "name": "critical_flag", "dataType": "String" }, 
        { "name": "critical_flag_detail", "dataType": "String" }, 
        { "name": "recruitment_review_meeting", "dataType": "String" }, 
        { "name": "study_goal", "dataType": "String" }, 
        { "name": "ssr_study", "dataType": "String" }, 
        { "name": "cd_primary_exclude_der", "dataType": "String" }, 
        { "name": "cd_trial_type_final_der", "dataType": "String" }, 
        { "name": "cd_clinpharm_study_der", "dataType": "String" }, 
        { "name": "cd_dev_japan_study_flag", "dataType": "String" }, 
        { "name": "cd_dev_china_study_flag", "dataType": "String" }, 
        { "name": "cd_pcru_study_flag", "dataType": "String" }, 
        { "name": "cd_concat_pcru_named_site_flag", "dataType": "String" }, 
        { "name": "cd_pcrc_study_flag", "dataType": "String" }, 
        { "name": "cd_concat_pcrc_named_site_flag", "dataType": "String" }, 
        { "name": "override_summary_list", "dataType": "String" }, 
        { "name": "country_list_terminated", "dataType": "String" }, 
        { "name": "planned_country_list", "dataType": "String" }, 
        { "name": "country_list_active", "dataType": "String" }, 
        { "name": "country_list_cancelled", "dataType": "String" }, 
        { "name": "country_list_completed", "dataType": "String" }, 
        { "name": "country_list_planned", "dataType": "String" }, 
        { "name": "country_list_proposed", "dataType": "String" }, 
        { "name": "candidate_disease_area", "dataType": "String" }, 
        { "name": "candidate_finance_code", "dataType": "String" }, 
        { "name": "candidate_phase", "dataType": "String" }, 
        { "name": "candidate_therapeutic_area", "dataType": "String" }, 
        { "name": "candidate_type", "dataType": "String" }, 
        { "name": "primary_indication_list", "dataType": "String" }, 
        { "name": "compound_source", "dataType": "String" }, 
        { "name": "compound_number", "dataType": "String" }, 
        { "name": "compound_type", "dataType": "String" }, 
        { "name": "compound_acquired_company_name", "dataType": "String" }, 
        { "name": "compound_type_binned", "dataType": "String" }, 
        { "name": "snapshot_date", "dataType": "Timestamp" }, 
        { "name": "study_end_date", "dataType": "Timestamp" }, 
        { "name": "mytrial_model", "dataType": "String" }, 
        { "name": "sites_terminated", "dataType": "Integer" }, 
        { "name": "lslb_pct_cmp", "dataType": "Integer" }, 
        { "name": "lslb_date", "dataType": "Timestamp" }, 
        { "name": "ps_sem2", "dataType": "Timestamp" }, 
        { "name": "fap_sem2", "dataType": "Timestamp" }, 
        { "name": "lslv_nda_submission_date", "dataType": "Timestamp" }, 
        { "name": "lslv_nda_submission_pct", "dataType": "Integer" }, 
        { "name": "lslv_maa_submission_date", "dataType": "Timestamp" }, 
        { "name": "lslv_maa_submission_pct", "dataType": "Integer" }, 
        { "name": "study_next_milestone_sem2", "dataType": "Timestamp" }, 
        { "name": "study_next_milestone_variance", "dataType": "Integer" }, 
        { "name": "candidate_investment_category", "dataType": "String" }, 
        { "name": "sites_activated", "dataType": "Integer" }, 
        { "name": "sites_selected", "dataType": "Integer" }, 
        { "name": "last_study_milestone", "dataType": "String" }, 
        { "name": "last_study_milestone_date", "dataType": "Timestamp" }, 
        { "name": "milestones_forecasted_12_months", "dataType": "String" }, 
        { "name": "rationale_for_mop_traffic_light", "dataType": "String" }, 
        { "name": "mop_lastsaved_by", "dataType": "String" }, 
        { "name": "country_list_selected", "dataType": "String" }, 
        { "name": "country_list_activated", "dataType": "String" }, 
        { "name": "country_list_unknown_legacy_status", "dataType": "String" }, 
        { "name": "study_min_site_activated_dt", "dataType": "Timestamp" }, 
        { "name": "study_min_derived_site_activated_dt", "dataType": "Timestamp" }, 
        { "name": "study_min_derived_site_activated_dt_source", "dataType": "String" }, 
        { "name": "poc_ss_target_bl", "dataType": "Timestamp" }, 
        { "name": "poc_target_bl", "dataType": "Timestamp" }, 
        { "name": "phase_3_start_target_bl", "dataType": "Timestamp" }, 
        { "name": "nda_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "maa_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "jnda_submission_target_bl", "dataType": "Timestamp" }, 
        { "name": "nda_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "maa_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "jnda_approval_target_bl", "dataType": "Timestamp" }, 
        { "name": "csr_target_bl", "dataType": "Timestamp" }, 
        { "name": "dbr_target_bl", "dataType": "Timestamp" }, 
        { "name": "fsfd_target_bl", "dataType": "Timestamp" }, 
        { "name": "fsfv_target_bl", "dataType": "Timestamp" }, 
        { "name": "ft_target_bl", "dataType": "Timestamp" }, 
        { "name": "lsfv_target_bl", "dataType": "Timestamp" }, 
        { "name": "lslv_pcd_target_bl", "dataType": "Timestamp" }, 
        { "name": "lslv_target_bl", "dataType": "Timestamp" }, 
        { "name": "pcd_target_bl", "dataType": "Timestamp" }, 
        { "name": "scd_target_bl", "dataType": "Timestamp" }, 
        { "name": "scsr_target_bl", "dataType": "Timestamp" }, 
        { "name": "sdbr_target_bl", "dataType": "Timestamp" }, 
        { "name": "sites100_target_bl", "dataType": "Timestamp" }, 
        { "name": "sites50_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects25_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects50_target_bl", "dataType": "Timestamp" }, 
        { "name": "subjects75_target_bl", "dataType": "Timestamp" }, 
        { "name": "tlr_target_bl", "dataType": "Timestamp" }, 
        { "name": "ps_target_bl", "dataType": "Timestamp" }, 
        { "name": "fap_target_bl", "dataType": "Timestamp" }, 
        { "name": "study_next_milestone_target_bl", "dataType": "Timestamp" }, 
        { "name": "study_baseline_event", "dataType": "String" }, 
        { "name": "study_id", "dataType": "String" }, 
        { "name": "cohort_type", "dataType": "String" }, 
        { "name": "cohort_name", "dataType": "String" }, 
        { "name": "dvso_partial_plan_flag", "dataType": "String" }, 
        { "name": "dvso_partial_plan_desc", "dataType": "String" }, 
        { "name": "site_file_rand_count", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['activation_actuals', 'activation_plan_to_date', 'activation_plan'], 
      false, 
      '', 
      true, 
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

Formula_374_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                activation_plan_to_date_src = 'Unavailable')
            ) OR (activation_plan_to_date_src IS NULL)
          )
          AND (activation_plan_to_date > 0)
        )
          THEN (activation_actuals / activation_plan_to_date)
        WHEN (
          (
            (
              NOT(
                activation_plan_to_date_src = 'Unavailable')
            ) OR (activation_plan_to_date_src IS NULL)
          )
          AND (activation_plan_to_date = 0)
        )
          THEN (activation_actuals / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS activation_to_date_variance,
    *
  
  FROM Cleanse_373 AS in0

),

Formula_374_1 AS (

  SELECT 
    CAST((1 - activation_to_date_variance) AS DOUBLE) AS activation_to_date_fraction,
    CAST((
      CASE
        WHEN (
          (
            (
              NOT(
                activation_plan_src = 'Unavailable')
            ) OR (activation_plan_src IS NULL)
          )
          AND (activation_plan > 0)
        )
          THEN (activation_actuals / activation_plan)
        WHEN (
          (
            (
              NOT(
                activation_plan_src = 'Unavailable')
            ) OR (activation_plan_src IS NULL)
          )
          AND (activation_plan = 0)
        )
          THEN (activation_actuals / 1)
        ELSE NULL
      END
    ) AS DOUBLE) AS activation_total_variance,
    *
  
  FROM Formula_374_0 AS in0

)

SELECT *

FROM Formula_374_1
