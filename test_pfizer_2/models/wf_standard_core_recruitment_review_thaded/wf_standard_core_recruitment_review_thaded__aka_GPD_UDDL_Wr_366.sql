{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_366",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH aka_GPDIP_EDLUD_506 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_GPDIP_EDLUD_506') }}

),

AlteryxSelect_507 AS (

  SELECT 
    study_number_pfe AS study_number_pfe,
    study_id AS study_id,
    cd_diversity_tower_inc_exc_details AS cd_diversity_tower_inc_exc_details,
    cd_diversity_tower_inc_exc_logic AS cd_diversity_tower_inc_exc_logic
  
  FROM aka_GPDIP_EDLUD_506 AS in0

),

aka_test_oracle_536 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_536') }}

),

Formula_539_0 AS (

  SELECT 
    CAST(protocols AS string) AS ID,
    *
  
  FROM aka_test_oracle_536 AS in0

),

TextToColumns_538 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_539_0'], 
      'protocols', 
      ",", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'protocols', 
      'protocols', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_538_dropGem_0 AS (

  SELECT 
    generatedColumnName AS protocols,
    * EXCEPT (`generatedColumnName`, `protocols`)
  
  FROM TextToColumns_538 AS in0

),

Cleanse_540 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_538_dropGem_0'], 
      [
        { "name": "protocols", "dataType": "String" }, 
        { "name": "ID", "dataType": "String" }, 
        { "name": "endorsed_goals_r_other_unknown", "dataType": "String" }, 
        { "name": "endorsed_goals_sex_male", "dataType": "String" }, 
        { "name": "endorsed_goals_r_black_aa", "dataType": "String" }, 
        { "name": "endorsed_goals_eth_non_hispanic", "dataType": "String" }, 
        { "name": "endorsed_goals_r_caucasian", "dataType": "String" }, 
        { "name": "endorsed_goals_eth_hispanic_latino", "dataType": "String" }, 
        { "name": "endorsed_goals_r_amerindian_alaska", "dataType": "String" }, 
        { "name": "endorsed_goals_sex_female", "dataType": "String" }, 
        { "name": "endorsed_goals_r_hawaiian_pacific", "dataType": "String" }, 
        { "name": "endorsed_goals_r_asian", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['endorsed_goals_r_black_aa', 'endorsed_goals_eth_hispanic_latino', 'protocols', 'ID'], 
      false, 
      '', 
      false, 
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

Filter_542 AS (

  SELECT * 
  
  FROM Cleanse_540 AS in0
  
  WHERE (NOT(protocols IS NULL))

),

Formula_543_0 AS (

  SELECT 
    CAST('EPI_FRAMEWORK' AS string) AS Goals_Approach,
    *
  
  FROM Filter_542 AS in0

),

aka_test_oracle_563 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_563') }}

),

AlteryxSelect_535 AS (

  SELECT 
    study_number_pfe AS study_number_pfe,
    protocol_id AS protocol_id,
    fsfv_date AS fsfv_date,
    lsfv_date AS lsfv_date,
    subject_type AS subject_type,
    primary_exclude AS primary_exclude,
    lsfv_yr AS lsfv_yr,
    `dashboard flag` AS `dashboard flag`,
    `today's status_reg` AS `today's status_reg`,
    `goal includeslashexclude_recon` AS `goal includeslashexclude_recon`,
    report_date AS report_date,
    `trial type` AS `trial type`
  
  FROM aka_test_oracle_563 AS in0

),

Join_544_right AS (

  SELECT in0.*
  
  FROM AlteryxSelect_535 AS in0
  ANTI JOIN Formula_543_0 AS in1
     ON (in1.protocols = in0.study_number_pfe)

),

TextInput_548 AS (

  SELECT * 
  
  FROM {{ ref('seed_548')}}

),

TextInput_548_cast AS (

  SELECT 
    CAST(study_id AS string) AS study_id,
    CAST(`Dashboard flag` AS string) AS `Dashboard flag`
  
  FROM TextInput_548 AS in0

),

Join_545_left AS (

  SELECT in0.*
  
  FROM Join_544_right AS in0
  ANTI JOIN TextInput_548_cast AS in1
     ON (in0.study_number_pfe = in1.study_id)

),

Filter_551 AS (

  SELECT * 
  
  FROM Join_545_left AS in0
  
  WHERE (primary_exclude IS NULL)

),

Formula_558_0 AS (

  SELECT 
    CAST('TBD' AS string) AS Goals_Approach,
    *
  
  FROM Filter_551 AS in0

),

aka_test_oracle_490 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_490') }}

),

AlteryxSelect_587 AS (

  SELECT 
    study_id AS study_id,
    drug_program_code AS drug_program_code,
    business_category_der AS business_category_der,
    asset AS asset,
    tot_subj_planned_study AS tot_subj_planned_study,
    all_patients AS all_patients,
    us_patients AS us_patients,
    country_name AS country_name,
    site_name AS site_name,
    pi_name AS pi_name,
    study_site_number AS study_site_number,
    state_province_county AS state_province_county,
    city_town_village AS city_town_village,
    total_enrolled_at_site AS total_enrolled_at_site,
    total_screened_at_site AS total_screened_at_site,
    subject_status AS subject_status,
    category AS category,
    demography_diversity AS demography_diversity,
    patient_count AS patient_count,
    county_level AS county_level,
    country_level_goal AS country_level_goal,
    source_database AS source_database,
    goals_approach AS goals_approach,
    last_refresh AS last_refresh,
    name AS name,
    recruitment_review_meeting AS recruitment_review_meeting,
    `always on` AS `always on`
  
  FROM aka_test_oracle_490 AS in0

),

Filter_529_to_Filter_527 AS (

  SELECT * 
  
  FROM AlteryxSelect_587 AS in0
  
  WHERE (
          (
            (
              NOT(
                study_id = 'Select a Study')
            ) AND (country_name = 'UNITED STATES')
          )
          AND (CAST(subject_status AS string) IN ('COMPLETE', 'DISCONTINUED', 'ENROLLED/RANDOMIZED', 'FOLLOW-UP'))
        )

),

AlteryxSelect_528 AS (

  SELECT 
    CAST(country_level_goal AS DOUBLE) AS country_level_goal,
    * EXCEPT (`country_level_goal`)
  
  FROM Filter_529_to_Filter_527 AS in0

),

Filter_495 AS (

  SELECT * 
  
  FROM AlteryxSelect_528 AS in0
  
  WHERE (category = 'Ethnicity')

),

Summarize_494 AS (

  SELECT 
    SUM(patient_count) AS Sum_patient_count,
    MAX(us_patients) AS Max_us_patients,
    MAX(country_level_goal) AS Max_country_level_goal,
    study_id AS study_id,
    category AS category,
    demography_diversity AS demography_diversity
  
  FROM Filter_495 AS in0
  
  GROUP BY 
    study_id, category, demography_diversity

),

Formula_498_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (((Sum_patient_count / Max_us_patients) / 1.0E-6D) < 0)
          AND (
                (
                  ((Sum_patient_count / Max_us_patients) / 1.0E-6D)
                  - floor(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
                ) = 0.5
              )
        )
          THEN ceil(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
        ELSE round(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
      END
      * 1.0E-6D
    ) AS DOUBLE) AS patient_pct,
    *
  
  FROM Summarize_494 AS in0

),

Formula_498_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (((patient_pct - Max_country_level_goal) / 1.0E-6D) < 0)
          AND (
                (
                  ((patient_pct - Max_country_level_goal) / 1.0E-6D)
                  - floor(((patient_pct - Max_country_level_goal) / 1.0E-6D))
                ) = 0.5
              )
        )
          THEN ceil(((patient_pct - Max_country_level_goal) / 1.0E-6D))
        ELSE round(((patient_pct - Max_country_level_goal) / 1.0E-6D))
      END
      * 1.0E-6D
    ) AS DOUBLE) AS variance_from_goal,
    *
  
  FROM Formula_498_0 AS in0

),

Filter_499 AS (

  SELECT * 
  
  FROM Formula_498_1 AS in0
  
  WHERE (demography_diversity = 'Hispanic or Latino(a) or of Spanish Origin')

),

AlteryxSelect_501 AS (

  SELECT 
    Max_country_level_goal AS hispanic_goal,
    patient_pct AS hispanic_pct,
    variance_from_goal AS hispanic_variance_from_goal,
    * EXCEPT (`category`, 
    `demography_diversity`, 
    `Sum_patient_count`, 
    `Max_us_patients`, 
    `Max_country_level_goal`, 
    `patient_pct`, 
    `variance_from_goal`)
  
  FROM Filter_499 AS in0

),

Join_544_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_543_0 AS in0
  INNER JOIN AlteryxSelect_535 AS in1
     ON (in0.protocols = in1.study_number_pfe)

),

AlteryxSelect_552 AS (

  SELECT 
    protocols AS study_id,
    Goals_Approach AS Goals_Approach,
    endorsed_goals_r_black_aa AS Avg_Black_or_African_American,
    endorsed_goals_r_caucasian AS endorsed_goals_r_caucasian,
    endorsed_goals_r_asian AS endorsed_goals_r_asian,
    endorsed_goals_r_amerindian_alaska AS endorsed_goals_r_amerindian_alaska,
    endorsed_goals_r_hawaiian_pacific AS endorsed_goals_r_hawaiian_pacific,
    endorsed_goals_r_other_unknown AS endorsed_goals_r_other_unknown,
    endorsed_goals_eth_hispanic_latino AS Avg_Hispanic_Latino_a__or_of_Spanish_Origin,
    endorsed_goals_eth_non_hispanic AS endorsed_goals_eth_non_hispanic,
    endorsed_goals_sex_male AS endorsed_goals_sex_male,
    endorsed_goals_sex_female AS endorsed_goals_sex_female
  
  FROM Join_544_inner AS in0

),

Summarize_500 AS (

  SELECT 
    *,
    MAX(us_patients) OVER (PARTITION BY study_id ORDER BY study_id ASC NULLS FIRST, us_patients DESC NULLS FIRST, goals_approach DESC NULLS FIRST) AS us_patients_tmp,
    first(goals_approach) OVER (PARTITION BY study_id ORDER BY study_id ASC NULLS FIRST, us_patients DESC NULLS FIRST, goals_approach DESC NULLS FIRST) AS goals_approach_tmp,
    row_number() OVER (PARTITION BY study_id ORDER BY study_id ASC NULLS FIRST, us_patients DESC NULLS FIRST, goals_approach DESC NULLS FIRST) AS row_number
  
  FROM AlteryxSelect_528 AS in0

),

Summarize_500_rename AS (

  SELECT 
    us_patients_tmp AS us_patients,
    goals_approach_tmp AS goals_approach,
    * EXCEPT (`us_patients`, `goals_approach`, `us_patients_tmp`, `goals_approach_tmp`)
  
  FROM Summarize_500 AS in0

),

`500_filter` AS (

  SELECT * 
  
  FROM Summarize_500_rename AS in0
  
  WHERE (row_number = 1)

),

Filter_492 AS (

  SELECT * 
  
  FROM AlteryxSelect_528 AS in0
  
  WHERE (category = 'Race')

),

Summarize_491 AS (

  SELECT 
    SUM(patient_count) AS Sum_patient_count,
    MAX(us_patients) AS Max_us_patients,
    MAX(country_level_goal) AS Max_country_level_goal,
    study_id AS study_id,
    category AS category,
    demography_diversity AS demography_diversity
  
  FROM Filter_492 AS in0
  
  GROUP BY 
    study_id, category, demography_diversity

),

Formula_493_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (((Sum_patient_count / Max_us_patients) / 1.0E-6D) < 0)
          AND (
                (
                  ((Sum_patient_count / Max_us_patients) / 1.0E-6D)
                  - floor(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
                ) = 0.5
              )
        )
          THEN ceil(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
        ELSE round(((Sum_patient_count / Max_us_patients) / 1.0E-6D))
      END
      * 1.0E-6D
    ) AS DOUBLE) AS patient_pct,
    *
  
  FROM Summarize_491 AS in0

),

Formula_493_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (((patient_pct - Max_country_level_goal) / 1.0E-6D) < 0)
          AND (
                (
                  ((patient_pct - Max_country_level_goal) / 1.0E-6D)
                  - floor(((patient_pct - Max_country_level_goal) / 1.0E-6D))
                ) = 0.5
              )
        )
          THEN ceil(((patient_pct - Max_country_level_goal) / 1.0E-6D))
        ELSE round(((patient_pct - Max_country_level_goal) / 1.0E-6D))
      END
      * 1.0E-6D
    ) AS DOUBLE) AS variance_from_goal,
    *
  
  FROM Formula_493_0 AS in0

),

Filter_513 AS (

  SELECT * 
  
  FROM Formula_493_1 AS in0
  
  WHERE (demography_diversity = 'Native Hawaiian or Other Pacific Islander')

),

AlteryxSelect_514 AS (

  SELECT 
    Max_country_level_goal AS native_hawaiian_or_pacific_goal,
    patient_pct AS native_hawaiian_or_pacific_pct,
    variance_from_goal AS native_hawaiian_or_pacific_variance_from_goal,
    * EXCEPT (`category`, 
    `demography_diversity`, 
    `Sum_patient_count`, 
    `Max_us_patients`, 
    `Max_country_level_goal`, 
    `patient_pct`, 
    `variance_from_goal`)
  
  FROM Filter_513 AS in0

),

Formula_515_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (native_hawaiian_or_pacific_variance_from_goal >= 0)
          THEN 'At or Above'
        WHEN (native_hawaiian_or_pacific_variance_from_goal < 0)
          THEN 'Below'
        ELSE NULL
      END
    ) AS string) AS native_hawaiian_or_pacific_against_goal,
    *
  
  FROM AlteryxSelect_514 AS in0

),

Join_545_inner AS (

  SELECT 
    in1.`Dashboard flag` AS `Right_Dashboard flag`,
    in0.*,
    in1.* EXCEPT (`Dashboard flag`)
  
  FROM Join_544_right AS in0
  INNER JOIN TextInput_548_cast AS in1
     ON (in0.study_number_pfe = in1.study_id)

),

Formula_546_0 AS (

  SELECT 
    CAST('LEGACY' AS string) AS Goals_Approach,
    *
  
  FROM Join_545_inner AS in0

),

Filter_547_reject AS (

  SELECT * 
  
  FROM Formula_546_0 AS in0
  
  WHERE (
          (
            NOT(
              (NOT(primary_exclude IS NULL))
              AND (
                    (
                      NOT(
                        primary_exclude = 'Include')
                    ) OR (primary_exclude IS NULL)
                  ))
          )
          OR (
               (
                 (NOT(primary_exclude IS NULL))
                 AND (
                       (
                         NOT(
                           primary_exclude = 'Include')
                       ) OR (primary_exclude IS NULL)
                     )
               ) IS NULL
             )
        )

),

Filter_547 AS (

  SELECT * 
  
  FROM Formula_546_0 AS in0
  
  WHERE (
          (NOT(primary_exclude IS NULL))
          AND (
                (
                  NOT(
                    primary_exclude = 'Include')
                ) OR (primary_exclude IS NULL)
              )
        )

),

Formula_556_0 AS (

  SELECT 
    CAST('OUT OF SCOPE' AS string) AS Goals_Approach,
    * EXCEPT (`goals_approach`)
  
  FROM Filter_547 AS in0

),

AlteryxSelect_557 AS (

  SELECT 
    study_number_pfe AS study_id,
    Goals_Approach AS Goals_Approach
  
  FROM Formula_556_0 AS in0

),

Union_555_reformat_1 AS (

  SELECT 
    Goals_Approach AS Goals_Approach,
    study_id AS study_id
  
  FROM AlteryxSelect_557 AS in0

),

Union_555_reformat_0 AS (

  SELECT 
    CAST(Avg_Black_or_African_American AS string) AS Avg_Black_or_African_American,
    CAST(Avg_Hispanic_Latino_a__or_of_Spanish_Origin AS string) AS Avg_Hispanic_Latino_a__or_of_Spanish_Origin,
    Goals_Approach AS Goals_Approach,
    CAST(endorsed_goals_eth_non_hispanic AS string) AS endorsed_goals_eth_non_hispanic,
    CAST(endorsed_goals_r_amerindian_alaska AS string) AS endorsed_goals_r_amerindian_alaska,
    CAST(endorsed_goals_r_asian AS string) AS endorsed_goals_r_asian,
    CAST(endorsed_goals_r_caucasian AS string) AS endorsed_goals_r_caucasian,
    CAST(endorsed_goals_r_hawaiian_pacific AS string) AS endorsed_goals_r_hawaiian_pacific,
    CAST(endorsed_goals_r_other_unknown AS string) AS endorsed_goals_r_other_unknown,
    CAST(endorsed_goals_sex_female AS string) AS endorsed_goals_sex_female,
    CAST(endorsed_goals_sex_male AS string) AS endorsed_goals_sex_male,
    study_id AS study_id
  
  FROM AlteryxSelect_552 AS in0

),

AlteryxSelect_554 AS (

  SELECT 
    study_number_pfe AS study_id,
    Goals_Approach AS Goals_Approach
  
  FROM Filter_547_reject AS in0

),

Formula_553_0 AS (

  SELECT 
    CAST(0.134 AS DOUBLE) AS Avg_Black_or_African_American,
    CAST(0.763 AS DOUBLE) AS endorsed_goals_r_caucasian,
    CAST(0.059 AS DOUBLE) AS endorsed_goals_r_asian,
    CAST(0.013 AS DOUBLE) AS endorsed_goals_r_amerindian_alaska,
    CAST(0.002 AS DOUBLE) AS endorsed_goals_r_hawaiian_pacific,
    CAST(0.185 AS DOUBLE) AS Avg_Hispanic_Latino_a__or_of_Spanish_Origin,
    CAST(0.492 AS DOUBLE) AS endorsed_goals_sex_male,
    CAST(0.508 AS DOUBLE) AS endorsed_goals_sex_female,
    *
  
  FROM AlteryxSelect_554 AS in0

),

Union_555_reformat_3 AS (

  SELECT 
    CAST(Avg_Black_or_African_American AS string) AS Avg_Black_or_African_American,
    CAST(Avg_Hispanic_Latino_a__or_of_Spanish_Origin AS string) AS Avg_Hispanic_Latino_a__or_of_Spanish_Origin,
    Goals_Approach AS Goals_Approach,
    CAST(endorsed_goals_r_amerindian_alaska AS string) AS endorsed_goals_r_amerindian_alaska,
    CAST(endorsed_goals_r_asian AS string) AS endorsed_goals_r_asian,
    CAST(endorsed_goals_r_caucasian AS string) AS endorsed_goals_r_caucasian,
    CAST(endorsed_goals_r_hawaiian_pacific AS string) AS endorsed_goals_r_hawaiian_pacific,
    CAST(endorsed_goals_sex_female AS string) AS endorsed_goals_sex_female,
    CAST(endorsed_goals_sex_male AS string) AS endorsed_goals_sex_male,
    study_id AS study_id
  
  FROM Formula_553_0 AS in0

),

AlteryxSelect_559 AS (

  SELECT 
    study_number_pfe AS study_id,
    Goals_Approach AS Goals_Approach
  
  FROM Formula_558_0 AS in0

),

Union_555_reformat_2 AS (

  SELECT 
    Goals_Approach AS Goals_Approach,
    study_id AS study_id
  
  FROM AlteryxSelect_559 AS in0

),

Union_555 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_555_reformat_0', 'Union_555_reformat_3', 'Union_555_reformat_1', 'Union_555_reformat_2'], 
      [
        '[{"name": "Avg_Black_or_African_American", "dataType": "String"}, {"name": "Avg_Hispanic_Latino_a__or_of_Spanish_Origin", "dataType": "String"}, {"name": "Goals_Approach", "dataType": "String"}, {"name": "endorsed_goals_eth_non_hispanic", "dataType": "String"}, {"name": "endorsed_goals_r_amerindian_alaska", "dataType": "String"}, {"name": "endorsed_goals_r_asian", "dataType": "String"}, {"name": "endorsed_goals_r_caucasian", "dataType": "String"}, {"name": "endorsed_goals_r_hawaiian_pacific", "dataType": "String"}, {"name": "endorsed_goals_r_other_unknown", "dataType": "String"}, {"name": "endorsed_goals_sex_female", "dataType": "String"}, {"name": "endorsed_goals_sex_male", "dataType": "String"}, {"name": "study_id", "dataType": "String"}]', 
        '[{"name": "Avg_Black_or_African_American", "dataType": "String"}, {"name": "Avg_Hispanic_Latino_a__or_of_Spanish_Origin", "dataType": "String"}, {"name": "Goals_Approach", "dataType": "String"}, {"name": "endorsed_goals_r_amerindian_alaska", "dataType": "String"}, {"name": "endorsed_goals_r_asian", "dataType": "String"}, {"name": "endorsed_goals_r_caucasian", "dataType": "String"}, {"name": "endorsed_goals_r_hawaiian_pacific", "dataType": "String"}, {"name": "endorsed_goals_sex_female", "dataType": "String"}, {"name": "endorsed_goals_sex_male", "dataType": "String"}, {"name": "study_id", "dataType": "String"}]', 
        '[{"name": "Goals_Approach", "dataType": "String"}, {"name": "study_id", "dataType": "String"}]', 
        '[{"name": "Goals_Approach", "dataType": "String"}, {"name": "study_id", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_564_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Goals_Approach = 'Legacy')
          THEN 60.1
        ELSE NULL
      END
    ) AS DOUBLE) AS endorsed_goals_r_eth_nonhispanic_white,
    CAST((
      CASE
        WHEN (Goals_Approach = 'Legacy')
          THEN 39.9
        ELSE NULL
      END
    ) AS DOUBLE) AS endorsed_goals_r_eth_diverse,
    *
  
  FROM Union_555 AS in0

),

Transpose_560 AS (

  {{
    prophecy_basics.Transpose(
      ['Formula_564_0'], 
      ['study_id', 'Goals_Approach'], 
      [
        'Avg_Black_or_African_American', 
        'endorsed_goals_r_caucasian', 
        'endorsed_goals_r_asian', 
        'endorsed_goals_r_amerindian_alaska', 
        'endorsed_goals_r_hawaiian_pacific', 
        'endorsed_goals_r_other_unknown', 
        'Avg_Hispanic_Latino_a__or_of_Spanish_Origin', 
        'endorsed_goals_eth_non_hispanic', 
        'endorsed_goals_sex_male', 
        'endorsed_goals_sex_female', 
        'endorsed_goals_r_eth_nonhispanic_white', 
        'endorsed_goals_r_eth_diverse'
      ], 
      'Name', 
      'Value', 
      [
        'endorsed_goals_r_eth_nonhispanic_white', 
        'endorsed_goals_r_eth_diverse', 
        'Avg_Black_or_African_American', 
        'Avg_Hispanic_Latino_a__or_of_Spanish_Origin', 
        'Goals_Approach', 
        'endorsed_goals_eth_non_hispanic', 
        'endorsed_goals_r_amerindian_alaska', 
        'endorsed_goals_r_asian', 
        'endorsed_goals_r_caucasian', 
        'endorsed_goals_r_hawaiian_pacific', 
        'endorsed_goals_r_other_unknown', 
        'endorsed_goals_sex_female', 
        'endorsed_goals_sex_male', 
        'study_id'
      ], 
      true
    )
  }}

),

Formula_561_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Name = 'Avg_Black_or_African_American')
          THEN 'Black or African American'
        WHEN (Name = 'endorsed_goals_r_amerindian_alaska')
          THEN 'American Indian or Alaska Native'
        WHEN (Name = 'endorsed_goals_r_asian')
          THEN 'Asian'
        WHEN (Name = 'endorsed_goals_r_caucasian')
          THEN 'White'
        WHEN (Name = 'endorsed_goals_r_hawaiian_pacific')
          THEN 'Native Hawaiian or Other Pacific Islander'
        WHEN (Name = 'endorsed_goals_r_other_unknown')
          THEN 'Other'
        WHEN (Name = 'Avg_Hispanic_Latino_a__or_of_Spanish_Origin')
          THEN 'Hispanic or Latino(a) or of Spanish Origin'
        WHEN (Name = 'endorsed_goals_eth_non_hispanic')
          THEN 'Not Hispanic or Latino(a) or of Spanish Origin'
        WHEN (Name = 'endorsed_goals_sex_female')
          THEN 'Female'
        WHEN (Name = 'endorsed_goals_sex_male')
          THEN 'Male'
        WHEN (Name = 'endorsed_goals_r_eth_nonhispanic_white')
          THEN 'Non-Hispanic White'
        WHEN (Name = 'endorsed_goals_r_eth_diverse')
          THEN 'Diverse Race or Ethnicity'
        ELSE 'ZZZ'
      END
    ) AS string) AS demography_diversity,
    CAST(CASE
      WHEN ((Goals_Approach = 'EPI_FRAMEWORK') AND NOT (isnull(Value)))
        THEN (CAST(Value AS INT) / 100)
      ELSE Value
    END AS DOUBLE) AS country_level_goal,
    *
  
  FROM Transpose_560 AS in0

),

Formula_561_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (demography_diversity = 'American Indian or Alaska Native')
          THEN 'Race'
        WHEN (demography_diversity = 'Asian')
          THEN 'Race'
        WHEN (demography_diversity = 'Black or African American')
          THEN 'Race'
        WHEN (demography_diversity = 'White')
          THEN 'Race'
        WHEN (demography_diversity = 'Native Hawaiian or Other Pacific Islander')
          THEN 'Race'
        WHEN (demography_diversity = 'Hispanic or Latino(a) or of Spanish Origin')
          THEN 'Ethnicity'
        WHEN (demography_diversity = 'Not Hispanic or Latino(a) or of Spanish Origin')
          THEN 'Ethnicity'
        WHEN (demography_diversity = 'Other')
          THEN 'Race'
        WHEN (demography_diversity = 'Female')
          THEN 'Sex'
        WHEN (demography_diversity = 'Male')
          THEN 'Sex'
        WHEN (demography_diversity = 'Non-Hispanic White')
          THEN 'Race/Ethnicity'
        WHEN (demography_diversity = 'Diverse Race or Ethnicity')
          THEN 'Race/Ethnicity'
        ELSE 'ZZZ'
      END
    ) AS string) AS category,
    *
  
  FROM Formula_561_0 AS in0

),

AlteryxSelect_562 AS (

  SELECT 
    study_id AS study_id,
    Goals_Approach AS goals_approach,
    category AS category,
    demography_diversity AS demography_diversity,
    country_level_goal AS country_level_goal
  
  FROM Formula_561_1 AS in0

),

Filter_565 AS (

  SELECT * 
  
  FROM AlteryxSelect_562 AS in0
  
  WHERE (category IN ('Race', 'Ethnicity'))

),

CrossTab_566 AS (

  SELECT *
  
  FROM (
    SELECT 
      demography_diversity,
      country_level_goal
    
    FROM Filter_565 AS in0
  )
  PIVOT (
    FIRST(country_level_goal) AS First
    FOR demography_diversity
    IN (
      'Black_or_African_American', 
      'Other', 
      'Native_Hawaiian_or_Other_Pacific_Islander', 
      'White', 
      'Not_Hispanic_or_Latino_a__or_of_Spanish_Origin', 
      'Asian', 
      'American_Indian_or_Alaska_Native', 
      'Hispanic_or_Latino_a__or_of_Spanish_Origin'
    )
  )

),

AlteryxSelect_567 AS (

  SELECT 
    American_Indian_or_Alaska_Native AS american_indian_or_alaskan_goal,
    Asian AS asian_goal,
    Black_or_African_American AS black_or_aa_goal,
    Hispanic_or_Latino_a__or_of_Spanish_Origin AS hispanic_goal,
    Native_Hawaiian_or_Other_Pacific_Islander AS native_hawaiian_or_pacific_goal,
    * EXCEPT (`Not_Hispanic_or_Latino_a__or_of_Spanish_Origin`, 
    `Other`, 
    `White`, 
    `American_Indian_or_Alaska_Native`, 
    `Asian`, 
    `Black_or_African_American`, 
    `Hispanic_or_Latino_a__or_of_Spanish_Origin`, 
    `Native_Hawaiian_or_Other_Pacific_Islander`)
  
  FROM CrossTab_566 AS in0

),

Filter_496 AS (

  SELECT * 
  
  FROM Formula_493_1 AS in0
  
  WHERE (demography_diversity = 'Black or African American')

),

AlteryxSelect_497 AS (

  SELECT 
    Max_country_level_goal AS black_or_aa_goal,
    patient_pct AS black_or_aa_pct,
    variance_from_goal AS black_or_aa_variance_from_goal,
    * EXCEPT (`category`, 
    `demography_diversity`, 
    `Sum_patient_count`, 
    `Max_us_patients`, 
    `Max_country_level_goal`, 
    `patient_pct`, 
    `variance_from_goal`)
  
  FROM Filter_496 AS in0

),

Formula_505_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (black_or_aa_variance_from_goal >= 0)
          THEN 'At or Above'
        WHEN (black_or_aa_variance_from_goal < 0)
          THEN 'Below'
        ELSE NULL
      END
    ) AS string) AS black_or_aa_against_goal,
    *
  
  FROM AlteryxSelect_497 AS in0

),

Filter_516 AS (

  SELECT * 
  
  FROM Formula_493_1 AS in0
  
  WHERE (demography_diversity = 'Asian')

),

Filter_510 AS (

  SELECT * 
  
  FROM Formula_493_1 AS in0
  
  WHERE (demography_diversity = 'American Indian or Alaska Native')

),

AlteryxSelect_511 AS (

  SELECT 
    Max_country_level_goal AS american_indian_or_alaskan_goal,
    patient_pct AS american_indian_or_alaskan_pct,
    variance_from_goal AS american_indian_or_alaskan_variance_from_goal,
    * EXCEPT (`category`, 
    `demography_diversity`, 
    `Sum_patient_count`, 
    `Max_us_patients`, 
    `Max_country_level_goal`, 
    `patient_pct`, 
    `variance_from_goal`)
  
  FROM Filter_510 AS in0

),

Formula_512_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (american_indian_or_alaskan_variance_from_goal >= 0)
          THEN 'At or Above'
        WHEN (american_indian_or_alaskan_variance_from_goal < 0)
          THEN 'Below'
        ELSE NULL
      END
    ) AS string) AS american_indian_or_alaskan_against_goal,
    *
  
  FROM AlteryxSelect_511 AS in0

),

Summarize_500_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `500_filter` AS in0

),

Join_502_right_UnionRightOuter AS (

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
  
  FROM Summarize_500_drop_0 AS in0
  RIGHT JOIN Formula_505_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Join_519_right_UnionRightOuter AS (

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
  
  FROM Join_502_right_UnionRightOuter AS in0
  RIGHT JOIN Formula_512_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Join_521_right_UnionRightOuter AS (

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
  
  FROM Join_519_right_UnionRightOuter AS in0
  RIGHT JOIN Formula_515_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Formula_504_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (hispanic_variance_from_goal >= 0)
          THEN 'At or Above'
        WHEN (hispanic_variance_from_goal < 0)
          THEN 'Below'
        ELSE NULL
      END
    ) AS string) AS hispanic_against_goal,
    *
  
  FROM AlteryxSelect_501 AS in0

),

AlteryxSelect_517 AS (

  SELECT 
    Max_country_level_goal AS asian_goal,
    patient_pct AS asian_pct,
    variance_from_goal AS asian_variance_from_goal,
    * EXCEPT (`category`, 
    `demography_diversity`, 
    `Sum_patient_count`, 
    `Max_us_patients`, 
    `Max_country_level_goal`, 
    `patient_pct`, 
    `variance_from_goal`)
  
  FROM Filter_516 AS in0

),

Formula_518_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (asian_variance_from_goal >= 0)
          THEN 'At or Above'
        WHEN (asian_variance_from_goal < 0)
          THEN 'Below'
        ELSE NULL
      END
    ) AS string) AS asian_against_goal,
    *
  
  FROM AlteryxSelect_517 AS in0

),

Join_523_right_UnionRightOuter AS (

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
  
  FROM Join_521_right_UnionRightOuter AS in0
  RIGHT JOIN Formula_518_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Join_503_right_UnionRightOuter AS (

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
  
  FROM Join_523_right_UnionRightOuter AS in0
  RIGHT JOIN Formula_504_0 AS in1
     ON (in0.study_id = in1.study_id)

),

Join_531_left_UnionFullOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`american_indian_or_alaskan_goal`, 
    `asian_goal`, 
    `black_or_aa_goal`, 
    `hispanic_goal`, 
    `native_hawaiian_or_pacific_goal`)
  
  FROM Join_503_right_UnionRightOuter AS in0
  FULL JOIN AlteryxSelect_567 AS in1
     ON (in0.study_id = in1.study_id)

),

Formula_395_0 AS (

  SELECT *
  
  FROM {{ ref('wf_standard_core_recruitment_review_thaded__Formula_395_0')}}

),

Join_367_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`drug_program_code`, `business_category_der`, `recruitment_review_meeting`, `asset`),
    in1.* EXCEPT (`study_id`)
  
  FROM Formula_395_0 AS in0
  LEFT JOIN Join_531_left_UnionFullOuter AS in1
     ON (in0.study_id = in1.study_id)

),

Join_370_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`study_number_pfe`, `study_id`)
  
  FROM Join_367_left_UnionLeftOuter AS in0
  LEFT JOIN AlteryxSelect_507 AS in1
     ON (in0.study_id = in1.study_id)

),

AlteryxSelect_584 AS (

  SELECT 
    study_id AS study_id,
    activation_actuals AS activation_actuals,
    rand_dvso_baseline_to_date AS rand_dvso_baseline_to_date,
    activation_mtp_baseline_to_date AS activation_mtp_baseline_to_date,
    activation_dvso_baseline_to_date AS activation_dvso_baseline_to_date,
    rand_latest_estimate_to_date AS rand_latest_estimate_to_date,
    activation_mtp_plan_to_date AS activation_mtp_plan_to_date,
    rand_rapid_baseline_to_date AS rand_rapid_baseline_to_date,
    rand_dvso_baseline_total AS rand_dvso_baseline_total,
    activation_mtp_baseline_total AS activation_mtp_baseline_total,
    activation_dvso_baseline_total AS activation_dvso_baseline_total,
    rand_latest_estimate_total AS rand_latest_estimate_total,
    activation_mtp_plan_total AS activation_mtp_plan_total,
    rand_rapid_baseline_total AS rand_rapid_baseline_total,
    protocol_id AS protocol_id,
    study_status AS study_status,
    study_type AS study_type,
    planned_patients AS planned_patients,
    asset AS asset,
    business_category_der AS business_category_der,
    priority_level AS priority_level,
    candidate_priority AS candidate_priority,
    rapid_model AS rapid_model,
    amendments_prior_to_fsfv AS amendments_prior_to_fsfv,
    max_amendment_date_prior_to_fsfv AS max_amendment_date_prior_to_fsfv,
    amendments_during_enrollment AS amendments_during_enrollment,
    max_amendment_date_during_enrollment AS max_amendment_date_during_enrollment,
    total_number_amendments AS total_number_amendments,
    lsfv_date AS lsfv_date,
    lsfv_pct_cmp AS lsfv_pct_cmp,
    dvso_partial_plan_desc AS dvso_partial_plan_desc,
    cohort_type AS cohort_type,
    cohort_name AS cohort_name,
    study_number_pfe AS study_number_pfe,
    activation_rapid_plan_total AS activation_rapid_plan_total,
    sites100_date AS sites100_date,
    sites100_pct_cmp AS sites100_pct_cmp,
    recruitment_review_scope_flag AS recruitment_review_scope_flag,
    candidate_portfolio_priority AS candidate_portfolio_priority,
    goal_study AS goal_study,
    rand_actuals AS rand_actuals,
    rand_plan_to_date AS rand_plan_to_date,
    rand_plan_to_date_src AS rand_plan_to_date_src,
    rand_plan AS rand_plan,
    rand_plan_src AS rand_plan_src,
    rand_to_date_variance AS rand_to_date_variance,
    rand_to_date_fraction AS rand_to_date_fraction,
    rand_total_variance AS rand_total_variance,
    rand_actuals_90d AS rand_actuals_90d,
    rand_dvso_baseline_to_date_90d AS rand_dvso_baseline_to_date_90d,
    rand_latest_estimate_to_date_90d AS rand_latest_estimate_to_date_90d,
    rand_rapid_baseline_to_date_90d AS rand_rapid_baseline_to_date_90d,
    rand_actuals_30d AS rand_actuals_30d,
    rand_dvso_baseline_to_date_30d AS rand_dvso_baseline_to_date_30d,
    rand_latest_estimate_to_date_30d AS rand_latest_estimate_to_date_30d,
    rand_rapid_baseline_to_date_30d AS rand_rapid_baseline_to_date_30d,
    fsfv_date AS fsfv_date,
    fsfv_pct_cmp AS fsfv_pct_cmp,
    study_phase_bin AS study_phase_bin,
    mop_study_status AS mop_study_status,
    rand_plan_to_date_90d AS rand_plan_to_date_90d,
    rand_plan_to_date_30d AS rand_plan_to_date_30d,
    mop_tl_current AS mop_tl_current,
    cd_patient_population_age_der AS cd_patient_population_age_der,
    rand_to_date_variance_90d AS rand_to_date_variance_90d,
    rand_to_date_variance_30d AS rand_to_date_variance_30d,
    drug_program_code AS drug_program_code,
    candidate_code AS candidate_code,
    protocol_description_working AS protocol_description_working,
    protocol_description_planning AS protocol_description_planning,
    study_title AS study_title,
    study_phase AS study_phase,
    study_phase_planned AS study_phase_planned,
    study_sub_type AS study_sub_type,
    methodology_study AS methodology_study,
    compassionate_use_study AS compassionate_use_study,
    post_author_safety_study AS post_author_safety_study,
    post_author_effective_study AS post_author_effective_study,
    ccs_clinical_placement AS ccs_clinical_placement,
    project_plan_type AS project_plan_type,
    study_execution_state AS study_execution_state,
    subject_type AS subject_type,
    study_status_planning AS study_status_planning,
    transfer_status AS transfer_status,
    model AS model,
    front_end AS front_end,
    study_termination_date AS study_termination_date,
    back_end AS back_end,
    study_closed AS study_closed,
    commercial_bu AS commercial_bu,
    study_type_plan AS study_type_plan,
    assigned_ctms AS assigned_ctms,
    patient_database AS patient_database,
    sponsor_product AS sponsor_product,
    business_group AS business_group,
    medical_responsibility AS medical_responsibility,
    terminate_decision_date AS terminate_decision_date,
    sponsoring_division AS sponsoring_division,
    sponsoring_unit AS sponsoring_unit,
    study_therapeutic_area AS study_therapeutic_area,
    study_design AS study_design,
    business_rationale AS business_rationale,
    pediatric_study AS pediatric_study,
    primary_data_collection AS primary_data_collection,
    secondary_data_collection AS secondary_data_collection,
    study_project_planner_planning AS study_project_planner_planning,
    funding_source AS funding_source,
    subj_max_age_der AS subj_max_age_der,
    subj_min_age_der AS subj_min_age_der,
    study_post_reg_commitment AS study_post_reg_commitment,
    eudract_num AS eudract_num,
    study_status_assessment_date AS study_status_assessment_date,
    study_finance_code AS study_finance_code,
    load_ts_cdm AS load_ts_cdm,
    study_alias AS study_alias,
    study_nctid AS study_nctid,
    bl137t_date AS bl137t_date,
    bl137t_pct_cmp AS bl137t_pct_cmp,
    bl137p_date AS bl137p_date,
    bl137p_pct_cmp AS bl137p_pct_cmp,
    arp_finish_date AS arp_finish_date,
    arp_finish_pct_cmp AS arp_finish_pct_cmp,
    arp_start_date AS arp_start_date,
    bdrprep_date AS bdrprep_date,
    bdrprep_pct_cmp AS bdrprep_pct_cmp,
    bdrprep_start_date AS bdrprep_start_date,
    prc_date AS prc_date,
    prc_pct_cmp AS prc_pct_cmp,
    ps_date AS ps_date,
    ps_pct_cmp AS ps_pct_cmp,
    ep3_date AS ep3_date,
    ep3_pct_cmp AS ep3_pct_cmp,
    fap_date AS fap_date,
    fap_pct_cmp AS fap_pct_cmp,
    fap_source AS fap_source,
    fap_plw_date AS fap_plw_date,
    fap_plw_pct_cmp AS fap_plw_pct_cmp,
    dba_date AS dba_date,
    dba_pct_cmp AS dba_pct_cmp,
    siv_date AS siv_date,
    siv_pct_cmp AS siv_pct_cmp,
    siv_source AS siv_source,
    siv_us_date AS siv_us_date,
    siv_us_pct_cmp AS siv_us_pct_cmp,
    siv_exus_date AS siv_exus_date,
    siv_exus_pct_cmp AS siv_exus_pct_cmp,
    crfdata_date AS crfdata_date,
    crfdata_pct_cmp AS crfdata_pct_cmp,
    csr_max_date AS csr_max_date,
    csr_min_date AS csr_min_date,
    csr_pct_cmp AS csr_pct_cmp,
    csr_plw_date AS csr_plw_date,
    csr_plw_pct_cmp AS csr_plw_pct_cmp,
    csr_sem2 AS csr_sem2,
    csr_source AS csr_source,
    dataready227t_date AS dataready227t_date,
    dataready227t_pct_cmp AS dataready227t_pct_cmp,
    dbr_date AS dbr_date,
    dbr_pct_cmp AS dbr_pct_cmp,
    dbr_sem2 AS dbr_sem2,
    der_csr_max_date AS der_csr_max_date,
    der_csr_max_pct_cmp AS der_csr_max_pct_cmp,
    der_csr_max_source AS der_csr_max_source,
    der_dbr_max_date AS der_dbr_max_date,
    der_dbr_max_pct_cmp AS der_dbr_max_pct_cmp,
    der_dbr_max_source AS der_dbr_max_source,
    der_lslv_max_date AS der_lslv_max_date,
    der_lslv_max_pct_cmp AS der_lslv_max_pct_cmp,
    der_lslv_max_source AS der_lslv_max_source,
    fsfd_date AS fsfd_date,
    fsfd_pct_cmp AS fsfd_pct_cmp,
    fsfd_sem2 AS fsfd_sem2,
    fsfv_plw_date AS fsfv_plw_date,
    fsfv_plw_pct_cmp AS fsfv_plw_pct_cmp,
    fsfv_sem2 AS fsfv_sem2,
    fsfv_source AS fsfv_source,
    ft_date AS ft_date,
    ft_pct_cmp AS ft_pct_cmp,
    ft_sem2 AS ft_sem2,
    labdata_date AS labdata_date,
    labdata_pct_cmp AS labdata_pct_cmp,
    lastdata_date AS lastdata_date,
    lastdata_pct_cmp AS lastdata_pct_cmp,
    lastdata_source AS lastdata_source,
    ldi_date AS ldi_date,
    ldi_pct_cmp AS ldi_pct_cmp,
    ldis_date AS ldis_date,
    ldis_pct_cmp AS ldis_pct_cmp,
    lsfv_sem2 AS lsfv_sem2,
    lslv_date AS lslv_date,
    lslv_pcd_date AS lslv_pcd_date,
    lslv_pcd_pct_cmp AS lslv_pcd_pct_cmp,
    lslv_pcd_sem2 AS lslv_pcd_sem2,
    lslv_pcd_source AS lslv_pcd_source,
    lslv_pct_cmp AS lslv_pct_cmp,
    lslv_plw_date AS lslv_plw_date,
    lslv_plw_pct_cmp AS lslv_plw_pct_cmp,
    lslv_sem2 AS lslv_sem2,
    lslv_source AS lslv_source,
    pcd_date AS pcd_date,
    pcd_pct_cmp AS pcd_pct_cmp,
    pcd_sem2 AS pcd_sem2,
    pkdata_date AS pkdata_date,
    pkdata_pct_cmp AS pkdata_pct_cmp,
    scd_date AS scd_date,
    scd_pct_cmp AS scd_pct_cmp,
    scd_sem2 AS scd_sem2,
    scsr_date AS scsr_date,
    scsr_pct_cmp AS scsr_pct_cmp,
    scsr_sem2 AS scsr_sem2,
    sdbr_date AS sdbr_date,
    sdbr_pct_cmp AS sdbr_pct_cmp,
    sdbr_sem2 AS sdbr_sem2,
    serology_date AS serology_date,
    serology_pct_cmp AS serology_pct_cmp,
    sites100_sem2 AS sites100_sem2,
    sites50_date AS sites50_date,
    sites50_pct_cmp AS sites50_pct_cmp,
    sites50_sem2 AS sites50_sem2,
    slock353p_date AS slock353p_date,
    slock353p_pct_cmp AS slock353p_pct_cmp,
    stlftlr_date AS stlftlr_date,
    stlftlr_pct_cmp AS stlftlr_pct_cmp,
    stlr_date AS stlr_date,
    stlr_pct_cmp AS stlr_pct_cmp,
    subjects25_date AS subjects25_date,
    subjects25_pct_cmp AS subjects25_pct_cmp,
    subjects25_sem2 AS subjects25_sem2,
    subjects50_date AS subjects50_date,
    subjects50_pct_cmp AS subjects50_pct_cmp,
    subjects50_sem2 AS subjects50_sem2,
    subjects75_date AS subjects75_date,
    subjects75_pct_cmp AS subjects75_pct_cmp,
    subjects75_sem2 AS subjects75_sem2,
    tlftlr_date AS tlftlr_date,
    tlftlr_pct_cmp AS tlftlr_pct_cmp,
    tlftlr_start_date AS tlftlr_start_date,
    tlr_date AS tlr_date,
    tlr_pct_cmp AS tlr_pct_cmp,
    tlr_sem2 AS tlr_sem2,
    fih_date AS fih_date,
    fih_pct AS fih_pct,
    pom_date AS pom_date,
    pom_pct AS pom_pct,
    poc_ss_date AS poc_ss_date,
    poc_ss_pct AS poc_ss_pct,
    poc_date AS poc_date,
    poc_pct AS poc_pct,
    phase_3_start_date AS phase_3_start_date,
    phase_3_start_pct AS phase_3_start_pct,
    nda_submission_date AS nda_submission_date,
    nda_submission_pct AS nda_submission_pct,
    maa_submission_date AS maa_submission_date,
    maa_submission_pct AS maa_submission_pct,
    jnda_submission_date AS jnda_submission_date,
    jnda_submission_pct AS jnda_submission_pct,
    nda_approval_date AS nda_approval_date,
    nda_approval_pct AS nda_approval_pct,
    maa_approval_date AS maa_approval_date,
    maa_approval_pct AS maa_approval_pct,
    jnda_approval_date AS jnda_approval_date,
    jnda_approval_pct AS jnda_approval_pct,
    poc_ss_gem_date AS poc_ss_gem_date,
    poc_gem_date AS poc_gem_date,
    phase_3_start_gem_date AS phase_3_start_gem_date,
    nda_submission_gem_date AS nda_submission_gem_date,
    maa_submission_gem_date AS maa_submission_gem_date,
    jnda_submission_gem_date AS jnda_submission_gem_date,
    nda_approval_gem_date AS nda_approval_gem_date,
    maa_approval_gem_date AS maa_approval_gem_date,
    jnda_approval_gem_date AS jnda_approval_gem_date,
    manual_milestone_change AS manual_milestone_change,
    multi_csr_flag AS multi_csr_flag,
    study_subjects_active_reg AS study_subjects_active_reg,
    study_subjects_screened_reg AS study_subjects_screened_reg,
    study_subjects_randomized AS study_subjects_randomized,
    study_subjects_completed_reg AS study_subjects_completed_reg,
    study_subjects_discontinued_reg AS study_subjects_discontinued_reg,
    study_subjects_screen_failed_reg AS study_subjects_screen_failed_reg,
    retention_rate AS retention_rate,
    screen_failure_rate AS screen_failure_rate,
    sites_completed AS sites_completed,
    sites_cancelled AS sites_cancelled,
    sites_active AS sites_active,
    sites_proposed AS sites_proposed,
    sites_planned AS sites_planned,
    sites_total_der AS sites_total_der,
    product_der AS product_der,
    business_category AS business_category,
    study_status_indicator AS study_status_indicator,
    study_phase_lifecycle AS study_phase_lifecycle,
    lead_clinician AS lead_clinician,
    study_point_of_contact AS study_point_of_contact,
    extension_study AS extension_study,
    internalized_study AS internalized_study,
    indication_preferred_term_list AS indication_preferred_term_list,
    acquisition_date AS acquisition_date,
    study_end_date_source AS study_end_date_source,
    partner_binned AS partner_binned,
    partner_allocated AS partner_allocated,
    msa_vendor_name AS msa_vendor_name,
    tier1_provider AS tier1_provider,
    opco_agmt_min_date AS opco_agmt_min_date,
    opco_agmt_max_date AS opco_agmt_max_date,
    opco_study_start AS opco_study_start,
    cost_division AS cost_division,
    study_next_milestone AS study_next_milestone,
    study_next_milestone_date AS study_next_milestone_date,
    gov_tracked_asset AS gov_tracked_asset,
    goal_detail_list AS goal_detail_list,
    bic_scope AS bic_scope,
    bic_scope_working AS bic_scope_working,
    mop_tl_previous AS mop_tl_previous,
    mop_lastsaved_dt AS mop_lastsaved_dt,
    dvso_approved_plan AS dvso_approved_plan,
    candidate_division AS candidate_division,
    candidate_status AS candidate_status,
    candidate_sub_division AS candidate_sub_division,
    candidate_sub_unit AS candidate_sub_unit,
    compound_type_der AS compound_type_der,
    mechanism_of_action AS mechanism_of_action,
    pacd AS pacd,
    cd_subj_min_age_yr_der AS cd_subj_min_age_yr_der,
    cd_subj_max_age_yr_der AS cd_subj_max_age_yr_der,
    critical_flag AS critical_flag,
    critical_flag_detail AS critical_flag_detail,
    recruitment_review_meeting AS recruitment_review_meeting,
    study_goal AS study_goal,
    ssr_study AS ssr_study,
    cd_primary_exclude_der AS cd_primary_exclude_der,
    cd_trial_type_final_der AS cd_trial_type_final_der,
    cd_clinpharm_study_der AS cd_clinpharm_study_der,
    cd_dev_japan_study_flag AS cd_dev_japan_study_flag,
    cd_dev_china_study_flag AS cd_dev_china_study_flag,
    cd_pcru_study_flag AS cd_pcru_study_flag,
    cd_concat_pcru_named_site_flag AS cd_concat_pcru_named_site_flag,
    cd_pcrc_study_flag AS cd_pcrc_study_flag,
    cd_concat_pcrc_named_site_flag AS cd_concat_pcrc_named_site_flag,
    override_summary_list AS override_summary_list,
    country_list_terminated AS country_list_terminated,
    planned_country_list AS planned_country_list,
    country_list_active AS country_list_active,
    country_list_cancelled AS country_list_cancelled,
    country_list_completed AS country_list_completed,
    country_list_planned AS country_list_planned,
    country_list_proposed AS country_list_proposed,
    candidate_disease_area AS candidate_disease_area,
    candidate_finance_code AS candidate_finance_code,
    candidate_phase AS candidate_phase,
    candidate_therapeutic_area AS candidate_therapeutic_area,
    candidate_type AS candidate_type,
    primary_indication_list AS primary_indication_list,
    compound_source AS compound_source,
    compound_number AS compound_number,
    compound_type AS compound_type,
    compound_acquired_company_name AS compound_acquired_company_name,
    compound_type_binned AS compound_type_binned,
    snapshot_date AS snapshot_date,
    study_end_date AS study_end_date,
    mytrial_model AS mytrial_model,
    mop_lastsaved_by AS mop_lastsaved_by,
    dvso_partial_plan_flag AS dvso_partial_plan_flag,
    country_list_selected AS country_list_selected,
    country_list_activated AS country_list_activated,
    country_list_unknown_legacy_status AS country_list_unknown_legacy_status,
    study_min_site_activated_dt AS study_min_site_activated_dt,
    study_min_derived_site_activated_dt AS study_min_derived_site_activated_dt,
    study_min_derived_site_activated_dt_source AS study_min_derived_site_activated_dt_source,
    enrollment_indicator AS enrollment_indicator,
    activation_plan_to_date AS activation_plan_to_date,
    activation_plan_to_date_src AS activation_plan_to_date_src,
    activation_plan AS activation_plan,
    activation_plan_src AS activation_plan_src,
    activation_to_date_variance AS activation_to_date_variance,
    activation_to_date_fraction AS activation_to_date_fraction,
    activation_total_variance AS activation_total_variance,
    activation_indicator AS activation_indicator,
    us_patients AS us_patients,
    goals_approach AS goals_approach,
    black_or_aa_goal AS black_or_aa_goal,
    black_or_aa_pct AS black_or_aa_pct,
    black_or_aa_variance_from_goal AS black_or_aa_variance_from_goal,
    black_or_aa_against_goal AS black_or_aa_against_goal,
    american_indian_or_alaskan_goal AS american_indian_or_alaskan_goal,
    american_indian_or_alaskan_pct AS american_indian_or_alaskan_pct,
    american_indian_or_alaskan_variance_from_goal AS american_indian_or_alaskan_variance_from_goal,
    american_indian_or_alaskan_against_goal AS american_indian_or_alaskan_against_goal,
    native_hawaiian_or_pacific_goal AS native_hawaiian_or_pacific_goal,
    native_hawaiian_or_pacific_pct AS native_hawaiian_or_pacific_pct,
    native_hawaiian_or_pacific_variance_from_goal AS native_hawaiian_or_pacific_variance_from_goal,
    native_hawaiian_or_pacific_against_goal AS native_hawaiian_or_pacific_against_goal,
    asian_goal AS asian_goal,
    asian_pct AS asian_pct,
    asian_variance_from_goal AS asian_variance_from_goal,
    asian_against_goal AS asian_against_goal,
    hispanic_goal AS hispanic_goal,
    hispanic_pct AS hispanic_pct,
    hispanic_variance_from_goal AS hispanic_variance_from_goal,
    hispanic_against_goal AS hispanic_against_goal,
    cd_diversity_tower_inc_exc_details AS cd_diversity_tower_inc_exc_details,
    cd_diversity_tower_inc_exc_logic AS cd_diversity_tower_inc_exc_logic
  
  FROM Join_370_left_UnionLeftOuter AS in0

)

SELECT *

FROM AlteryxSelect_584
