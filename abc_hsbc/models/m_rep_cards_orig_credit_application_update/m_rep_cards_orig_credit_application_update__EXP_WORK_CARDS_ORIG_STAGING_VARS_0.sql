{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

),

SQ_WORK_CARDS_ORIG_STAGING AS (

  SELECT 
    /*+ parallel(src_final) parallel(tgt) use_hash(src_final tgt)*/
    src_final.*,
    tgt.accid AS accid_tgt,
    tgt.initial_decision_coarse_tgt,
    tgt.initial_decision_primary_tgt,
    tgt.final_decision_coarse_tgt,
    tgt.final_decision_primary_tgt,
    tgt.efrom AS efrom_tgt,
    tgt.action_date AS action_date_tgt
  
  FROM (
    SELECT 
      /*+ parallel(aa) parallel(src) use_hash(aa src)*/
      aa.accid AS accid_src,
      src.*
    
    FROM (
      SELECT 
        wrk.*,
        wrk_initial.initial_application_status AS initial_application_status_src,
        wrk_final.final_application_status AS final_application_status_src
      
      FROM (
        SELECT wrk_temp.*
        
        FROM (
          SELECT 
            application_number AS application_number,
            application_status AS application_status,
            employment_status,
            cdd_result,
            rts_transaction_status,
            identity_verification_status,
            fraud_result,
            credit_decision_result,
            marital_status,
            application_store_number,
            application_date,
            substr(account_number, -10),
            product,
            application_source_code,
            application_score,
            ciiscore,
            delphi_score,
            first_party_fraud_score,
            credit_decision_annual_percentage_rate,
            residential_status,
            annual_income_amount,
            residential_moved_in_date,
            previous_addresses_moved_in_date,
            occupation,
            employment_main_startdate,
            credit_decision_final_credit_limit_offer,
            offered_limit_amount,
            annual_salary_amount,
            offer_id,
            customer_number,
            employment_role,
            product_number,
            number_of_dependents,
            credit_requested_amount,
            email,
            marketing_preferences_preferred_channels,
            postcode,
            date_of_birth,
            foresee_changes_to_personal_circumstances,
            last_action_date AS last_action_date_max,
            substr(TO_CHAR(last_action_date), 11, 2)
            || substr(TO_CHAR(last_action_date), 14, 2)
            || substr(TO_CHAR(last_action_date), 17, 2) AS time_stamp_of_action,
            ROW_NUMBER() OVER (PARTITION BY wrk.application_number ORDER BY last_action_date DESC) row_num
          
          FROM work_cards_orig_staging AS wrk
        ) AS wrk_temp
        
        WHERE wrk_temp.row_num = 1
      ) AS wrk, (
        SELECT 
          DISTINCT application_number,
          application_status AS initial_application_status
        
        FROM work_cards_orig_staging
        
        WHERE (application_number, last_action_date) IN (
                SELECT 
                  application_number,
                  MIN(last_action_date)
                
                FROM work_cards_orig_staging
                
                WHERE application_status != 'PRODUCT_ONBOARDING_COMPLETED'
                
                GROUP BY application_number
               )
      ) AS wrk_initial, (
        SELECT 
          DISTINCT application_number,
          application_status AS final_application_status
        
        FROM work_cards_orig_staging
        
        WHERE (application_number, last_action_date) IN (
                SELECT 
                  application_number,
                  MAX(last_action_date)
                
                FROM work_cards_orig_staging
                
                WHERE application_status != 'PRODUCT_ONBOARDING_COMPLETED'
                
                GROUP BY application_number
               )
      ) AS wrk_final
      
      WHERE wrk.application_number = wrk_initial.application_number (+)
            AND wrk.application_number = wrk_final.application_number (+)
    ) AS src, account_alias AS aa
    
    WHERE aa.account_alias_id = src.application_number AND aa.eto IS NULL AND aa.class = '563'
  ) AS src_final, (
    SELECT 
      /*parallel(tgt)*/
      accid,
      tgt.initial_decision_coarse AS initial_decision_coarse_tgt,
      tgt.initial_decision_primary AS initial_decision_primary_tgt,
      tgt.final_decision_coarse AS final_decision_coarse_tgt,
      tgt.final_decision_primary AS final_decision_primary_tgt,
      tgt.efrom,
      action_date
    
    FROM credit_application AS tgt
    
    WHERE eto IS NULL
  ) AS tgt
  
  WHERE src_final.accid_src = tgt.accid (+)

),

SQ_WORK_CARDS_ORIG_STAGING_EXPR_19 AS (

  SELECT 
    PRODUCT_NUMBER AS PRODUCT_NUMBER,
    NUMBER_OF_DEPENDENTS AS NUMBER_OF_DEPENDENTS,
    OFFERED_LIMIT_AMOUNT AS in_OFFERED_LIMIT_AMOUNT,
    CREDIT_REQUESTED_AMOUNT AS CREDIT_REQUESTED_AMOUNT,
    EMAIL AS EMAIL,
    MARKETING_PREFERENCES_PREFERRED_CHANNELS AS MARKETING_PREFERENCES_PREFERRED_CHANNELS,
    POSTCODE AS POSTCODE,
    DATE_OF_BIRTH AS DATE_OF_BIRTH,
    FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES AS FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES,
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    APPLICATION_STATUS AS APPLICATION_STATUS,
    RESIDENTIAL_MOVED_IN_DATE AS RESIDENTIAL_MOVED_IN_DATE,
    PREVIOUS_ADDRESSES_MOVED_IN_DATE AS in_PREVIOUS_ADDRESSES_MOVED_IN_DATE,
    EMPLOYMENT_MAIN_STARTDATE AS EMPLOYMENT_MAIN_STARTDATE,
    ROW_NUM AS ROW_NUM,
    ANNUAL_INCOME_AMOUNT AS ANNUAL_INCOME_AMOUNT1,
    TIME_STAMP_OF_ACTION AS TIME_STAMP_OF_ACTION,
    ACCID_TGT AS ACCID_TGT,
    APPLICATION_STATUS2 AS APPLICATION_STATUS1,
    INITIAL_DECISION_COARSE_TGT AS INITIAL_DECISION_COARSE_TGT,
    INITIAL_DECISION_PRIMARY_TGT AS INITIAL_DECISION_PRIMARY_TGT,
    FINAL_DECISION_COARSE_TGT AS FINAL_DECISION_COARSE_TGT,
    FINAL_DECISION_PRIMARY_TGT AS FINAL_DECISION_PRIMARY_TGT,
    APPLICATION_STATUS1 AS APPLICATION_STATUS2,
    LAST_ACTION_DATE AS LAST_ACTION_DATE,
    EFROM_TGT AS EFROM_TGT,
    ACTION_DATE_TGT AS ACTION_DATE_TGT,
    ACCID_SRC AS ACCID_SRC,
    APPLICATION_DATE AS APPLICATION_DATE,
    EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS,
    CDD_RESULT AS CDD_RESULT,
    RTS_TRANSACTION_STATUS AS RTS_TRANSACTION_STATUS,
    IDENTITY_VERIFICATION_STATUS AS IDENTITY_VERIFICATION_STATUS,
    FRAUD_RESULT AS FRAUD_RESULT,
    CREDIT_DECISION_RESULT AS CREDIT_DECISION_RESULT,
    MARITAL_STATUS AS MARITAL_STATUS,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
    PRODUCT AS PRODUCT,
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    APPLICATION_SCORE AS APPLICATION_SCORE,
    CIISCORE AS CIISCORE,
    DELPHI_SCORE AS DELPHI_SCORE,
    FIRST_PARTY_FRAUD_SCORE AS FIRST_PARTY_FRAUD_SCORE,
    CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE AS in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE,
    RESIDENTIAL_STATUS AS RESIDENTIAL_STATUS,
    OCCUPATION AS OCCUPATION1,
    CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER AS in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER,
    ANNUAL_SALARY_AMOUNT AS ANNUAL_SALARY_AMOUNT,
    OFFER_ID AS OFFER_ID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    EMPLOYMENT_ROLE AS EMPLOYMENT_ROLE
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

),

EXP_WORK_CARDS_ORIG_STAGING_LOOKUP_24 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.PRODUCT_NUMBER AS PRODUCT_NUMBER,
    in1.NUMBER_OF_DEPENDENTS AS NUMBER_OF_DEPENDENTS,
    in1.in_OFFERED_LIMIT_AMOUNT AS in_OFFERED_LIMIT_AMOUNT,
    in1.CREDIT_REQUESTED_AMOUNT AS CREDIT_REQUESTED_AMOUNT,
    in1.EMAIL AS EMAIL,
    in1.MARKETING_PREFERENCES_PREFERRED_CHANNELS AS MARKETING_PREFERENCES_PREFERRED_CHANNELS,
    in1.POSTCODE AS POSTCODE,
    in1.DATE_OF_BIRTH AS DATE_OF_BIRTH,
    in1.FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES AS FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES,
    in1.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in1.APPLICATION_STATUS AS APPLICATION_STATUS,
    in1.RESIDENTIAL_MOVED_IN_DATE AS RESIDENTIAL_MOVED_IN_DATE,
    in1.in_PREVIOUS_ADDRESSES_MOVED_IN_DATE AS in_PREVIOUS_ADDRESSES_MOVED_IN_DATE,
    in1.EMPLOYMENT_MAIN_STARTDATE AS EMPLOYMENT_MAIN_STARTDATE,
    in1.ROW_NUM AS ROW_NUM,
    in1.ANNUAL_INCOME_AMOUNT1 AS ANNUAL_INCOME_AMOUNT1,
    in1.TIME_STAMP_OF_ACTION AS TIME_STAMP_OF_ACTION,
    in1.ACCID_TGT AS ACCID_TGT,
    in1.APPLICATION_STATUS1 AS APPLICATION_STATUS1,
    in1.INITIAL_DECISION_COARSE_TGT AS INITIAL_DECISION_COARSE_TGT,
    in1.INITIAL_DECISION_PRIMARY_TGT AS INITIAL_DECISION_PRIMARY_TGT,
    in1.FINAL_DECISION_COARSE_TGT AS FINAL_DECISION_COARSE_TGT,
    in1.FINAL_DECISION_PRIMARY_TGT AS FINAL_DECISION_PRIMARY_TGT,
    in1.APPLICATION_STATUS2 AS APPLICATION_STATUS2,
    in1.LAST_ACTION_DATE AS LAST_ACTION_DATE,
    in1.EFROM_TGT AS EFROM_TGT,
    in1.ACTION_DATE_TGT AS ACTION_DATE_TGT,
    in1.ACCID_SRC AS ACCID_SRC,
    in1.APPLICATION_DATE AS APPLICATION_DATE,
    in1.EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS,
    in1.CDD_RESULT AS CDD_RESULT,
    in1.RTS_TRANSACTION_STATUS AS RTS_TRANSACTION_STATUS,
    in1.IDENTITY_VERIFICATION_STATUS AS IDENTITY_VERIFICATION_STATUS,
    in1.FRAUD_RESULT AS FRAUD_RESULT,
    in1.CREDIT_DECISION_RESULT AS CREDIT_DECISION_RESULT,
    in1.MARITAL_STATUS AS MARITAL_STATUS,
    in1.APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    in1.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
    in1.PRODUCT AS PRODUCT,
    in1.APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    in1.APPLICATION_SCORE AS APPLICATION_SCORE,
    in1.CIISCORE AS CIISCORE,
    in1.DELPHI_SCORE AS DELPHI_SCORE,
    in1.FIRST_PARTY_FRAUD_SCORE AS FIRST_PARTY_FRAUD_SCORE,
    in1.in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE AS in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE,
    in1.RESIDENTIAL_STATUS AS RESIDENTIAL_STATUS,
    in1.OCCUPATION1 AS OCCUPATION1,
    in1.in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER AS in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER,
    in1.ANNUAL_SALARY_AMOUNT AS ANNUAL_SALARY_AMOUNT,
    in1.OFFER_ID AS OFFER_ID,
    in1.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.EMPLOYMENT_ROLE AS EMPLOYMENT_ROLE
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SQ_WORK_CARDS_ORIG_STAGING_EXPR_19 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_WORK_CARDS_ORIG_STAGING_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_WORK_CARDS_ORIG_STAGING_LOOKUP_24 AS in0

)

SELECT *

FROM EXP_WORK_CARDS_ORIG_STAGING_VARS_0
