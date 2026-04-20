{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_WORK_CARDS_ORIG_STAGING AS (

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

SQ_WORK_CARDS_ORIG_STAGING_EXPR_322 AS (

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

EXP_WORK_CARDS_ORIG_STAGING_VARS_0 AS (

  SELECT 
    (SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS count1,
    unknown_expression(
      '\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'businessdate\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\'') AS lookup_string,
    unknown_expression(
      'IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))') AS businessdate,
    unknown_expression(
      'if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL') AS business_date_string,
    *
  
  FROM SQ_WORK_CARDS_ORIG_STAGING_EXPR_322 AS in0

),

EXP_WORK_CARDS_ORIG_STAGING AS (

  SELECT 
    ACCID_SRC AS ACCID_SRC,
    APPLICATION_DATE AS APPLICATION_DATE,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    (
      CASE
        WHEN (CDD_RESULT = 'PASSED')
          THEN 'PASS'
        WHEN (CDD_RESULT = 'REFER')
          THEN 'REFE'
        WHEN (CDD_RESULT = 'FINALCOMPLETE')
          THEN 'FINA'
        WHEN (CDD_RESULT = 'DECLINED')
          THEN 'DECL'
        WHEN (CDD_RESULT = 'TIMEOUT')
          THEN 'TIMO'
        WHEN (CDD_RESULT = 'PEPAPPROVED')
          THEN 'PEPA'
        WHEN (CDD_RESULT = 'OPSTIMEOUT')
          THEN 'OPST'
        WHEN (CDD_RESULT = 'INPROGRESS')
          THEN 'INPR'
        WHEN (CDD_RESULT = 'INERROR')
          THEN 'INER'
        WHEN (CDD_RESULT = 'NOT_CHECKED')
          THEN 'NOTC'
        WHEN (CDD_RESULT = 'NOT_APPLICABLE')
          THEN 'NOTA'
        ELSE SUBSTRING(CDD_RESULT, 0, 4)
      END
    ) AS o_CDD_RESULT,
    (
      CASE
        WHEN (RTS_TRANSACTION_STATUS = 'PASSED')
          THEN 'PASS'
        WHEN (RTS_TRANSACTION_STATUS = 'REFER')
          THEN 'REFE'
        WHEN (RTS_TRANSACTION_STATUS = 'FINALCOMPLETE')
          THEN 'FINA'
        WHEN (RTS_TRANSACTION_STATUS = 'DECLINED')
          THEN 'DECL'
        WHEN (RTS_TRANSACTION_STATUS = 'TIMEOUT')
          THEN 'TIMO'
        WHEN (RTS_TRANSACTION_STATUS = 'PEPAPPROVED')
          THEN 'PEPA'
        WHEN (RTS_TRANSACTION_STATUS = 'OPSTIMEOUT')
          THEN 'OPST'
        WHEN (RTS_TRANSACTION_STATUS = 'INPROGRESS')
          THEN 'INPR'
        WHEN (RTS_TRANSACTION_STATUS = 'INERROR')
          THEN 'INER'
        WHEN (RTS_TRANSACTION_STATUS = 'NOT_CHECKED')
          THEN 'NOTC'
        WHEN (RTS_TRANSACTION_STATUS = 'NOT_APPLICABLE')
          THEN 'NOTA'
        ELSE SUBSTRING(RTS_TRANSACTION_STATUS, 0, 4)
      END
    ) AS o_RTS_TRANSACTION_STATUS,
    (
      CASE
        WHEN (IDENTITY_VERIFICATION_STATUS = 'PASSED')
          THEN 'PASS'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'REFER')
          THEN 'REFE'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'FAILED')
          THEN 'FAIL'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'DECLINED')
          THEN 'DECL'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'IN_PROGRESS')
          THEN 'INPR'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'ERROR')
          THEN 'ERRO'
        WHEN (IDENTITY_VERIFICATION_STATUS = 'NOT_CHECKED')
          THEN 'NOTC'
        ELSE SUBSTRING(IDENTITY_VERIFICATION_STATUS, 0, 4)
      END
    ) AS o_in_IDENTITY_VERIFICATION_STATUS,
    (
      CASE
        WHEN (FRAUD_RESULT = 'PASSED')
          THEN 'PASS'
        WHEN (FRAUD_RESULT = 'REFER')
          THEN 'REFE'
        WHEN (FRAUD_RESULT = 'DECLINE')
          THEN 'DECL'
        ELSE SUBSTRING(FRAUD_RESULT, 0, 4)
      END
    ) AS o_FRAUD_RESULT,
    (
      CASE
        WHEN (CREDIT_DECISION_RESULT = 'ACCEPT')
          THEN 'ACCE'
        WHEN (CREDIT_DECISION_RESULT = 'REFER')
          THEN 'REFE'
        WHEN (CREDIT_DECISION_RESULT = 'REJECT')
          THEN 'REJE'
        WHEN (CREDIT_DECISION_RESULT = 'NOT_CHECKED')
          THEN 'NOTC'
        ELSE SUBSTRING(CREDIT_DECISION_RESULT, 0, 4)
      END
    ) AS o_in_CREDIT_DECISION_RESULT,
    ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    APPLICATION_STATUS AS APPLICATION_STATUS,
    'MS' AS o_PRODUCT,
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    DATE_OF_BIRTH AS DATE_OF_BIRTH,
    POSTCODE AS POSTCODE,
    (
      CASE
        WHEN (ANNUAL_INCOME_AMOUNT1 IS NULL)
          THEN 0
        ELSE ANNUAL_INCOME_AMOUNT1
      END
    ) AS ANNUAL_INCOME_AMOUNT,
    'T' AS APPLICATION_TYPE,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    (
      CASE
        WHEN (EMPLOYMENT_ROLE = 'BUSINESS_OWNER')
          THEN 'B'
        WHEN (EMPLOYMENT_ROLE = 'KEY_CONTROLLER')
          THEN 'K'
        WHEN (EMPLOYMENT_ROLE = 'EMPLOYEE')
          THEN 'E'
        WHEN (EMPLOYMENT_ROLE = 'SOLE_TRADER')
          THEN 'S'
        ELSE SUBSTRING(EMPLOYMENT_ROLE, 0, 1)
      END
    ) AS o_EMPLOYMENT_ROLE,
    (
      CASE
        WHEN (EMPLOYMENT_STATUS = 'AT_HOME')
          THEN 'AH'
        WHEN (EMPLOYMENT_STATUS = 'EMPLOYED_FULL_TIME')
          THEN 'EF'
        WHEN (EMPLOYMENT_STATUS = 'EMPLOYED_PART_TIME')
          THEN 'EP'
        WHEN (EMPLOYMENT_STATUS = 'SELF_EMPLOYED')
          THEN 'SE'
        WHEN (EMPLOYMENT_STATUS = 'RETIRED')
          THEN 'RT'
        WHEN (EMPLOYMENT_STATUS = 'DISABLED')
          THEN 'DI'
        WHEN (EMPLOYMENT_STATUS = 'STUDENT')
          THEN 'ST'
        WHEN (EMPLOYMENT_STATUS = 'UNEMPLOYED')
          THEN 'UN'
        ELSE SUBSTRING(EMPLOYMENT_STATUS, 0, 2)
      END
    ) AS o_EMPLOYMENT_STATUS,
    (
      CASE
        WHEN (MARITAL_STATUS = 'MARRIED')
          THEN 'M'
        WHEN (MARITAL_STATUS = 'SINGLE')
          THEN 'S'
        WHEN (MARITAL_STATUS = 'DIVORCED')
          THEN 'D'
        WHEN (MARITAL_STATUS = 'LIVING_WITH_PARTNER')
          THEN 'P'
        WHEN (MARITAL_STATUS = 'WIDOWED')
          THEN 'W'
        ELSE SUBSTRING(MARITAL_STATUS, 0, 1)
      END
    ) AS o_MARITAL_STATUS,
    (
      CASE
        WHEN (NUMBER_OF_DEPENDENTS IS NULL)
          THEN 0
        ELSE NUMBER_OF_DEPENDENTS
      END
    ) AS NUMBER_OF_CHILDREN,
    (
      CASE
        WHEN (RESIDENTIAL_STATUS = 'OTHER')
          THEN 'O'
        WHEN (RESIDENTIAL_STATUS = 'OWNER')
          THEN 'W'
        WHEN (RESIDENTIAL_STATUS = 'TENANT')
          THEN 'T'
        WHEN (RESIDENTIAL_STATUS = 'WITH_PARENTS')
          THEN 'P'
        ELSE SUBSTRING(RESIDENTIAL_STATUS, 0, 1)
      END
    ) AS o_RESIDENTIAL_STATUS,
    CAST(NULL AS STRING) AS OCCUPATION,
    PRODUCT_NUMBER AS PRODUCT_NUMBER,
    EMAIL AS EMAIL,
    (
      CASE
        WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'EMAIL')
          THEN 'E'
        WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'SMS')
          THEN 'S'
        WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'CALL')
          THEN 'C'
        WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'POST')
          THEN 'P'
        ELSE SUBSTRING(MARKETING_PREFERENCES_PREFERRED_CHANNELS, 0, 1)
      END
    ) AS o_CHANNEL_PREFERENCE_INDICATOR,
    APPLICATION_SCORE AS APPLICATION_SCORE,
    CIISCORE AS CIISCORE,
    DELPHI_SCORE AS DELPHI_SCORE,
    FIRST_PARTY_FRAUD_SCORE AS FIRST_PARTY_FRAUD_SCORE,
    (
      CASE
        WHEN (in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE IS NULL)
          THEN 0
        ELSE in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE
      END
    ) AS o_in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE,
    RESIDENTIAL_MOVED_IN_DATE AS RESIDENTIAL_MOVED_IN_DATE,
    unknown_expression(
      'IIF(ISNULL(RESIDENTIAL_MOVED_IN_DATE), \'\',   IIF( LENGTH(DATE_DIFF(TO_DATE(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,\'yyyymmdd\'),                                                       TO_DATE      (RESIDENTIAL_MOVED_IN_DATE||\'01\',\'yyyymmdd\'), 		                                          \'DDD\'                                                         ) ) >4, \'9999\', TO_CHAR(DATE_DIFF(TO_DATE(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,\'yyyymmdd\'),                                                       TO_DATE      (RESIDENTIAL_MOVED_IN_DATE||\'01\',\'yyyymmdd\'), 		                                          \'DDD\'                                                         )                         )        ) )') AS o_RESIDENTIAL_MOVED_IN_DATE,
    in_PREVIOUS_ADDRESSES_MOVED_IN_DATE AS in_PREVIOUS_ADDRESSES_MOVED_IN_DATE,
    (
      CASE
        WHEN (in_PREVIOUS_ADDRESSES_MOVED_IN_DATE IS NULL)
          THEN ''
        WHEN (
          (
            LENGTH(
              (
                DATE_DIFF(
                  (PARSE_TIMESTAMP('%Y%m%d', (CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')))), 
                  (PARSE_TIMESTAMP('%Y%m%d', (CONCAT(in_PREVIOUS_ADDRESSES_MOVED_IN_DATE, '01')))), 
                  DAY)
              ))
          ) > 4
        )
          THEN '9999'
        ELSE CAST((
          DATE_DIFF(
            (PARSE_TIMESTAMP('%Y%m%d', (CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')))), 
            (PARSE_TIMESTAMP('%Y%m%d', (CONCAT(in_PREVIOUS_ADDRESSES_MOVED_IN_DATE, '01')))), 
            DAY)
        ) AS STRING)
      END
    ) AS o_TIME_AT_PREVIOUS_ADDRESS,
    EMPLOYMENT_MAIN_STARTDATE AS EMPLOYMENT_MAIN_STARTDATE,
    unknown_expression(
      'IIF(ISNULL(EMPLOYMENT_MAIN_STARTDATE), \'\', IIF( LENGTH(DATE_DIFF(TO_DATE(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,\'yyyymmdd\'),                                                       TO_DATE      (EMPLOYMENT_MAIN_STARTDATE||\'01\',\'yyyymmdd\'), 		                                          \'DDD\'                                                         ) ) >4, \'9999\', TO_CHAR(DATE_DIFF(TO_DATE(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (count1 == 1) find_business_date(lookup_string) else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (count1 == 1) find_business_date(lookup_string) else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,\'yyyymmdd\'),                                                       TO_DATE      (EMPLOYMENT_MAIN_STARTDATE||\'01\',\'yyyymmdd\'), 		                                          \'DDD\'                                                         )                         ) ) )') AS o_EMPLOYMENT_MAIN_STARTDATE,
    (
      CASE
        WHEN (in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER IS NULL)
          THEN 0
        ELSE in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER
      END
    ) AS o_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER,
    OCCUPATION1 AS OCCUPATION1,
    (
      CASE
        WHEN (in_OFFERED_LIMIT_AMOUNT IS NULL)
          THEN 0
        ELSE in_OFFERED_LIMIT_AMOUNT
      END
    ) AS o_OFFERED_LIMIT_AMOUNT,
    (
      CASE
        WHEN (ANNUAL_SALARY_AMOUNT IS NULL)
          THEN 0
        ELSE ANNUAL_SALARY_AMOUNT
      END
    ) AS out_ANNUAL_SALARY_AMOUNT,
    OFFER_ID AS OFFER_ID,
    CREDIT_REQUESTED_AMOUNT AS CREDIT_REQUESTED_AMOUNT,
    FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES AS FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES,
    ROW_NUM AS ROW_NUM,
    unknown_expression(
      'IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))') AS BUSINESS_DATE,
    unknown_expression(
      'ADD_TO_DATE(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\')),\'DD\',-1)') AS BUSINESS_DATE_MINUS_1,
    unknown_expression(
      'if (over(sum(1), partitionBy(1), orderBy(1)) == 1) feed_number_generator1(\'U\',\'360\',if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,substr(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1))), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (over(lag(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (if (over(sum(1), partitionBy(1), orderBy(1)) == 1) find_business_date(\'over(lag(IIF(ISNULL(business_date_string), ABORT(\'No Business Date found on dbattribute\'), TO_DATE(SUBSTR (business_date_string,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL), partitionBy(1), orderBy(1)),1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8),\'YYYYMMDD\'))), partitionBy(1), orderBy(1))\') else (string)NULL,1,8)) else (string)NULL') AS FEED_UPDATE_ID,
    ACCID_TGT AS ACCID_TGT,
    APPLICATION_STATUS1 AS APPLICATION_STATUS1,
    (
      CASE
        WHEN (APPLICATION_STATUS1 = 'CANCELLED')
          THEN 'C'
        WHEN (APPLICATION_STATUS1 = 'APPROVED')
          THEN 'A'
        WHEN (APPLICATION_STATUS1 = 'ABORTED')
          THEN 'X'
        WHEN (APPLICATION_STATUS1 = 'REFERRED')
          THEN 'R'
        WHEN (APPLICATION_STATUS1 = 'DECLINED')
          THEN 'D'
        ELSE NULL
      END
    ) AS o_INITIAL_DECISION_COARSE_SRC,
    (
      CASE
        WHEN (APPLICATION_STATUS1 = 'CANCELLED')
          THEN 'CANC'
        WHEN (APPLICATION_STATUS1 = 'APPROVED')
          THEN 'APPR'
        WHEN (APPLICATION_STATUS1 = 'ABORTED')
          THEN 'ABOR'
        WHEN (APPLICATION_STATUS1 = 'REFERRED')
          THEN 'REFE'
        WHEN (APPLICATION_STATUS1 = 'DECLINED')
          THEN 'DECL'
        ELSE NULL
      END
    ) AS o_INITIAL_DECISION_PRIMARY_SRC,
    APPLICATION_STATUS2 AS APPLICATION_STATUS2,
    INITIAL_DECISION_COARSE_TGT AS INITIAL_DECISION_COARSE_TGT,
    INITIAL_DECISION_PRIMARY_TGT AS INITIAL_DECISION_PRIMARY_TGT,
    FINAL_DECISION_COARSE_TGT AS FINAL_DECISION_COARSE_TGT,
    FINAL_DECISION_PRIMARY_TGT AS FINAL_DECISION_PRIMARY_TGT,
    (
      CASE
        WHEN (
          (
            (
              CASE
                WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                  THEN 'C'
                WHEN (APPLICATION_STATUS2 = 'APPROVED')
                  THEN 'A'
                WHEN (APPLICATION_STATUS2 = 'ABORTED')
                  THEN 'X'
                WHEN (APPLICATION_STATUS2 = 'REFERRED')
                  THEN 'R'
                WHEN (APPLICATION_STATUS2 = 'DECLINED')
                  THEN 'D'
                ELSE NULL
              END
            ) IS NULL
          )
          AND (FINAL_DECISION_COARSE_TGT IS NOT NULL)
        )
          THEN FINAL_DECISION_COARSE_TGT
        WHEN (
          (
            (
              CASE
                WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                  THEN 'C'
                WHEN (APPLICATION_STATUS2 = 'APPROVED')
                  THEN 'A'
                WHEN (APPLICATION_STATUS2 = 'ABORTED')
                  THEN 'X'
                WHEN (APPLICATION_STATUS2 = 'REFERRED')
                  THEN 'R'
                WHEN (APPLICATION_STATUS2 = 'DECLINED')
                  THEN 'D'
                ELSE NULL
              END
            ) IS NULL
          )
          AND (FINAL_DECISION_COARSE_TGT IS NULL)
        )
          THEN CAST((
            CASE
              WHEN (APPLICATION_STATUS1 = 'CANCELLED')
                THEN 'C'
              WHEN (APPLICATION_STATUS1 = 'APPROVED')
                THEN 'A'
              WHEN (APPLICATION_STATUS1 = 'ABORTED')
                THEN 'X'
              WHEN (APPLICATION_STATUS1 = 'REFERRED')
                THEN 'R'
              WHEN (APPLICATION_STATUS1 = 'DECLINED')
                THEN 'D'
              ELSE NULL
            END
          ) AS STRING)
        WHEN (APPLICATION_STATUS2 = 'CANCELLED')
          THEN 'C'
        WHEN (APPLICATION_STATUS2 = 'APPROVED')
          THEN 'A'
        WHEN (APPLICATION_STATUS2 = 'ABORTED')
          THEN 'X'
        WHEN (APPLICATION_STATUS2 = 'REFERRED')
          THEN 'R'
        WHEN (APPLICATION_STATUS2 = 'DECLINED')
          THEN 'D'
        ELSE NULL
      END
    ) AS o_FINAL_DECISION_COARSE,
    (
      CASE
        WHEN (
          (
            (
              CASE
                WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                  THEN 'CANC'
                WHEN (APPLICATION_STATUS2 = 'APPROVED')
                  THEN 'APPR'
                WHEN (APPLICATION_STATUS2 = 'ABORTED')
                  THEN 'ABOR'
                WHEN (APPLICATION_STATUS2 = 'REFERRED')
                  THEN 'REFE'
                WHEN (APPLICATION_STATUS2 = 'DECLINED')
                  THEN 'DECL'
                ELSE NULL
              END
            ) IS NULL
          )
          AND (FINAL_DECISION_PRIMARY_TGT IS NOT NULL)
        )
          THEN FINAL_DECISION_PRIMARY_TGT
        WHEN (
          (
            (
              CASE
                WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                  THEN 'CANC'
                WHEN (APPLICATION_STATUS2 = 'APPROVED')
                  THEN 'APPR'
                WHEN (APPLICATION_STATUS2 = 'ABORTED')
                  THEN 'ABOR'
                WHEN (APPLICATION_STATUS2 = 'REFERRED')
                  THEN 'REFE'
                WHEN (APPLICATION_STATUS2 = 'DECLINED')
                  THEN 'DECL'
                ELSE NULL
              END
            ) IS NULL
          )
          AND (FINAL_DECISION_PRIMARY_TGT IS NULL)
        )
          THEN CAST((
            CASE
              WHEN (APPLICATION_STATUS1 = 'CANCELLED')
                THEN 'CANC'
              WHEN (APPLICATION_STATUS1 = 'APPROVED')
                THEN 'APPR'
              WHEN (APPLICATION_STATUS1 = 'ABORTED')
                THEN 'ABOR'
              WHEN (APPLICATION_STATUS1 = 'REFERRED')
                THEN 'REFE'
              WHEN (APPLICATION_STATUS1 = 'DECLINED')
                THEN 'DECL'
              ELSE NULL
            END
          ) AS STRING)
        WHEN (APPLICATION_STATUS2 = 'CANCELLED')
          THEN 'CANC'
        WHEN (APPLICATION_STATUS2 = 'APPROVED')
          THEN 'APPR'
        WHEN (APPLICATION_STATUS2 = 'ABORTED')
          THEN 'ABOR'
        WHEN (APPLICATION_STATUS2 = 'REFERRED')
          THEN 'REFE'
        WHEN (APPLICATION_STATUS2 = 'DECLINED')
          THEN 'DECL'
        ELSE NULL
      END
    ) AS o_FINAL_DECISION_PRIMARY,
    LAST_ACTION_DATE AS LAST_ACTION_DATE,
    TIME_STAMP_OF_ACTION AS TIME_STAMP_OF_ACTION,
    (
      CONCAT(
        SUBSTRING(LAST_ACTION_DATE, 10, 2), 
        SUBSTRING(LAST_ACTION_DATE, 13, 2), 
        SUBSTRING(LAST_ACTION_DATE, 16, 2))
    ) AS o_TIME_STAMP_OF_ACTION,
    ACTION_DATE_TGT AS ACTION_DATE_TGT,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                THEN 'CANC'
              WHEN (APPLICATION_STATUS2 = 'APPROVED')
                THEN 'APPR'
              WHEN (APPLICATION_STATUS2 = 'ABORTED')
                THEN 'ABOR'
              WHEN (APPLICATION_STATUS2 = 'REFERRED')
                THEN 'REFE'
              WHEN (APPLICATION_STATUS2 = 'DECLINED')
                THEN 'DECL'
              ELSE NULL
            END
          ) IS NULL
        )
          THEN ACTION_DATE_TGT
        WHEN (
          CAST((
            CASE
              WHEN (APPLICATION_STATUS2 = 'CANCELLED')
                THEN 'CANC'
              WHEN (APPLICATION_STATUS2 = 'APPROVED')
                THEN 'APPR'
              WHEN (APPLICATION_STATUS2 = 'ABORTED')
                THEN 'ABOR'
              WHEN (APPLICATION_STATUS2 = 'REFERRED')
                THEN 'REFE'
              WHEN (APPLICATION_STATUS2 = 'DECLINED')
                THEN 'DECL'
              ELSE NULL
            END
          ) AS STRING) = FINAL_DECISION_PRIMARY_TGT
        )
          THEN ACTION_DATE_TGT
        ELSE LAST_ACTION_DATE
      END
    ) AS o_LAST_ACTION_DATE,
    EFROM_TGT AS EFROM_TGT,
    2 AS CARD_ORIGIN
  
  FROM EXP_WORK_CARDS_ORIG_STAGING_VARS_0 AS in0

),

EXP_WORK_CARDS_ORIG_STAGING_EXPR_321 AS (

  SELECT 
    o_FRAUD_RESULT AS INITIAL_DECISION_5,
    o_in_CREDIT_DECISION_RESULT AS INITIAL_DECISION_6,
    o_MARITAL_STATUS AS MARITAL_STATUS,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    APPLICATION_SCORE AS APPLICATION_SCORE,
    CIISCORE AS CIISCORE,
    DELPHI_SCORE AS DELPHI_SCORE,
    FIRST_PARTY_FRAUD_SCORE AS FIRST_PARTY_FRAUD_SCORE,
    LAST_ACTION_DATE AS LAST_ACTION_DATE1,
    o_RESIDENTIAL_STATUS AS RESIDENTIAL_STATUS,
    OCCUPATION1 AS OCCUPATION,
    APPLICATION_TYPE AS APPLICATION_TYPE,
    OFFER_ID AS OFFER_ID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    o_EMPLOYMENT_ROLE AS CUSTOMER_ROLE,
    PRODUCT_NUMBER AS PRODUCT_NUMBER,
    o_PRODUCT AS PRODUCT,
    NUMBER_OF_CHILDREN AS NUMBER_OF_CHILDREN,
    CREDIT_REQUESTED_AMOUNT AS CREDIT_REQUESTED_AMOUNT,
    EMAIL AS EMAIL,
    o_CHANNEL_PREFERENCE_INDICATOR AS CHANNEL_PREFERENCE_INDICATOR,
    POSTCODE AS POSTCODE,
    DATE_OF_BIRTH AS DATE_OF_BIRTH,
    FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES AS FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES,
    ANNUAL_INCOME_AMOUNT AS ANNUAL_INCOME_AMOUNT,
    out_ANNUAL_SALARY_AMOUNT AS ANNUAL_HOUSEHOLD_INCOME,
    ACCID_TGT AS ACCID_TGT,
    BUSINESS_DATE AS BUSINESS_DATE,
    BUSINESS_DATE_MINUS_1 AS BUSINESS_DATE_MINUS_1,
    o_INITIAL_DECISION_COARSE_SRC AS INITIAL_DECISION_COARSE_SRC,
    o_INITIAL_DECISION_PRIMARY_SRC AS INITIAL_DECISION_PRIMARY_SRC,
    INITIAL_DECISION_COARSE_TGT AS INITIAL_DECISION_COARSE_TGT,
    INITIAL_DECISION_PRIMARY_TGT AS INITIAL_DECISION_PRIMARY_TGT,
    o_FINAL_DECISION_COARSE AS o_FINAL_DECISION_COARSE,
    o_FINAL_DECISION_PRIMARY AS o_FINAL_DECISION_PRIMARY,
    LAST_ACTION_DATE AS LAST_ACTION_DATE,
    EFROM_TGT AS EFROM_TGT,
    o_LAST_ACTION_DATE AS o_LAST_ACTION_DATE,
    ACCID_SRC AS ACCID_SRC,
    APPLICATION_DATE AS APPLICATION_DATE,
    o_EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS,
    o_CDD_RESULT AS INITIAL_DECISION_2,
    o_RTS_TRANSACTION_STATUS AS INITIAL_DECISION_3,
    o_in_IDENTITY_VERIFICATION_STATUS AS INITIAL_DECISION_4,
    o_TIME_AT_PREVIOUS_ADDRESS AS o_TIME_AT_PREVIOUS_ADDRESS,
    o_EMPLOYMENT_MAIN_STARTDATE AS o_EMPLOYMENT_MAIN_STARTDATE,
    o_RESIDENTIAL_MOVED_IN_DATE AS o_RESIDENTIAL_MOVED_IN_DATE,
    TIME_STAMP_OF_ACTION AS o_TIME_STAMP_OF_ACTION,
    o_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER AS CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER,
    o_OFFERED_LIMIT_AMOUNT AS OFFERED_LIMIT_AMOUNT,
    o_in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE AS CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE,
    ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
    CARD_ORIGIN AS CARD_ORIGIN,
    count1 AS count1,
    lookup_string AS lookup_string,
    businessdate AS businessdate,
    business_date_string AS business_date_string,
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    APPLICATION_STATUS AS APPLICATION_STATUS,
    RESIDENTIAL_MOVED_IN_DATE AS RESIDENTIAL_MOVED_IN_DATE,
    in_PREVIOUS_ADDRESSES_MOVED_IN_DATE AS in_PREVIOUS_ADDRESSES_MOVED_IN_DATE,
    EMPLOYMENT_MAIN_STARTDATE AS EMPLOYMENT_MAIN_STARTDATE,
    ROW_NUM AS ROW_NUM,
    FEED_UPDATE_ID AS FEED_UPDATE_ID,
    APPLICATION_STATUS1 AS APPLICATION_STATUS1,
    APPLICATION_STATUS2 AS APPLICATION_STATUS2,
    FINAL_DECISION_COARSE_TGT AS FINAL_DECISION_COARSE_TGT,
    FINAL_DECISION_PRIMARY_TGT AS FINAL_DECISION_PRIMARY_TGT,
    ACTION_DATE_TGT AS ACTION_DATE_TGT
  
  FROM EXP_WORK_CARDS_ORIG_STAGING AS in0

)

SELECT *

FROM EXP_WORK_CARDS_ORIG_STAGING_EXPR_321
