from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_credit_application_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(
          table = Dataset.DBTSource(name = "sp2", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    exp_work_cards_orig_staging = Process(
        name = "EXP_WORK_CARDS_ORIG_STAGING",
        properties = CallStoredProc(
          storedProcedureIdentifier = "prophecy-databricks-qa.Avpreet.sp1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END)",
            "arg4": "SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN business_date_string__find_business_date_lkp_1 ELSE NULL END) , 0 , 8)"
          },
          passThroughColumns = [{"alias" : "ACCID_SRC", "expression" : {"expression" : "ACCID_SRC"}},
           {"alias" : "APPLICATION_DATE", "expression" : {"expression" : "APPLICATION_DATE"}},
           {"alias" : "APPLICATION_STORE_NUMBER", "expression" : {"expression" : "APPLICATION_STORE_NUMBER"}},
           {
             "alias": "o_CDD_RESULT",
             "expression": {
               "expression": "(CASE WHEN (CDD_RESULT = 'PASSED') THEN 'PASS' WHEN (CDD_RESULT = 'REFER') THEN 'REFE' WHEN (CDD_RESULT = 'FINALCOMPLETE') THEN 'FINA' WHEN (CDD_RESULT = 'DECLINED') THEN 'DECL' WHEN (CDD_RESULT = 'TIMEOUT') THEN 'TIMO' WHEN (CDD_RESULT = 'PEPAPPROVED') THEN 'PEPA' WHEN (CDD_RESULT = 'OPSTIMEOUT') THEN 'OPST' WHEN (CDD_RESULT = 'INPROGRESS') THEN 'INPR' WHEN (CDD_RESULT = 'INERROR') THEN 'INER' WHEN (CDD_RESULT = 'NOT_CHECKED') THEN 'NOTC' WHEN (CDD_RESULT = 'NOT_APPLICABLE') THEN 'NOTA' ELSE SUBSTRING(CDD_RESULT , 0 , 4) END)"
             }
           },
           {
             "alias": "o_RTS_TRANSACTION_STATUS",
             "expression": {
               "expression": "(CASE WHEN (RTS_TRANSACTION_STATUS = 'PASSED') THEN 'PASS' WHEN (RTS_TRANSACTION_STATUS = 'REFER') THEN 'REFE' WHEN (RTS_TRANSACTION_STATUS = 'FINALCOMPLETE') THEN 'FINA' WHEN (RTS_TRANSACTION_STATUS = 'DECLINED') THEN 'DECL' WHEN (RTS_TRANSACTION_STATUS = 'TIMEOUT') THEN 'TIMO' WHEN (RTS_TRANSACTION_STATUS = 'PEPAPPROVED') THEN 'PEPA' WHEN (RTS_TRANSACTION_STATUS = 'OPSTIMEOUT') THEN 'OPST' WHEN (RTS_TRANSACTION_STATUS = 'INPROGRESS') THEN 'INPR' WHEN (RTS_TRANSACTION_STATUS = 'INERROR') THEN 'INER' WHEN (RTS_TRANSACTION_STATUS = 'NOT_CHECKED') THEN 'NOTC' WHEN (RTS_TRANSACTION_STATUS = 'NOT_APPLICABLE') THEN 'NOTA' ELSE SUBSTRING(RTS_TRANSACTION_STATUS , 0 , 4) END)"
             }
           },
           {
             "alias": "o_in_IDENTITY_VERIFICATION_STATUS",
             "expression": {
               "expression": "(CASE WHEN (IDENTITY_VERIFICATION_STATUS = 'PASSED') THEN 'PASS' WHEN (IDENTITY_VERIFICATION_STATUS = 'REFER') THEN 'REFE' WHEN (IDENTITY_VERIFICATION_STATUS = 'FAILED') THEN 'FAIL' WHEN (IDENTITY_VERIFICATION_STATUS = 'DECLINED') THEN 'DECL' WHEN (IDENTITY_VERIFICATION_STATUS = 'IN_PROGRESS') THEN 'INPR' WHEN (IDENTITY_VERIFICATION_STATUS = 'ERROR') THEN 'ERRO' WHEN (IDENTITY_VERIFICATION_STATUS = 'NOT_CHECKED') THEN 'NOTC' ELSE SUBSTRING(IDENTITY_VERIFICATION_STATUS , 0 , 4) END)"
             }
           },
           {
             "alias": "o_FRAUD_RESULT",
             "expression": {
               "expression": "(CASE WHEN (FRAUD_RESULT = 'PASSED') THEN 'PASS' WHEN (FRAUD_RESULT = 'REFER') THEN 'REFE' WHEN (FRAUD_RESULT = 'DECLINE') THEN 'DECL' ELSE SUBSTRING(FRAUD_RESULT , 0 , 4) END)"
             }
           },
           {
             "alias": "o_in_CREDIT_DECISION_RESULT",
             "expression": {
               "expression": "(CASE WHEN (CREDIT_DECISION_RESULT = 'ACCEPT') THEN 'ACCE' WHEN (CREDIT_DECISION_RESULT = 'REFER') THEN 'REFE' WHEN (CREDIT_DECISION_RESULT = 'REJECT') THEN 'REJE' WHEN (CREDIT_DECISION_RESULT = 'NOT_CHECKED') THEN 'NOTC' ELSE SUBSTRING(CREDIT_DECISION_RESULT , 0 , 4) END)"
             }
           },
           {"alias" : "ACCOUNT_NUMBER", "expression" : {"expression" : "ACCOUNT_NUMBER"}},
           {"alias" : "APPLICATION_NUMBER", "expression" : {"expression" : "APPLICATION_NUMBER"}},
           {"alias" : "APPLICATION_STATUS", "expression" : {"expression" : "APPLICATION_STATUS"}},
           {"alias" : "o_PRODUCT", "expression" : {"expression" : "'MS'"}},
           {"alias" : "APPLICATION_SOURCE_CODE", "expression" : {"expression" : "APPLICATION_SOURCE_CODE"}},
           {"alias" : "DATE_OF_BIRTH", "expression" : {"expression" : "DATE_OF_BIRTH"}},
           {"alias" : "POSTCODE", "expression" : {"expression" : "POSTCODE"}},
           {
             "alias": "ANNUAL_INCOME_AMOUNT",
             "expression": {
               "expression": "(CASE WHEN CAST((ANNUAL_INCOME_AMOUNT1 IS NULL) AS BOOL) THEN 0 ELSE ANNUAL_INCOME_AMOUNT1 END)"
             }
           },
           {"alias" : "APPLICATION_TYPE", "expression" : {"expression" : "'T'"}},
           {"alias" : "CUSTOMER_NUMBER", "expression" : {"expression" : "CUSTOMER_NUMBER"}},
           {
             "alias": "o_EMPLOYMENT_ROLE",
             "expression": {
               "expression": "(CASE WHEN (EMPLOYMENT_ROLE = 'BUSINESS_OWNER') THEN 'B' WHEN (EMPLOYMENT_ROLE = 'KEY_CONTROLLER') THEN 'K' WHEN (EMPLOYMENT_ROLE = 'EMPLOYEE') THEN 'E' WHEN (EMPLOYMENT_ROLE = 'SOLE_TRADER') THEN 'S' ELSE SUBSTRING(EMPLOYMENT_ROLE , 0 , 1) END)"
             }
           },
           {
             "alias": "o_EMPLOYMENT_STATUS",
             "expression": {
               "expression": "(CASE WHEN (EMPLOYMENT_STATUS = 'AT_HOME') THEN 'AH' WHEN (EMPLOYMENT_STATUS = 'EMPLOYED_FULL_TIME') THEN 'EF' WHEN (EMPLOYMENT_STATUS = 'EMPLOYED_PART_TIME') THEN 'EP' WHEN (EMPLOYMENT_STATUS = 'SELF_EMPLOYED') THEN 'SE' WHEN (EMPLOYMENT_STATUS = 'RETIRED') THEN 'RT' WHEN (EMPLOYMENT_STATUS = 'DISABLED') THEN 'DI' WHEN (EMPLOYMENT_STATUS = 'STUDENT') THEN 'ST' WHEN (EMPLOYMENT_STATUS = 'UNEMPLOYED') THEN 'UN' ELSE SUBSTRING(EMPLOYMENT_STATUS , 0 , 2) END)"
             }
           },
           {
             "alias": "o_MARITAL_STATUS",
             "expression": {
               "expression": "(CASE WHEN (MARITAL_STATUS = 'MARRIED') THEN 'M' WHEN (MARITAL_STATUS = 'SINGLE') THEN 'S' WHEN (MARITAL_STATUS = 'DIVORCED') THEN 'D' WHEN (MARITAL_STATUS = 'LIVING_WITH_PARTNER') THEN 'P' WHEN (MARITAL_STATUS = 'WIDOWED') THEN 'W' ELSE SUBSTRING(MARITAL_STATUS , 0 , 1) END)"
             }
           },
           {
             "alias": "NUMBER_OF_CHILDREN",
             "expression": {
               "expression": "(CASE WHEN CAST((NUMBER_OF_DEPENDENTS IS NULL) AS BOOL) THEN 0 ELSE NUMBER_OF_DEPENDENTS END)"
             }
           },
           {
             "alias": "o_RESIDENTIAL_STATUS",
             "expression": {
               "expression": "(CASE WHEN (RESIDENTIAL_STATUS = 'OTHER') THEN 'O' WHEN (RESIDENTIAL_STATUS = 'OWNER') THEN 'W' WHEN (RESIDENTIAL_STATUS = 'TENANT') THEN 'T' WHEN (RESIDENTIAL_STATUS = 'WITH_PARENTS') THEN 'P' ELSE SUBSTRING(RESIDENTIAL_STATUS , 0 , 1) END)"
             }
           },
           {"alias" : "OCCUPATION", "expression" : {"expression" : "CAST(NULL AS STRING)"}},
           {"alias" : "PRODUCT_NUMBER", "expression" : {"expression" : "PRODUCT_NUMBER"}},
           {"alias" : "EMAIL", "expression" : {"expression" : "EMAIL"}},
           {
             "alias": "o_CHANNEL_PREFERENCE_INDICATOR",
             "expression": {
               "expression": "(CASE WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'EMAIL') THEN 'E' WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'SMS') THEN 'S' WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'CALL') THEN 'C' WHEN (MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'POST') THEN 'P' ELSE SUBSTRING(MARKETING_PREFERENCES_PREFERRED_CHANNELS , 0 , 1) END)"
             }
           },
           {"alias" : "APPLICATION_SCORE", "expression" : {"expression" : "APPLICATION_SCORE"}},
           {"alias" : "CIISCORE", "expression" : {"expression" : "CIISCORE"}},
           {"alias" : "DELPHI_SCORE", "expression" : {"expression" : "DELPHI_SCORE"}},
           {"alias" : "FIRST_PARTY_FRAUD_SCORE", "expression" : {"expression" : "FIRST_PARTY_FRAUD_SCORE"}},
           {
             "alias": "o_in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE",
             "expression": {
               "expression": "(CASE WHEN CAST((IN_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE IS NULL) AS BOOL) THEN 0 ELSE IN_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE END)"
             }
           },
           {"alias" : "RESIDENTIAL_MOVED_IN_DATE", "expression" : {"expression" : "RESIDENTIAL_MOVED_IN_DATE"}},
           {
             "alias": "o_RESIDENTIAL_MOVED_IN_DATE",
             "expression": {
               "expression": "(CASE WHEN CAST((RESIDENTIAL_MOVED_IN_DATE IS NULL) AS BOOL) THEN '' WHEN ((LENGTH(CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')) AS STRING))), DAY) AS STRING))) > 4) THEN '9999' ELSE CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')) AS STRING))), DAY) AS STRING) END)"
             }
           },
           {
             "alias": "in_PREVIOUS_ADDRESSES_MOVED_IN_DATE",
             "expression": {"expression" : "in_PREVIOUS_ADDRESSES_MOVED_IN_DATE"}
           },
           {
             "alias": "o_TIME_AT_PREVIOUS_ADDRESS",
             "expression": {
               "expression": "(CASE WHEN CAST((IN_PREVIOUS_ADDRESSES_MOVED_IN_DATE IS NULL) AS BOOL) THEN '' WHEN ((LENGTH(CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(IN_PREVIOUS_ADDRESSES_MOVED_IN_DATE, '01')) AS STRING))), DAY) AS STRING))) > 4) THEN '9999' ELSE CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(RESIDENTIAL_MOVED_IN_DATE, '01')) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(IN_PREVIOUS_ADDRESSES_MOVED_IN_DATE, '01')) AS STRING))), DAY) AS STRING) END)"
             }
           },
           {"alias" : "EMPLOYMENT_MAIN_STARTDATE", "expression" : {"expression" : "EMPLOYMENT_MAIN_STARTDATE"}},
           {
             "alias": "o_EMPLOYMENT_MAIN_STARTDATE",
             "expression": {
               "expression": "(CASE WHEN CAST((EMPLOYMENT_MAIN_STARTDATE IS NULL) AS BOOL) THEN '' WHEN ((LENGTH(CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(EMPLOYMENT_MAIN_STARTDATE, '01')) AS STRING))), DAY) AS STRING))) > 4) THEN '9999' ELSE CAST(DATE_DIFF((PARSE_TIMESTAMP('%Y%m%d', CAST((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) AS STRING))), (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(EMPLOYMENT_MAIN_STARTDATE, '01')) AS STRING))), DAY) AS STRING) END)"
             }
           },
           {
             "alias": "o_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER",
             "expression": {
               "expression": "(CASE WHEN CAST((IN_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER IS NULL) AS BOOL) THEN 0 ELSE IN_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER END)"
             }
           },
           {"alias" : "OCCUPATION1", "expression" : {"expression" : "OCCUPATION1"}},
           {
             "alias": "o_OFFERED_LIMIT_AMOUNT",
             "expression": {
               "expression": "(CASE WHEN CAST((IN_OFFERED_LIMIT_AMOUNT IS NULL) AS BOOL) THEN 0 ELSE IN_OFFERED_LIMIT_AMOUNT END)"
             }
           },
           {
             "alias": "out_ANNUAL_SALARY_AMOUNT",
             "expression": {
               "expression": "(CASE WHEN CAST((ANNUAL_SALARY_AMOUNT IS NULL) AS BOOL) THEN 0 ELSE ANNUAL_SALARY_AMOUNT END)"
             }
           },
           {"alias" : "OFFER_ID", "expression" : {"expression" : "OFFER_ID"}},
           {"alias" : "CREDIT_REQUESTED_AMOUNT", "expression" : {"expression" : "CREDIT_REQUESTED_AMOUNT"}},
           {
             "alias": "FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES",
             "expression": {"expression" : "FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES"}
           },
           {"alias" : "ROW_NUM", "expression" : {"expression" : "ROW_NUM"}},
           {
             "alias": "BUSINESS_DATE",
             "expression": {
               "expression": "(CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattribute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END)"
             }
           },
           {
             "alias": "BUSINESS_DATE_MINUS_1",
             "expression": {
               "expression": "(DATE_ADD((CASE WHEN CAST(((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) IS NULL) AS BOOL) THEN (ERROR('No Business Date found on dbattribute')) ELSE (PARSE_TIMESTAMP('%Y%m%d', CAST(SUBSTRING((CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1) THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1 ELSE CAST(NULL AS STRING) END) , 0 , 8) AS STRING))) END)  ,INTERVAL -1  DAY))"
             }
           },
           {
             "alias": "FEED_UPDATE_ID",
             "expression": {
               "expression": "(CASE WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1) THEN output_sp ELSE NULL END)"
             }
           },
           {"alias" : "ACCID_TGT", "expression" : {"expression" : "ACCID_TGT"}},
           {"alias" : "APPLICATION_STATUS1", "expression" : {"expression" : "APPLICATION_STATUS1"}},
           {
             "alias": "o_INITIAL_DECISION_COARSE_SRC",
             "expression": {
               "expression": "(CASE WHEN (APPLICATION_STATUS1 = 'CANCELLED') THEN 'C' WHEN (APPLICATION_STATUS1 = 'APPROVED') THEN 'A' WHEN (APPLICATION_STATUS1 = 'ABORTED') THEN 'X' WHEN (APPLICATION_STATUS1 = 'REFERRED') THEN 'R' WHEN (APPLICATION_STATUS1 = 'DECLINED') THEN 'D' ELSE NULL END)"
             }
           },
           {
             "alias": "o_INITIAL_DECISION_PRIMARY_SRC",
             "expression": {
               "expression": "(CASE WHEN (APPLICATION_STATUS1 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS1 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS1 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS1 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS1 = 'DECLINED') THEN 'DECL' ELSE NULL END)"
             }
           },
           {"alias" : "APPLICATION_STATUS2", "expression" : {"expression" : "APPLICATION_STATUS2"}},
           {"alias" : "INITIAL_DECISION_COARSE_TGT", "expression" : {"expression" : "INITIAL_DECISION_COARSE_TGT"}},
           {"alias" : "INITIAL_DECISION_PRIMARY_TGT", "expression" : {"expression" : "INITIAL_DECISION_PRIMARY_TGT"}},
           {"alias" : "FINAL_DECISION_COARSE_TGT", "expression" : {"expression" : "FINAL_DECISION_COARSE_TGT"}},
           {"alias" : "FINAL_DECISION_PRIMARY_TGT", "expression" : {"expression" : "FINAL_DECISION_PRIMARY_TGT"}},
           {
             "alias": "o_FINAL_DECISION_COARSE",
             "expression": {
               "expression": "(CASE WHEN (((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'C' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'A' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'X' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'R' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'D' ELSE NULL END) IS NULL) AND (FINAL_DECISION_COARSE_TGT IS NOT NULL)) THEN FINAL_DECISION_COARSE_TGT WHEN (((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'C' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'A' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'X' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'R' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'D' ELSE NULL END) IS NULL) AND (FINAL_DECISION_COARSE_TGT IS NULL)) THEN CAST((CASE WHEN (APPLICATION_STATUS1 = 'CANCELLED') THEN 'C' WHEN (APPLICATION_STATUS1 = 'APPROVED') THEN 'A' WHEN (APPLICATION_STATUS1 = 'ABORTED') THEN 'X' WHEN (APPLICATION_STATUS1 = 'REFERRED') THEN 'R' WHEN (APPLICATION_STATUS1 = 'DECLINED') THEN 'D' ELSE NULL END) AS STRING) WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'C' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'A' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'X' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'R' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'D' ELSE NULL END)"
             }
           },
           {
             "alias": "o_FINAL_DECISION_PRIMARY",
             "expression": {
               "expression": "(CASE WHEN (((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'DECL' ELSE NULL END) IS NULL) AND (FINAL_DECISION_PRIMARY_TGT IS NOT NULL)) THEN FINAL_DECISION_PRIMARY_TGT WHEN (((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'DECL' ELSE NULL END) IS NULL) AND (FINAL_DECISION_PRIMARY_TGT IS NULL)) THEN CAST((CASE WHEN (APPLICATION_STATUS1 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS1 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS1 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS1 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS1 = 'DECLINED') THEN 'DECL' ELSE NULL END) AS STRING) WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'DECL' ELSE NULL END)"
             }
           },
           {"alias" : "LAST_ACTION_DATE", "expression" : {"expression" : "LAST_ACTION_DATE"}},
           {"alias" : "TIME_STAMP_OF_ACTION", "expression" : {"expression" : "TIME_STAMP_OF_ACTION"}},
           {
             "alias": "o_TIME_STAMP_OF_ACTION",
             "expression": {
               "expression": "(CONCAT(SUBSTRING(LAST_ACTION_DATE , 10 , 2), SUBSTRING(LAST_ACTION_DATE , 13 , 2), SUBSTRING(LAST_ACTION_DATE , 16 , 2)))"
             }
           },
           {"alias" : "ACTION_DATE_TGT", "expression" : {"expression" : "ACTION_DATE_TGT"}},
           {
             "alias": "o_LAST_ACTION_DATE",
             "expression": {
               "expression": "(CASE WHEN CAST(((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'DECL' ELSE NULL END) IS NULL) AS BOOL) THEN ACTION_DATE_TGT WHEN ((CASE WHEN (APPLICATION_STATUS2 = 'CANCELLED') THEN 'CANC' WHEN (APPLICATION_STATUS2 = 'APPROVED') THEN 'APPR' WHEN (APPLICATION_STATUS2 = 'ABORTED') THEN 'ABOR' WHEN (APPLICATION_STATUS2 = 'REFERRED') THEN 'REFE' WHEN (APPLICATION_STATUS2 = 'DECLINED') THEN 'DECL' ELSE NULL END) = FINAL_DECISION_PRIMARY_TGT) THEN ACTION_DATE_TGT ELSE LAST_ACTION_DATE END)"
             }
           },
           {"alias" : "EFROM_TGT", "expression" : {"expression" : "EFROM_TGT"}},
           {"alias" : "CARD_ORIGIN", "expression" : {"expression" : "2"}}]
        )
    )
    feed_number_generator1 = Process(
        name = "feed_number_generator1",
        properties = CallStoredProc(
          storedProcedureIdentifier = "generate_feed_id",
          parameters = {"IN_SOURCE_SYSTEM_CODE" : "dummy"},
          passThroughColumns = []
        ),
        input_ports = None
    )
    m_rep_cards_orig_credit_application_update__close_credit_application = Process(
        name = "m_rep_cards_orig_credit_application_update__CLOSE_CREDIT_APPLICATION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__CLOSE_CREDIT_APPLICATION")
    )
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_18 = Process(
        name = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_18",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_18")
    )
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_vars_0 = Process(
        name = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_VARS_0")
    )
    m_rep_cards_orig_credit_application_update__ins_new_credit_application = Process(
        name = "m_rep_cards_orig_credit_application_update__INS_NEW_CREDIT_APPLICATION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__INS_NEW_CREDIT_APPLICATION")
    )
    m_rep_cards_orig_credit_application_update__open_credit_application = Process(
        name = "m_rep_cards_orig_credit_application_update__OPEN_CREDIT_APPLICATION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__OPEN_CREDIT_APPLICATION")
    )
    m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group = Process(
        name = "m_rep_cards_orig_credit_application_update__RTR_CREDIT_APPLICATION_EXPR_OTHER_DAY_GROUP",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_credit_application_update__RTR_CREDIT_APPLICATION_EXPR_OTHER_DAY_GROUP"
        )
    )
    m_rep_cards_orig_credit_application_update__upd_credit_application = Process(
        name = "m_rep_cards_orig_credit_application_update__UPD_CREDIT_APPLICATION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__UPD_CREDIT_APPLICATION")
    )
    (
        m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group._out(0)
        >> [m_rep_cards_orig_credit_application_update__close_credit_application._in(0),
              m_rep_cards_orig_credit_application_update__open_credit_application._in(0)]
    )
    (
        m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_18._out(0)
        >> [m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group._in(0),
              m_rep_cards_orig_credit_application_update__upd_credit_application._in(0),
              m_rep_cards_orig_credit_application_update__ins_new_credit_application._in(0)]
    )
    dbattribute >> m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_vars_0
    exp_work_cards_orig_staging >> m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_18
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_vars_0 >> exp_work_cards_orig_staging
