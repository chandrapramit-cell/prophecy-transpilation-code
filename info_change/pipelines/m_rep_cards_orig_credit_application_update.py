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
    exp_work_cards_orig_staging_sp = Process(
        name = "EXP_WORK_CARDS_ORIG_STAGING_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "sp123",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "PRODUCT_NUMBER", "expression" : {"expression" : "PRODUCT_NUMBER"}},
           {"alias" : "NUMBER_OF_DEPENDENTS", "expression" : {"expression" : "NUMBER_OF_DEPENDENTS"}},
           {"alias" : "in_OFFERED_LIMIT_AMOUNT", "expression" : {"expression" : "in_OFFERED_LIMIT_AMOUNT"}},
           {"alias" : "CREDIT_REQUESTED_AMOUNT", "expression" : {"expression" : "CREDIT_REQUESTED_AMOUNT"}},
           {"alias" : "EMAIL", "expression" : {"expression" : "EMAIL"}},
           {
             "alias": "MARKETING_PREFERENCES_PREFERRED_CHANNELS",
             "expression": {"expression" : "MARKETING_PREFERENCES_PREFERRED_CHANNELS"}
           },
           {"alias" : "POSTCODE", "expression" : {"expression" : "POSTCODE"}},
           {"alias" : "DATE_OF_BIRTH", "expression" : {"expression" : "DATE_OF_BIRTH"}},
           {
             "alias": "FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES",
             "expression": {"expression" : "FORESEE_CHANGES_TO_PERSONAL_CIRCUMSTANCES"}
           },
           {"alias" : "APPLICATION_NUMBER", "expression" : {"expression" : "APPLICATION_NUMBER"}},
           {"alias" : "APPLICATION_STATUS", "expression" : {"expression" : "APPLICATION_STATUS"}},
           {"alias" : "RESIDENTIAL_MOVED_IN_DATE", "expression" : {"expression" : "RESIDENTIAL_MOVED_IN_DATE"}},
           {
             "alias": "in_PREVIOUS_ADDRESSES_MOVED_IN_DATE",
             "expression": {"expression" : "in_PREVIOUS_ADDRESSES_MOVED_IN_DATE"}
           },
           {"alias" : "EMPLOYMENT_MAIN_STARTDATE", "expression" : {"expression" : "EMPLOYMENT_MAIN_STARTDATE"}},
           {"alias" : "ROW_NUM", "expression" : {"expression" : "ROW_NUM"}},
           {"alias" : "ANNUAL_INCOME_AMOUNT1", "expression" : {"expression" : "ANNUAL_INCOME_AMOUNT1"}},
           {"alias" : "TIME_STAMP_OF_ACTION", "expression" : {"expression" : "TIME_STAMP_OF_ACTION"}},
           {"alias" : "ACCID_TGT", "expression" : {"expression" : "ACCID_TGT"}},
           {"alias" : "APPLICATION_STATUS1", "expression" : {"expression" : "APPLICATION_STATUS1"}},
           {"alias" : "INITIAL_DECISION_COARSE_TGT", "expression" : {"expression" : "INITIAL_DECISION_COARSE_TGT"}},
           {"alias" : "INITIAL_DECISION_PRIMARY_TGT", "expression" : {"expression" : "INITIAL_DECISION_PRIMARY_TGT"}},
           {"alias" : "FINAL_DECISION_COARSE_TGT", "expression" : {"expression" : "FINAL_DECISION_COARSE_TGT"}},
           {"alias" : "FINAL_DECISION_PRIMARY_TGT", "expression" : {"expression" : "FINAL_DECISION_PRIMARY_TGT"}},
           {"alias" : "APPLICATION_STATUS2", "expression" : {"expression" : "APPLICATION_STATUS2"}},
           {"alias" : "LAST_ACTION_DATE", "expression" : {"expression" : "LAST_ACTION_DATE"}},
           {"alias" : "EFROM_TGT", "expression" : {"expression" : "EFROM_TGT"}},
           {"alias" : "ACTION_DATE_TGT", "expression" : {"expression" : "ACTION_DATE_TGT"}},
           {"alias" : "ACCID_SRC", "expression" : {"expression" : "ACCID_SRC"}},
           {"alias" : "APPLICATION_DATE", "expression" : {"expression" : "APPLICATION_DATE"}},
           {"alias" : "EMPLOYMENT_STATUS", "expression" : {"expression" : "EMPLOYMENT_STATUS"}},
           {"alias" : "CDD_RESULT", "expression" : {"expression" : "CDD_RESULT"}},
           {"alias" : "RTS_TRANSACTION_STATUS", "expression" : {"expression" : "RTS_TRANSACTION_STATUS"}},
           {"alias" : "IDENTITY_VERIFICATION_STATUS", "expression" : {"expression" : "IDENTITY_VERIFICATION_STATUS"}},
           {"alias" : "FRAUD_RESULT", "expression" : {"expression" : "FRAUD_RESULT"}},
           {"alias" : "CREDIT_DECISION_RESULT", "expression" : {"expression" : "CREDIT_DECISION_RESULT"}},
           {"alias" : "MARITAL_STATUS", "expression" : {"expression" : "MARITAL_STATUS"}},
           {"alias" : "APPLICATION_STORE_NUMBER", "expression" : {"expression" : "APPLICATION_STORE_NUMBER"}},
           {"alias" : "ACCOUNT_NUMBER", "expression" : {"expression" : "ACCOUNT_NUMBER"}},
           {"alias" : "PRODUCT", "expression" : {"expression" : "PRODUCT"}},
           {"alias" : "APPLICATION_SOURCE_CODE", "expression" : {"expression" : "APPLICATION_SOURCE_CODE"}},
           {"alias" : "APPLICATION_SCORE", "expression" : {"expression" : "APPLICATION_SCORE"}},
           {"alias" : "CIISCORE", "expression" : {"expression" : "CIISCORE"}},
           {"alias" : "DELPHI_SCORE", "expression" : {"expression" : "DELPHI_SCORE"}},
           {"alias" : "FIRST_PARTY_FRAUD_SCORE", "expression" : {"expression" : "FIRST_PARTY_FRAUD_SCORE"}},
           {
             "alias": "in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE",
             "expression": {"expression" : "in_CREDIT_DECISION_ANNUAL_PERCENTAGE_RATE"}
           },
           {"alias" : "RESIDENTIAL_STATUS", "expression" : {"expression" : "RESIDENTIAL_STATUS"}},
           {"alias" : "OCCUPATION1", "expression" : {"expression" : "OCCUPATION1"}},
           {
             "alias": "in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER",
             "expression": {"expression" : "in_CREDIT_DECISION_FINAL_CREDIT_LIMIT_OFFER"}
           },
           {"alias" : "ANNUAL_SALARY_AMOUNT", "expression" : {"expression" : "ANNUAL_SALARY_AMOUNT"}},
           {"alias" : "OFFER_ID", "expression" : {"expression" : "OFFER_ID"}},
           {"alias" : "CUSTOMER_NUMBER", "expression" : {"expression" : "CUSTOMER_NUMBER"}},
           {"alias" : "EMPLOYMENT_ROLE", "expression" : {"expression" : "EMPLOYMENT_ROLE"}},
           {"alias" : "lookup_string", "expression" : {"expression" : "lookup_string"}},
           {
             "alias": "business_date_string__find_business_date_lkp_1",
             "expression": {"expression" : "business_date_string__find_business_date_lkp_1"}
           },
           {"alias" : "LOOKUP_VARIABLE_3", "expression" : {"expression" : "LOOKUP_VARIABLE_3"}},
           {"alias" : "OCCUPATION", "expression" : {"expression" : "OCCUPATION"}}]
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
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_1 = Process(
        name = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_1")
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
    dbattribute >> m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_vars_0
    (
        m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_1._out(0)
        >> [m_rep_cards_orig_credit_application_update__upd_credit_application._in(0),
              m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group._in(0),
              m_rep_cards_orig_credit_application_update__ins_new_credit_application._in(0)]
    )
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_vars_0 >> exp_work_cards_orig_staging_sp
    exp_work_cards_orig_staging_sp >> m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_1
