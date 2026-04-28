from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_account_history_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_get_dates_and_feed_number_sp = Process(
        name = "EXP_GET_DATES_AND_FEED_NUMBER_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "ACCID_ACCOUNT_ALIAS", "expression" : {"expression" : "ACCID_ACCOUNT_ALIAS"}},
           {"alias" : "ACCID_TGT", "expression" : {"expression" : "ACCID_TGT"}},
           {"alias" : "STATUS_DATE_SRC", "expression" : {"expression" : "STATUS_DATE_SRC"}},
           {"alias" : "STATUS_SRC", "expression" : {"expression" : "STATUS_SRC"}},
           {"alias" : "DOMICILE_BRANCH_TGT", "expression" : {"expression" : "DOMICILE_BRANCH_TGT"}},
           {"alias" : "STATUS_TGT", "expression" : {"expression" : "STATUS_TGT"}},
           {"alias" : "STATUS_DATE_TGT", "expression" : {"expression" : "STATUS_DATE_TGT"}},
           {"alias" : "total_compare", "expression" : {"expression" : "total_compare"}},
           {"alias" : "APPLICATION_STORE_NUMBER_SRC", "expression" : {"expression" : "APPLICATION_STORE_NUMBER_SRC"}},
           {"alias" : "EFROM_TGT", "expression" : {"expression" : "EFROM_TGT"}},
           {"alias" : "lookup_string", "expression" : {"expression" : "lookup_string"}},
           {
             "alias": "business_date_string__find_business_date_lkp_1",
             "expression": {"expression" : "business_date_string__find_business_date_lkp_1"}
           },
           {"alias" : "LOOKUP_VARIABLE_3", "expression" : {"expression" : "LOOKUP_VARIABLE_3"}}]
        ),
        is_custom_output_schema = True
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
    m_rep_cards_orig_account_history_update__close_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__CLOSE_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__CLOSE_ACCOUNT_HISTORY_CHANGE")
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1 = Process(
        name = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1")
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_vars_0 = Process(
        name = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_VARS_0")
    )
    m_rep_cards_orig_account_history_update__ins_account_history = Process(
        name = "m_rep_cards_orig_account_history_update__INS_ACCOUNT_HISTORY",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__INS_ACCOUNT_HISTORY")
    )
    m_rep_cards_orig_account_history_update__new_maintains = Process(
        name = "m_rep_cards_orig_account_history_update__NEW_MAINTAINS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__NEW_MAINTAINS")
    )
    m_rep_cards_orig_account_history_update__open_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__OPEN_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__OPEN_ACCOUNT_HISTORY_CHANGE")
    )
    m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change = Process(
        name = "m_rep_cards_orig_account_history_update__RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_account_history_update__RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE"
        )
    )
    m_rep_cards_orig_account_history_update__upd_account_history_same_date_change = Process(
        name = "m_rep_cards_orig_account_history_update__UPD_ACCOUNT_HISTORY_SAME_DATE_CHANGE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_history_update__UPD_ACCOUNT_HISTORY_SAME_DATE_CHANGE")
    )
    (
        m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change._out(0)
        >> [m_rep_cards_orig_account_history_update__close_account_history_change._in(0),
              m_rep_cards_orig_account_history_update__open_account_history_change._in(0)]
    )
    exp_get_dates_and_feed_number_sp >> m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1
    (
        m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_expr_1._out(0)
        >> [m_rep_cards_orig_account_history_update__rtr_account_history_change_expr_account_history_change._in(0),
              m_rep_cards_orig_account_history_update__upd_account_history_same_date_change._in(0),
              m_rep_cards_orig_account_history_update__ins_account_history._in(0),
              m_rep_cards_orig_account_history_update__new_maintains._in(0)]
    )
    m_rep_cards_orig_account_history_update__exp_get_dates_and_feed_number_vars_0 >> exp_get_dates_and_feed_number_sp
