from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_ohc_account_transfer_history_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_conv_sp = Process(
        name = "EXP_CONV_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'G'",
            "arg2": "'560'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "OLD_ACCOUNT_NUMBER_IN", "expression" : {"expression" : "OLD_ACCOUNT_NUMBER_IN"}},
           {"alias" : "NEW_ACCOUNT_NUMBER_IN", "expression" : {"expression" : "NEW_ACCOUNT_NUMBER_IN"}},
           {"alias" : "TRANSFER_EFFECTIVE_DATE_IN", "expression" : {"expression" : "TRANSFER_EFFECTIVE_DATE_IN"}},
           {"alias" : "prophecy_sk", "expression" : {"expression" : "prophecy_sk"}},
           {"alias" : "lookup_string", "expression" : {"expression" : "lookup_string"}},
           {
             "alias": "business_date_string__find_business_date_lkp_1",
             "expression": {"expression" : "business_date_string__find_business_date_lkp_1"}
           },
           {"alias" : "LOOKUP_VARIABLE_3", "expression" : {"expression" : "LOOKUP_VARIABLE_3"}}]
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
    m_rep_ohc_account_transfer_history_update__close_account_transfer_history_target_definition = Process(
        name = "m_rep_ohc_account_transfer_history_update__CLOSE_ACCOUNT_TRANSFER_HISTORY_Target_Definition",
        properties = ModelTransform(
          modelName = "m_rep_ohc_account_transfer_history_update__CLOSE_ACCOUNT_TRANSFER_HISTORY_Target_Definition"
        )
    )
    m_rep_ohc_account_transfer_history_update__exp_conv_vars_0 = Process(
        name = "m_rep_ohc_account_transfer_history_update__EXP_CONV_VARS_0",
        properties = ModelTransform(modelName = "m_rep_ohc_account_transfer_history_update__EXP_CONV_VARS_0")
    )
    m_rep_ohc_account_transfer_history_update__exp_derive_accid = Process(
        name = "m_rep_ohc_account_transfer_history_update__EXP_DERIVE_ACCID",
        properties = ModelTransform(modelName = "m_rep_ohc_account_transfer_history_update__EXP_DERIVE_ACCID")
    )
    m_rep_ohc_account_transfer_history_update__minerva_exception = Process(
        name = "m_rep_ohc_account_transfer_history_update__MINERVA_EXCEPTION",
        properties = ModelTransform(modelName = "m_rep_ohc_account_transfer_history_update__MINERVA_EXCEPTION")
    )
    m_rep_ohc_account_transfer_history_update__new_account_transfer_history_target_definition = Process(
        name = "m_rep_ohc_account_transfer_history_update__NEW_ACCOUNT_TRANSFER_HISTORY_Target_Definition",
        properties = ModelTransform(
          modelName = "m_rep_ohc_account_transfer_history_update__NEW_ACCOUNT_TRANSFER_HISTORY_Target_Definition"
        )
    )
    m_rep_ohc_account_transfer_history_update__open_account_transfer_history_target_definition = Process(
        name = "m_rep_ohc_account_transfer_history_update__OPEN_ACCOUNT_TRANSFER_HISTORY_Target_Definition",
        properties = ModelTransform(
          modelName = "m_rep_ohc_account_transfer_history_update__OPEN_ACCOUNT_TRANSFER_HISTORY_Target_Definition"
        )
    )
    m_rep_ohc_account_transfer_history_update__rtrtrans_expr_existing_account_transfer_history = Process(
        name = "m_rep_ohc_account_transfer_history_update__RTRTRANS_EXPR_EXISTING_ACCOUNT_TRANSFER_HISTORY",
        properties = ModelTransform(
          modelName = "m_rep_ohc_account_transfer_history_update__RTRTRANS_EXPR_EXISTING_ACCOUNT_TRANSFER_HISTORY"
        )
    )
    m_rep_ohc_account_transfer_history_update__rtrtrans_join_expr_107 = Process(
        name = "m_rep_ohc_account_transfer_history_update__RTRTRANS_JOIN_EXPR_107",
        properties = ModelTransform(modelName = "m_rep_ohc_account_transfer_history_update__RTRTRANS_JOIN_EXPR_107")
    )
    (
        m_rep_ohc_account_transfer_history_update__rtrtrans_join_expr_107._out(0)
        >> [m_rep_ohc_account_transfer_history_update__new_account_transfer_history_target_definition._in(0),
              m_rep_ohc_account_transfer_history_update__rtrtrans_expr_existing_account_transfer_history._in(0)]
    )
    (
        m_rep_ohc_account_transfer_history_update__rtrtrans_expr_existing_account_transfer_history._out(0)
        >> [m_rep_ohc_account_transfer_history_update__close_account_transfer_history_target_definition._in(0),
              m_rep_ohc_account_transfer_history_update__open_account_transfer_history_target_definition._in(0)]
    )
    exp_conv_sp >> m_rep_ohc_account_transfer_history_update__exp_derive_accid
    (
        m_rep_ohc_account_transfer_history_update__exp_derive_accid._out(0)
        >> [m_rep_ohc_account_transfer_history_update__rtrtrans_join_expr_107._in(0),
              m_rep_ohc_account_transfer_history_update__minerva_exception._in(0)]
    )
    m_rep_ohc_account_transfer_history_update__exp_conv_vars_0 >> exp_conv_sp
