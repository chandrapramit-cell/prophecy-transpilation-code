from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_client_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_client_alias_sp = Process(
        name = "EXP_CLIENT_ALIAS_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'U'",
            "arg2": "360",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "CLID", "expression" : {"expression" : "CLID"}},
           {"alias" : "CUSTOMER_NUMBER", "expression" : {"expression" : "CUSTOMER_NUMBER"}},
           {"alias" : "GENERATED_CLID", "expression" : {"expression" : "GENERATED_CLID"}},
           {"alias" : "ADID", "expression" : {"expression" : "ADID"}},
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
    m_rep_cards_orig_client_alias_update__chooses = Process(
        name = "m_rep_cards_orig_client_alias_update__CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CHOOSES")
    )
    m_rep_cards_orig_client_alias_update__client_alias1 = Process(
        name = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__CLIENT_ALIAS1")
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_vars_0 = Process(
        name = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__EXP_CLIENT_ALIAS_VARS_0")
    )
    m_rep_cards_orig_client_alias_update__resides = Process(
        name = "m_rep_cards_orig_client_alias_update__RESIDES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RESIDES")
    )
    m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias = Process(
        name = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS")
    )
    exp_client_alias_sp >> m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias
    (
        m_rep_cards_orig_client_alias_update__rtr_client_alias_expr_new_client_alias._out(0)
        >> [m_rep_cards_orig_client_alias_update__chooses._in(0),
              m_rep_cards_orig_client_alias_update__resides._in(0),
              m_rep_cards_orig_client_alias_update__client_alias1._in(0)]
    )
    m_rep_cards_orig_client_alias_update__exp_client_alias_vars_0 >> exp_client_alias_sp
