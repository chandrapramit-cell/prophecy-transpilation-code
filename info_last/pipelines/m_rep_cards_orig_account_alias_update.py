from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_account_alias_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    dbattribute = Process(
        name = "DBATTRIBUTE",
        properties = Dataset(table = Dataset.DBTSource(name = "s1", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    exp_account_alias_sp = Process(
        name = "EXP_ACCOUNT_ALIAS_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "sp1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "APPLICATION_NUMBER", "expression" : {"expression" : "APPLICATION_NUMBER"}},
           {"alias" : "EXISTING_ACCID", "expression" : {"expression" : "EXISTING_ACCID"}},
           {"alias" : "NEW_ACCID", "expression" : {"expression" : "NEW_ACCID"}},
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
    m_rep_cards_orig_account_alias_update__exp_account_alias_vars_0 = Process(
        name = "m_rep_cards_orig_account_alias_update__EXP_ACCOUNT_ALIAS_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__EXP_ACCOUNT_ALIAS_VARS_0")
    )
    m_rep_cards_orig_account_alias_update__maintains = Process(
        name = "m_rep_cards_orig_account_alias_update__MAINTAINS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__MAINTAINS")
    )
    m_rep_cards_orig_account_alias_update__new_account_alias = Process(
        name = "m_rep_cards_orig_account_alias_update__NEW_ACCOUNT_ALIAS",
        properties = ModelTransform(modelName = "m_rep_cards_orig_account_alias_update__NEW_ACCOUNT_ALIAS")
    )
    m_rep_cards_orig_account_alias_update__rtr_fortrav_account_alias_expr_new_account_alias = Process(
        name = "m_rep_cards_orig_account_alias_update__RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS",
        properties = ModelTransform(
          modelName = "m_rep_cards_orig_account_alias_update__RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS"
        )
    )
    exp_account_alias_sp >> m_rep_cards_orig_account_alias_update__rtr_fortrav_account_alias_expr_new_account_alias
    (
        m_rep_cards_orig_account_alias_update__rtr_fortrav_account_alias_expr_new_account_alias._out(0)
        >> [m_rep_cards_orig_account_alias_update__new_account_alias._in(0),
              m_rep_cards_orig_account_alias_update__maintains._in(0)]
    )
    dbattribute >> m_rep_cards_orig_account_alias_update__exp_account_alias_vars_0
    m_rep_cards_orig_account_alias_update__exp_account_alias_vars_0 >> exp_account_alias_sp
