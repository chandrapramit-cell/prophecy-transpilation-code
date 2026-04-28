from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_locator_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_cards_orig_locator_sp = Process(
        name = "EXP_CARDS_ORIG_LOCATOR_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'U'",
            "arg2": "'360'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN business_date_string__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "CLID_CLIENT_ALIAS", "expression" : {"expression" : "CLID_CLIENT_ALIAS"}},
           {"alias" : "MERGE_CLID_CLIENT_ALIAS", "expression" : {"expression" : "MERGE_CLID_CLIENT_ALIAS"}},
           {"alias" : "CLASS", "expression" : {"expression" : "CLASS"}},
           {"alias" : "SUBCLASS", "expression" : {"expression" : "SUBCLASS"}},
           {"alias" : "VALUE", "expression" : {"expression" : "VALUE"}},
           {"alias" : "CLID_LOCATOR", "expression" : {"expression" : "CLID_LOCATOR"}},
           {"alias" : "EFROM_LOCATOR", "expression" : {"expression" : "EFROM_LOCATOR"}},
           {"alias" : "CLASS_LOCATOR", "expression" : {"expression" : "CLASS_LOCATOR"}},
           {"alias" : "SUBCLASS_LOCATOR", "expression" : {"expression" : "SUBCLASS_LOCATOR"}},
           {"alias" : "VALUE_LOCATOR", "expression" : {"expression" : "VALUE_LOCATOR"}},
           {"alias" : "CONTACT_POINT_EXTRA_INFO", "expression" : {"expression" : "CONTACT_POINT_EXTRA_INFO"}},
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
    m_rep_cards_orig_locator_update__chooses = Process(
        name = "m_rep_cards_orig_locator_update__CHOOSES",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__CHOOSES")
    )
    m_rep_cards_orig_locator_update__exp_cards_orig_locator_vars_0 = Process(
        name = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR_VARS_0",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR_VARS_0")
    )
    m_rep_cards_orig_locator_update__exp_cards_orig_locator_reformat = Process(
        name = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR_reformat",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR_reformat")
    )
    m_rep_cards_orig_locator_update__ins_locator_new = Process(
        name = "m_rep_cards_orig_locator_update__INS_LOCATOR_NEW",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__INS_LOCATOR_NEW")
    )
    m_rep_cards_orig_locator_update__ins_locator_open = Process(
        name = "m_rep_cards_orig_locator_update__INS_LOCATOR_OPEN",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__INS_LOCATOR_OPEN")
    )
    m_rep_cards_orig_locator_update__rtr_cards_orig_locator_expr_open_close_locator = Process(
        name = "m_rep_cards_orig_locator_update__RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR")
    )
    m_rep_cards_orig_locator_update__upd_locator = Process(
        name = "m_rep_cards_orig_locator_update__UPD_LOCATOR",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__UPD_LOCATOR")
    )
    m_rep_cards_orig_locator_update__upd_locator_close = Process(
        name = "m_rep_cards_orig_locator_update__UPD_LOCATOR_CLOSE",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__UPD_LOCATOR_CLOSE")
    )
    (
        m_rep_cards_orig_locator_update__rtr_cards_orig_locator_expr_open_close_locator._out(0)
        >> [m_rep_cards_orig_locator_update__ins_locator_open._in(0),
              m_rep_cards_orig_locator_update__upd_locator._in(0)]
    )
    exp_cards_orig_locator_sp >> m_rep_cards_orig_locator_update__exp_cards_orig_locator_reformat
    (
        m_rep_cards_orig_locator_update__exp_cards_orig_locator_reformat._out(0)
        >> [m_rep_cards_orig_locator_update__upd_locator_close._in(0),
              m_rep_cards_orig_locator_update__ins_locator_new._in(0),
              m_rep_cards_orig_locator_update__rtr_cards_orig_locator_expr_open_close_locator._in(0),
              m_rep_cards_orig_locator_update__chooses._in(0)]
    )
    m_rep_cards_orig_locator_update__exp_cards_orig_locator_vars_0 >> exp_cards_orig_locator_sp
