from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_ohc_card_detail_closure",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    exp_construct_card_detail_sp = Process(
        name = "EXP_CONSTRUCT_CARD_DETAIL_sp",
        properties = CallStoredProc(
          storedProcedureIdentifier = "feed_number_generator1",
          parameters = {
            "arg1": "'Y'",
            "arg2": "'560'",
            "arg3": "CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN BUSINESS_DATE_STRING__find_business_date_lkp_1 ELSE cast(null as STRING) END",
            "arg4": "substring(CASE WHEN sum(1) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST) = 1 THEN BUSINESS_DATE_STRING__find_business_date_lkp_1 ELSE cast(null as STRING) END, 1, 8)"
          },
          passThroughColumns = [{"alias" : "PREVIOUS_PROGRAM_ID", "expression" : {"expression" : "PREVIOUS_PROGRAM_ID"}},
           {"alias" : "CARD_NUMBER", "expression" : {"expression" : "CARD_NUMBER"}},
           {"alias" : "ACTIVE_DATE", "expression" : {"expression" : "ACTIVE_DATE"}},
           {"alias" : "BLOCK_CODE", "expression" : {"expression" : "BLOCK_CODE"}},
           {"alias" : "EXPIRY_DATE", "expression" : {"expression" : "EXPIRY_DATE"}},
           {"alias" : "CARDHOLDER_FLAG", "expression" : {"expression" : "CARDHOLDER_FLAG"}},
           {"alias" : "NUMBER_CARDS", "expression" : {"expression" : "NUMBER_CARDS"}},
           {"alias" : "ACTV_RQD_CURR", "expression" : {"expression" : "ACTV_RQD_CURR"}},
           {"alias" : "ACTV_RQD_PREV", "expression" : {"expression" : "ACTV_RQD_PREV"}},
           {"alias" : "EMBOSSER_NAME1", "expression" : {"expression" : "EMBOSSER_NAME1"}},
           {"alias" : "PRIOR_CARD_EXPIRY", "expression" : {"expression" : "PRIOR_CARD_EXPIRY"}},
           {"alias" : "PRY_SDY_IND", "expression" : {"expression" : "PRY_SDY_IND"}},
           {"alias" : "CURR_PREV_IND", "expression" : {"expression" : "CURR_PREV_IND"}},
           {"alias" : "ACCID", "expression" : {"expression" : "ACCID"}},
           {"alias" : "ACTUAL_PROGRAM_ID", "expression" : {"expression" : "ACTUAL_PROGRAM_ID"}},
           {"alias" : "prophecy_sk", "expression" : {"expression" : "prophecy_sk"}},
           {
             "alias": "BUSINESS_DATE_STRING__find_business_date_lkp_1",
             "expression": {"expression" : "BUSINESS_DATE_STRING__find_business_date_lkp_1"}
           },
           {"alias" : "LOOKUP_VARIABLE_4", "expression" : {"expression" : "LOOKUP_VARIABLE_4"}}]
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
    m_rep_ohc_card_detail_closure__close_card_detail = Process(
        name = "m_rep_ohc_card_detail_closure__CLOSE_CARD_DETAIL",
        properties = ModelTransform(modelName = "m_rep_ohc_card_detail_closure__CLOSE_CARD_DETAIL")
    )
    m_rep_ohc_card_detail_closure__exp_construct_card_detail_vars_0 = Process(
        name = "m_rep_ohc_card_detail_closure__EXP_CONSTRUCT_CARD_DETAIL_VARS_0",
        properties = ModelTransform(modelName = "m_rep_ohc_card_detail_closure__EXP_CONSTRUCT_CARD_DETAIL_VARS_0"),
        input_ports = ["in_0", "in_1"]
    )
    m_rep_ohc_card_detail_closure__minerva_exception = Process(
        name = "m_rep_ohc_card_detail_closure__MINERVA_EXCEPTION",
        properties = ModelTransform(modelName = "m_rep_ohc_card_detail_closure__MINERVA_EXCEPTION"),
        input_ports = ["in_0", "in_1"]
    )
    m_rep_ohc_card_detail_closure__sq_card_details_staging = Process(
        name = "m_rep_ohc_card_detail_closure__SQ_CARD_DETAILS_STAGING",
        properties = ModelTransform(modelName = "m_rep_ohc_card_detail_closure__SQ_CARD_DETAILS_STAGING"),
        input_ports = None
    )
    exp_construct_card_detail_sp >> m_rep_ohc_card_detail_closure__close_card_detail
    m_rep_ohc_card_detail_closure__exp_construct_card_detail_vars_0 >> exp_construct_card_detail_sp
    (
        m_rep_ohc_card_detail_closure__sq_card_details_staging._out(0)
        >> [m_rep_ohc_card_detail_closure__minerva_exception._in(1),
              m_rep_ohc_card_detail_closure__exp_construct_card_detail_vars_0._in(1)]
    )
