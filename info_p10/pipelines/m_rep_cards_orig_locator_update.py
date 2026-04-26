from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_locator_update",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
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
    m_rep_cards_orig_locator_update__exp_cards_orig_locator = Process(
        name = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR"),
        input_ports = None
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
    m_rep_cards_orig_locator_update__find_business_date = Process(
        name = "m_rep_cards_orig_locator_update__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_locator_update__find_business_date")
    )
    (
        m_rep_cards_orig_locator_update__rtr_cards_orig_locator_expr_open_close_locator._out(0)
        >> [m_rep_cards_orig_locator_update__ins_locator_open._in(0),
              m_rep_cards_orig_locator_update__upd_locator._in(0)]
    )
    (
        m_rep_cards_orig_locator_update__exp_cards_orig_locator._out(0)
        >> [m_rep_cards_orig_locator_update__upd_locator_close._in(0),
              m_rep_cards_orig_locator_update__ins_locator_new._in(0),
              m_rep_cards_orig_locator_update__rtr_cards_orig_locator_expr_open_close_locator._in(0),
              m_rep_cards_orig_locator_update__chooses._in(0)]
    )
