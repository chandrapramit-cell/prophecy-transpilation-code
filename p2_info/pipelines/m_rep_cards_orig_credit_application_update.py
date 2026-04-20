from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_credit_application_update",
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
    m_rep_cards_orig_credit_application_update__close_credit_application = Process(
        name = "m_rep_cards_orig_credit_application_update__CLOSE_CREDIT_APPLICATION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__CLOSE_CREDIT_APPLICATION")
    )
    m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_321 = Process(
        name = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_321",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__EXP_WORK_CARDS_ORIG_STAGING_EXPR_321"),
        input_ports = None
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
    m_rep_cards_orig_credit_application_update__find_business_date = Process(
        name = "m_rep_cards_orig_credit_application_update__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_credit_application_update__find_business_date")
    )
    (
        m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group._out(0)
        >> [m_rep_cards_orig_credit_application_update__close_credit_application._in(0),
              m_rep_cards_orig_credit_application_update__open_credit_application._in(0)]
    )
    (
        m_rep_cards_orig_credit_application_update__exp_work_cards_orig_staging_expr_321._out(0)
        >> [m_rep_cards_orig_credit_application_update__ins_new_credit_application._in(0),
              m_rep_cards_orig_credit_application_update__rtr_credit_application_expr_other_day_group._in(0),
              m_rep_cards_orig_credit_application_update__upd_credit_application._in(0)]
    )
