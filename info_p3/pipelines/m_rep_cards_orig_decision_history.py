from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_decision_history",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_rep_cards_orig_decision_history__credit_application_daily_hist = Process(
        name = "m_rep_cards_orig_decision_history__CREDIT_APPLICATION_DAILY_HIST",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__CREDIT_APPLICATION_DAILY_HIST")
    )
    m_rep_cards_orig_decision_history__minerva_exception = Process(
        name = "m_rep_cards_orig_decision_history__MINERVA_EXCEPTION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__MINERVA_EXCEPTION")
    )
    m_rep_cards_orig_decision_history__sq_cards_orig_decision_history = Process(
        name = "m_rep_cards_orig_decision_history__SQ_CARDS_ORIG_DECISION_HISTORY",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__SQ_CARDS_ORIG_DECISION_HISTORY"),
        input_ports = None
    )
    m_rep_cards_orig_decision_history__find_business_date = Process(
        name = "m_rep_cards_orig_decision_history__find_business_date",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__find_business_date")
    )
    (
        m_rep_cards_orig_decision_history__sq_cards_orig_decision_history._out(0)
        >> [m_rep_cards_orig_decision_history__credit_application_daily_hist._in(0),
              m_rep_cards_orig_decision_history__minerva_exception._in(0)]
    )
