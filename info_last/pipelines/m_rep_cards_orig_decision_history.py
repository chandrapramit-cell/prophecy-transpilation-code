from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_decision_history",
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
    m_rep_cards_orig_decision_history__credit_application_daily_hist = Process(
        name = "m_rep_cards_orig_decision_history__CREDIT_APPLICATION_DAILY_HIST",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__CREDIT_APPLICATION_DAILY_HIST")
    )
    m_rep_cards_orig_decision_history__minerva_exception = Process(
        name = "m_rep_cards_orig_decision_history__MINERVA_EXCEPTION",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__MINERVA_EXCEPTION"),
        input_ports = ["in_0", "in_1"]
    )
    m_rep_cards_orig_decision_history__sq_cards_orig_decision_history = Process(
        name = "m_rep_cards_orig_decision_history__SQ_CARDS_ORIG_DECISION_HISTORY",
        properties = ModelTransform(modelName = "m_rep_cards_orig_decision_history__SQ_CARDS_ORIG_DECISION_HISTORY"),
        input_ports = None
    )
    dbattribute >> m_rep_cards_orig_decision_history__minerva_exception._in(1)
    (
        m_rep_cards_orig_decision_history__sq_cards_orig_decision_history._out(0)
        >> [m_rep_cards_orig_decision_history__credit_application_daily_hist._in(0),
              m_rep_cards_orig_decision_history__minerva_exception._in(0)]
    )
