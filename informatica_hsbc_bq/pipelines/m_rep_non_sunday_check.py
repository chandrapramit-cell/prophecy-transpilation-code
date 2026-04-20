from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_non_sunday_check",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_rep_non_sunday_check__rep_inf_workflow = Process(
        name = "m_rep_non_sunday_check__REP_INF_WORKFLOW",
        properties = ModelTransform(modelName = "m_rep_non_sunday_check__REP_INF_WORKFLOW"),
        input_ports = None
    )

