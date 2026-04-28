from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "m_rep_cards_orig_work_staging_load",
    version = 1,
    auto_layout = False,
    params = Parameters(jdbcUrl = "''", username = "''", password = "''", SCHEMA = "'SCHEMA'")
)

with Pipeline(args) as pipeline:
    m_rep_cards_orig_work_staging_load__work_cards_orig_staging = Process(
        name = "m_rep_cards_orig_work_staging_load__WORK_CARDS_ORIG_STAGING",
        properties = ModelTransform(modelName = "m_rep_cards_orig_work_staging_load__WORK_CARDS_ORIG_STAGING"),
        input_ports = ["in_0", "in_1"]
    )

