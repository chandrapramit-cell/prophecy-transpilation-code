from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "1_11_0_0_Feed_Cards_Origination",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'1_11_0_0_Feed_Cards_Origination'")
)

with Pipeline(args) as pipeline:
    pass

