from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "something", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    something__sqlstatement_0 = Process(
        name = "something__SQLStatement_0",
        properties = ModelTransform(modelName = "something__SQLStatement_0"),
        input_ports = None
    )

