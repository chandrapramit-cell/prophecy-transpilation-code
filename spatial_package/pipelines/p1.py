from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__createpoint_0 = Process(
        name = "p1__CreatePoint_0",
        properties = ModelTransform(modelName = "p1__CreatePoint_0"),
        input_ports = None
    )

