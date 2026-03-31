from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "sdff", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    sdff__unpivot_0 = Process(
        name = "sdff__Unpivot_0",
        properties = ModelTransform(modelName = "sdff__Unpivot_0"),
        input_ports = None
    )

