from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p2", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p2__unpivot_0 = Process(name = "p2__Unpivot_0", properties = ModelTransform(modelName = "p2__Unpivot_0"))

