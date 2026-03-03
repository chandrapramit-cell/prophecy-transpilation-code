from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "test", version = 1, auto_layout = False, params = Parameters(workflow_name = "'test'"))

with Pipeline(args) as pipeline:
    test__recordid_1 = Process(
        name = "test__RecordID_1",
        properties = ModelTransform(modelName = "test__RecordID_1"),
        input_ports = None
    )

