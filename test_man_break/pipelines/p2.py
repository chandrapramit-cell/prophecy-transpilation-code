from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p2", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p2__retrieved_records_ordered = Process(
        name = "p2__retrieved_records_ordered",
        properties = ModelTransform(modelName = "p2__retrieved_records_ordered")
    )

