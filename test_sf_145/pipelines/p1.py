from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    table_0 = Process(
        name = "Table_0",
        properties = Dataset(
          table = Dataset.DBTSource(name = "ABC", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None
    )
    p1__reformat_1 = Process(name = "p1__Reformat_1", properties = ModelTransform(modelName = "p1__Reformat_1"))
    table_0 >> p1__reformat_1
