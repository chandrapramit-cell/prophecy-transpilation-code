from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__reformat_1 = Process(name = "p1__Reformat_1", properties = ModelTransform(modelName = "p1__Reformat_1"))
    seed_403_459 = Process(
        name = "seed_403_459",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_403_459", sourceType = "Seed")
        ),
        input_ports = None
    )
    seed_403_459 >> p1__reformat_1
