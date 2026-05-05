from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__except_1 = Process(name = "p1__Except_1", properties = ModelTransform(modelName = "p1__Except_1"), input_ports = None)
    p1__unionbyname_1 = Process(
        name = "p1__UnionByName_1",
        properties = ModelTransform(modelName = "p1__UnionByName_1"),
        input_ports = None
    )
    p1__union_1 = Process(
        name = "p1__Union_1",
        properties = ModelTransform(modelName = "p1__Union_1"),
        input_ports = ["in_0", "in_1"]
    )
    seed_61 = Process(
        name = "seed_61",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_61", sourceType = "Seed")
        ),
        input_ports = None
    )
    seed_61._out(0) >> [p1__union_1._in(0), p1__union_1._in(1)]
