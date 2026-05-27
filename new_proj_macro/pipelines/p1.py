from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    pipeline_1 = Process(
        name = "Pipeline_1",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "p1",
          parameterSet = "",
          parameters = {}
        )
    )
    anonymous_string_fields_missing_format = Process(
        name = "anonymous_string_fields_missing_format",
        properties = Dataset(
          table = Dataset.DBTSource(name = "s23", sourceType = "Seed"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        input_ports = None,
        comment = "Loads seed dataset s23 to refresh the target dataset by overwriting existing records."
    )
    p1__join_1 = Process(
        name = "p1__Join_1",
        properties = ModelTransform(modelName = "p1__Join_1"),
        input_ports = ["in_0", "in_1"]
    )
    s23 = Process(
        name = "s23",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "s23", sourceType = "Seed")
        ),
        input_ports = None
    )
    s23 = Process(
        name = "s23",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "s23", sourceType = "Seed")
        ),
        input_ports = None
    )
    anonymous_string_fields_missing_format >> p1__join_1._in(0)
    s23 >> p1__join_1._in(1)
    p1__join_1 >> pipeline_1
