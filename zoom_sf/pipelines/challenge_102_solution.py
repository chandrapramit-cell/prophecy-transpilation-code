from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_102_solution",
    version = 1,
    auto_layout = False,
    params = Parameters(WORKFLOW_NAME = "'challenge_102_solution'")
)

with Pipeline(args) as pipeline:
    textinput_18 = Process(
        name = "TextInput_18",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_18", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_102_solution__dynamicrename_15_after = Process(
        name = "challenge_102_solution__DynamicRename_15_after",
        properties = ModelTransform(modelName = "challenge_102_solution__DynamicRename_15_after")
    )
    textinput_18 >> challenge_102_solution__dynamicrename_15_after
