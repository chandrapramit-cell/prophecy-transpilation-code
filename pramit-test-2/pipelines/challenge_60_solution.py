from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_60_solution",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'challenge_60_solution'")
)

with Pipeline(args) as pipeline:
    textinput_11 = Process(
        name = "TextInput_11",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_11", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_60_solution__summarize_10 = Process(
        name = "challenge_60_solution__Summarize_10",
        properties = ModelTransform(modelName = "challenge_60_solution__Summarize_10")
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_60_solution__textinput_11_cast = Process(
        name = "challenge_60_solution__TextInput_11_cast",
        properties = ModelTransform(modelName = "challenge_60_solution__TextInput_11_cast")
    )
    textinput_1 >> challenge_60_solution__summarize_10
    textinput_11 >> challenge_60_solution__textinput_11_cast
