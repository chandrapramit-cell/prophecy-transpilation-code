from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_138_soltuion_GR",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'challenge_138_soltuion_GR'")
)

with Pipeline(args) as pipeline:
    challenge_138_soltuion_gr__alteryxselect_62 = Process(
        name = "challenge_138_soltuion_GR__AlteryxSelect_62",
        properties = ModelTransform(modelName = "challenge_138_soltuion_GR__AlteryxSelect_62")
    )
    textinput_44 = Process(
        name = "TextInput_44",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_44", sourceType = "Seed")
        ),
        input_ports = None
    )
    challenge_138_soltuion_gr__textinput_44_cast = Process(
        name = "challenge_138_soltuion_GR__TextInput_44_cast",
        properties = ModelTransform(modelName = "challenge_138_soltuion_GR__TextInput_44_cast")
    )
    textinput_42 = Process(
        name = "TextInput_42",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_42", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_42 >> challenge_138_soltuion_gr__alteryxselect_62
    textinput_44 >> challenge_138_soltuion_gr__textinput_44_cast
