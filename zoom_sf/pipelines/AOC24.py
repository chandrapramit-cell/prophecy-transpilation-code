from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "AOC24", version = 1, auto_layout = False, params = Parameters(WORKFLOW_NAME = "'AOC24'"))

with Pipeline(args) as pipeline:
    aoc24__filter_16 = Process(name = "AOC24__Filter_16", properties = ModelTransform(modelName = "AOC24__Filter_16"))
    aoc24__filter_16_reject = Process(
        name = "AOC24__Filter_16_reject",
        properties = ModelTransform(modelName = "AOC24__Filter_16_reject")
    )
    aoc24__filter_17 = Process(name = "AOC24__Filter_17", properties = ModelTransform(modelName = "AOC24__Filter_17"))
    aoc24__textinput_1_cast = Process(
        name = "AOC24__TextInput_1_cast",
        properties = ModelTransform(modelName = "AOC24__TextInput_1_cast")
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_15 = Process(
        name = "TextInput_15",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_15", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_1 >> aoc24__textinput_1_cast
    textinput_15 >> aoc24__filter_17
    aoc24__filter_17._out(0) >> [aoc24__filter_16._in(0), aoc24__filter_16_reject._in(0)]
