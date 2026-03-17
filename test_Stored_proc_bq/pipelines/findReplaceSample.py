from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "findReplaceSample",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'findReplaceSample'")
)

with Pipeline(args) as pipeline:
    findreplacesample__findreplace_3_reorg_0 = Process(
        name = "findReplaceSample__FindReplace_3_reorg_0",
        properties = ModelTransform(modelName = "findReplaceSample__FindReplace_3_reorg_0")
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_2 = Process(
        name = "TextInput_2",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_2", sourceType = "Seed")
        ),
        input_ports = None
    )
    findreplace_3 = Process(
        name = "FindReplace_3",
        properties = CallStoredProc(
          storedProcedureIdentifier = "prophecy-databricks-qa.avpreetModels.find_replace_3",
          passThroughColumns = [{
             "alias": "description",
             "expression": {"expression" : "find_replace_3(description, TO_JSON_STRING(_rules))"}
           }]
        ),
        is_custom_output_schema = True
    )
    findreplacesample__findreplace_3_join = Process(
        name = "findReplaceSample__FindReplace_3_join",
        properties = ModelTransform(modelName = "findReplaceSample__FindReplace_3_join"),
        input_ports = ["in_0", "in_1"]
    )
    findreplace_3 >> findreplacesample__findreplace_3_reorg_0
    textinput_1 >> findreplacesample__findreplace_3_join._in(0)
    textinput_2 >> findreplacesample__findreplace_3_join._in(1)
    findreplacesample__findreplace_3_join >> findreplace_3
