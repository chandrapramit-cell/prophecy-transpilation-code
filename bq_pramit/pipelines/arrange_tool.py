from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "arrange_tool",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'arrange_tool'")
)

with Pipeline(args) as pipeline:
    arrange_tool__arrange_2_selectcols = Process(
        name = "arrange_tool__Arrange_2_selectCols",
        properties = ModelTransform(modelName = "arrange_tool__Arrange_2_selectCols")
    )
    textinput_1 = Process(
        name = "TextInput_1",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_1", sourceType = "Seed")
        ),
        input_ports = None
    )
    arrange_tool__reformat_1 = Process(
        name = "arrange_tool__reformat_1",
        properties = ModelTransform(modelName = "arrange_tool__reformat_1")
    )
    fib_1 = Process(
        name = "fib_1",
        properties = CallStoredProc(storedProcedureIdentifier = "prophecy-databricks-qa.avpreetTables.fib", passThroughColumns = [])
    )
    textinput_1 >> arrange_tool__arrange_2_selectcols
    fib_1 >> arrange_tool__reformat_1
