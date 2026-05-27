from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "test", version = 1, auto_layout = False, params = Parameters(workflow_name = "'test'"))

with Pipeline(args) as pipeline:
    macro_5 = Process(
        name = "Macro_5",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "test",
          parameters = {"Box_Size" : "Box Size"}
        )
    )
    textinput_2 = Process(
        name = "TextInput_2",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_test_2", sourceType = "Seed")
        ),
        input_ports = None
    )
    textinput_3 = Process(
        name = "TextInput_3",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_test_3", sourceType = "Seed")
        ),
        input_ports = None
    )
    test__5_input19_macro_ip = Process(
        name = "test__5_Input19_macro_ip",
        properties = ModelTransform(modelName = "test__5_Input19_macro_ip")
    )
    test__batchmacrooutputboundarytruncate_macro_5 = Process(
        name = "test__BatchMacroOutputBoundaryTruncate_Macro_5",
        properties = ModelTransform(modelName = "test__BatchMacroOutputBoundaryTruncate_Macro_5")
    )
    test__textinput_2_cast = Process(
        name = "test__TextInput_2_cast",
        properties = ModelTransform(modelName = "test__TextInput_2_cast")
    )
    textinput_2 >> test__textinput_2_cast
    test__textinput_2_cast >> macro_5
    textinput_3 >> test__5_input19_macro_ip
    test__5_input19_macro_ip >> test__batchmacrooutputboundarytruncate_macro_5
