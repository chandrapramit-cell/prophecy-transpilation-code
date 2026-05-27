from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p2", version = 1, auto_layout = False, params = Parameters(workflow_name = "'p2'"))

with Pipeline(args) as pipeline:
    batchmacrooutputboundarytruncate_macro_5 = Process(
        name = "BatchMacroOutputBoundaryTruncate_Macro_5",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "spark.sql(\"TRUNCATE TABLE 14_5_Output14_macro_op\")\nout0 = in0"
        ),
        is_custom_output_schema = True
    )
    macro_5 = Process(
        name = "Macro_5",
        properties = PipelineTrigger(
          maxTriggers = 10000,
          triggerCondition = "Always",
          enableMaxTriggers = False,
          pipelineName = "p1",
          parameterSet = "default",
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
    p2__5_input19_macro_ip = Process(
        name = "p2__5_Input19_macro_ip",
        properties = ModelTransform(modelName = "p2__5_Input19_macro_ip")
    )
    p2__textinput_2_cast = Process(
        name = "p2__TextInput_2_cast",
        properties = ModelTransform(modelName = "p2__TextInput_2_cast")
    )
    textinput_2 >> p2__textinput_2_cast
    p2__textinput_2_cast >> macro_5
    textinput_3 >> p2__5_input19_macro_ip
    p2__5_input19_macro_ip >> batchmacrooutputboundarytruncate_macro_5
