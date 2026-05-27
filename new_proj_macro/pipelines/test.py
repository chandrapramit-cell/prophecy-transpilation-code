from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "test",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Box_Size = 6,
      variable11_FormulaFields_FormulaField_expression = Expr("ceil((Units / {{ var('Box_Size') }}))"),
      workflow_name = "'test'",
      Question__Macro_Output_14 = "''",
      Question__ControlParam__Control_Parameter_16 = "''",
      Question__Macro_Input_19 = "''"
    )
)

with Pipeline(args) as pipeline:
    test__14_5_output14_macro_op = Process(
        name = "test__14_5_Output14_macro_op",
        properties = ModelTransform(modelName = "test__14_5_Output14_macro_op")
    )

