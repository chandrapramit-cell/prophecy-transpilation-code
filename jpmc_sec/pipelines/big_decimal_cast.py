from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "big_decimal_cast", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    big_decimal_cast__reformat_as_decimal = Process(
        name = "big_decimal_cast__reformat_as_decimal",
        properties = ModelTransform(modelName = "big_decimal_cast__reformat_as_decimal")
    )
    big_decimal_cast__reformat_as_double = Process(
        name = "big_decimal_cast__reformat_as_double",
        properties = ModelTransform(modelName = "big_decimal_cast__reformat_as_double")
    )
    big_decimal_strings = Process(
        name = "big_decimal_strings",
        properties = Dataset(table = Dataset.DBTSource(name = "big_decimal_strings", sourceType = "Seed")),
        input_ports = None
    )
    (
        big_decimal_strings._out(0)
        >> [big_decimal_cast__reformat_as_decimal._in(0), big_decimal_cast__reformat_as_double._in(0)]
    )
