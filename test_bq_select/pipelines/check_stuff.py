from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "check_stuff", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    check_stuff__reformat_1 = Process(
        name = "check_stuff__reformat_1",
        properties = ModelTransform(modelName = "check_stuff__reformat_1")
    )
    check_stuff__generate_record_id = Process(
        name = "check_stuff__generate_record_id",
        properties = ModelTransform(modelName = "check_stuff__generate_record_id")
    )
    check_stuff__query_with_redundant_aliases = Process(
        name = "check_stuff__query_with_redundant_aliases",
        properties = ModelTransform(modelName = "check_stuff__query_with_redundant_aliases")
    )
    random = Process(
        name = "random",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "random", sourceType = "Seed")
        ),
        input_ports = None
    )
    check_stuff__exclude_id_column = Process(
        name = "check_stuff__exclude_id_column",
        properties = ModelTransform(modelName = "check_stuff__exclude_id_column")
    )
    (
        random._out(0)
        >> [check_stuff__generate_record_id._in(0), check_stuff__reformat_1._in(0),
              check_stuff__exclude_id_column._in(0), check_stuff__query_with_redundant_aliases._in(0)]
    )
