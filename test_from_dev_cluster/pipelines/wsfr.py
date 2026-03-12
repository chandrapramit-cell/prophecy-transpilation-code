from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "wsfr", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    wsfr__reformat_1 = Process(name = "wsfr__Reformat_1", properties = ModelTransform(modelName = "wsfr__Reformat_1"))
    table_0 = Process(
        name = "Table_0",
        properties = Dataset(table = Dataset.DBTSource(name = "sf", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None
    )
    wsfr__flattenschema_1 = Process(
        name = "wsfr__FlattenSchema_1",
        properties = ModelTransform(modelName = "wsfr__FlattenSchema_1")
    )
    wsfr__sqlstatement_1 = Process(
        name = "wsfr__SQLStatement_1",
        properties = ModelTransform(modelName = "wsfr__SQLStatement_1")
    )
    wsfr__recordid_1 = Process(name = "wsfr__RecordID_1", properties = ModelTransform(modelName = "wsfr__RecordID_1"))
    (
        table_0._out(0)
        >> [wsfr__reformat_1._in(0), wsfr__flattenschema_1._in(0), wsfr__sqlstatement_1._in(0),
              wsfr__recordid_1._in(0)]
    )
