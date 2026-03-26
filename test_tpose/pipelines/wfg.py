from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "wfg", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    store_sales_qtrly_csv = Process(
        name = "store_sales_qtrly_csv",
        properties = Dataset(table = Dataset.DBTSource(name = "s", sourceType = "Seed"), writeOptions = {"writeMode" : "overwrite"}),
        input_ports = None,
        comment = "Updates the seed dataset and refreshes the target table to reflect latest source data."
    )
    wfg__unpivot_1 = Process(name = "wfg__Unpivot_1", properties = ModelTransform(modelName = "wfg__Unpivot_1"))
    wfg__transpose_store_sales_quad_to_dimensions = Process(
        name = "wfg__transpose_store_sales_quad_to_dimensions",
        properties = ModelTransform(modelName = "wfg__transpose_store_sales_quad_to_dimensions")
    )
    store_sales_qtrly_csv._out(0) >> [wfg__transpose_store_sales_quad_to_dimensions._in(0), wfg__unpivot_1._in(0)]
