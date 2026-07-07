from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    newmachinesample_xlsx_1 = Process(
        name = "NewMachineSample_xlsx_1",
        properties = DatabricksVolumeSource(
          compression = DatabricksVolumeSource.Compression(kind = "uncompressed"),
          connector = "databricks_default",
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            schema = "external_sources/p1/NewMachineSample_xlsx_1.yml"
          ),
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "/Volumes/main/anurag/data/NewMachineSample.xlsx"
          )
        ),
        input_ports = None
    )
    p1__recordid_1 = Process(name = "p1__RecordID_1", properties = ModelTransform(modelName = "p1__RecordID_1"))
    p1__remove_all_null_rows = Process(
        name = "p1__remove_all_null_rows",
        properties = ModelTransform(modelName = "p1__remove_all_null_rows")
    )
    p1_input_data = Process(
        name = "p1_input_data",
        properties = Dataset(table = Dataset.DBTSource(name = "p1_input_data", sourceType = "Seed")),
        input_ports = None
    )
    newmachinesample_xlsx_1 >> p1__recordid_1
    p1_input_data >> p1__remove_all_null_rows
