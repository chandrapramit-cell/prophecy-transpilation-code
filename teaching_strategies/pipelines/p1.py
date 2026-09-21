from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__sample_1_1 = Process(name = "p1__Sample_1_1", properties = ModelTransform(modelName = "p1__Sample_1_1"))
    seed_databricks_calculation_engine_monthly_master_ec2_version_mom_590 = Process(
        name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(
            name = "seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590",
            sourceType = "Seed"
          )
        ),
        input_ports = None
    )
    seed_databricks_calculation_engine_monthly_master_ec2_version_mom_590 >> p1__sample_1_1
