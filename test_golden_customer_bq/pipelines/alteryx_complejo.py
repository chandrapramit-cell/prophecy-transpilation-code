from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "alteryx_complejo",
    version = 1,
    auto_layout = False,
    params = Parameters(
      password_aka_Server_UYDB_501 = "''",
      password_aka_Server_UYDB_696 = "''",
      username_aka_Server_UYDB_696 = "''",
      username_aka_Server_UYDB_543 = "''",
      password_aka_Server_UYDB_692 = "''",
      jdbcUrl_aka_Server_UYDB_51 = "''",
      password_aka_Server_UYDB_606 = "''",
      password_aka_Server_UYDB_597 = "''",
      password_aka_Server_UYDB_660 = "''",
      password_aka_Server_UYDB_551 = "''",
      username_aka_Server_UYDB_830 = "''",
      password_aka_Server_UYDB_504 = "''",
      username_aka_Server_UYDB_827 = "''",
      password_aka_Server_UYDB_616 = "''",
      username_aka_Server_UYDB_606 = "''",
      username_aka_Server_UYDB_504 = "''",
      username_aka_Server_UYDB_704 = "''",
      workflow_name = "'alteryx_complejo'",
      password_aka_Server_UYDB_311 = "''",
      username_aka_Server_UYDB_597 = "''",
      username_aka_Server_UYDB_51 = "''",
      password_aka_Server_UYDB_704 = "''",
      password_aka_Server_UYDB_662 = "''",
      username_aka_Server_UYDB_551 = "''",
      password_aka_Server_UYDB_194 = "''",
      password_aka_Server_UYDB_827 = "''",
      password_aka_Server_UYDB_543 = "''",
      username_aka_Server_UYDB_501 = "''",
      username_aka_Server_UYDB_201 = "''",
      username_aka_Server_UYDB_660 = "''",
      username_aka_Server_UYDB_311 = "''",
      username_aka_Server_UYDB_740 = "''",
      username_aka_Server_UYDB_502 = "''",
      password_aka_Server_UYDB_502 = "''",
      username_aka_Server_UYDB_194 = "''",
      username_aka_Server_UYDB_616 = "''",
      username_aka_Server_UYDB_662 = "''",
      password_aka_Server_UYDB_51 = "''",
      username_aka_Server_UYDB_692 = "''",
      password_aka_Server_UYDB_740 = "''",
      password_aka_Server_UYDB_830 = "''",
      password_aka_Server_UYDB_201 = "''"
    )
)

with Pipeline(args) as pipeline:
    alteryx_complejo__aka_server_uydb_51 = Process(
        name = "alteryx_complejo__aka_Server_UYDB_51",
        properties = ModelTransform(modelName = "alteryx_complejo__aka_Server_UYDB_51"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29", "in_30", "in_31", "in_32", "in_33", "in_34",
         "in_35"]
    )

