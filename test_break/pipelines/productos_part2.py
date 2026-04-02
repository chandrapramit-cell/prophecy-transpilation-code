from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "productos_part2",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_Server_UYDB_1033 = "''",
      username_aka_Server_UYDB_1037 = "''",
      username_aka_Server_UYDB_1044 = "''",
      password_aka_Server_UYDB_1039 = "''",
      password_aka_Server_UYDB_1034 = "''",
      username_aka_Server_UYDB_1041 = "''",
      password_aka_Server_UYDB_1128 = "''",
      password_aka_Server_UYDB_1045 = "''",
      jdbcUrl_aka_Server_UYDB_898 = "''",
      password_aka_Server_UYDB_1030 = "''",
      password_aka_Server_UYDB_898 = "''",
      password_aka_Server_UYDB_1041 = "''",
      username_aka_Server_UYDB_1128 = "''",
      username_aka_Server_UYDB_1030 = "''",
      username_aka_Server_UYDB_1048 = "''",
      password_aka_Server_UYDB_1042 = "''",
      password_aka_Server_UYDB_1038 = "''",
      username_aka_Server_UYDB_898 = "''",
      password_aka_Server_UYDB_1048 = "''",
      workflow_name = "'productos'",
      password_aka_Server_UYDB_1037 = "''",
      username_aka_Server_UYDB_1036 = "''",
      username_aka_Server_UYDB_1116 = "''",
      password_aka_Server_UYDB_1035 = "''",
      username_aka_Server_UYDB_1031 = "''",
      username_aka_Server_UYDB_1042 = "''",
      password_aka_Server_UYDB_1046 = "''",
      password_aka_Server_UYDB_1031 = "''",
      username_aka_Server_UYDB_900 = "''",
      password_aka_Server_UYDB_1040 = "''",
      password_aka_Server_UYDB_1043 = "''",
      username_aka_Server_UYDB_1039 = "''",
      password_aka_Server_UYDB_1032 = "''",
      password_aka_Server_UYDB_1047 = "''",
      password_aka_Server_UYDB_1036 = "''",
      username_aka_Server_UYDB_1035 = "''",
      username_aka_Server_UYDB_1043 = "''",
      username_aka_Server_UYDB_1032 = "''",
      username_aka_Server_UYDB_1047 = "''",
      jdbcUrl_aka_Server_UYDB_900 = "''",
      username_aka_Server_UYDB_1034 = "''",
      password_aka_Server_UYDB_1044 = "''",
      username_aka_Server_UYDB_1045 = "''",
      password_aka_Server_UYDB_1033 = "''",
      username_aka_Server_UYDB_1038 = "''",
      username_aka_Server_UYDB_1040 = "''",
      username_aka_Server_UYDB_1046 = "''",
      password_aka_Server_UYDB_1116 = "''",
      password_aka_Server_UYDB_900 = "''"
    )
)

with Pipeline(args) as pipeline:
    productos_part2__union_895 = Process(
        name = "productos_part2__Union_895",
        properties = ModelTransform(modelName = "productos_part2__Union_895"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29", "in_30", "in_31"]
    )
    productos_part2__aka_server_uydb_900 = Process(
        name = "productos_part2__aka_Server_UYDB_900",
        properties = ModelTransform(modelName = "productos_part2__aka_Server_UYDB_900")
    )
    productos_part2__aka_server_uydb_898 = Process(
        name = "productos_part2__aka_Server_UYDB_898",
        properties = ModelTransform(modelName = "productos_part2__aka_Server_UYDB_898")
    )
    (
        productos_part2__union_895._out(0)
        >> [productos_part2__aka_server_uydb_900._in(0), productos_part2__aka_server_uydb_898._in(0)]
    )
