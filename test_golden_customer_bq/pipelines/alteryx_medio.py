from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "alteryx_medio",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_Server_UYDB_215 = "''",
      password_aka_Server_UYDB_217 = "''",
      username_aka_Server_UYDB_220 = "''",
      username_aka_Server_UYDB_192 = "''",
      jdbcUrl_aka_Server_UYDB_192 = "''",
      password_aka_Server_UYDB_221 = "''",
      password_aka_Server_UYDB_214 = "''",
      username_aka_Server_UYDB_221 = "''",
      username_aka_Server_UYDB_214 = "''",
      password_aka_Server_UYDB_218 = "''",
      workflow_name = "'alteryx_medio'",
      username_aka_Server_UYDB_218 = "''",
      password_aka_Server_UYDB_220 = "''",
      password_aka_Server_UYDB_215 = "''",
      username_aka_Server_UYDB_217 = "''",
      username_aka_Server_UYDB_223 = "''",
      password_aka_Server_UYDB_213 = "''",
      password_aka_Server_UYDB_223 = "''",
      username_aka_Server_UYDB_219 = "''",
      username_aka_Server_UYDB_213 = "''",
      password_aka_Server_UYDB_219 = "''",
      password_aka_Server_UYDB_216 = "''",
      password_aka_Server_UYDB_192 = "''",
      password_aka_Server_UYDB_222 = "''",
      username_aka_Server_UYDB_216 = "''",
      username_aka_Server_UYDB_222 = "''"
    )
)

with Pipeline(args) as pipeline:
    alteryx_medio__textinput_285_cast = Process(
        name = "alteryx_medio__TextInput_285_cast",
        properties = ModelTransform(modelName = "alteryx_medio__TextInput_285_cast")
    )
    alteryx_medio__summarize_158 = Process(
        name = "alteryx_medio__Summarize_158",
        properties = ModelTransform(modelName = "alteryx_medio__Summarize_158")
    )
    alteryx_medio__union_170 = Process(
        name = "alteryx_medio__Union_170",
        properties = ModelTransform(modelName = "alteryx_medio__Union_170"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    alteryx_medio__appendfields_227 = Process(
        name = "alteryx_medio__AppendFields_227",
        properties = ModelTransform(modelName = "alteryx_medio__AppendFields_227"),
        input_ports = ["in_0", "in_1"]
    )
    textinput_224 = Process(
        name = "TextInput_224",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_224", sourceType = "Seed")
        ),
        input_ports = None
    )
    alteryx_medio__aka_server_uydb_192 = Process(
        name = "alteryx_medio__aka_Server_UYDB_192",
        properties = ModelTransform(modelName = "alteryx_medio__aka_Server_UYDB_192"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11"]
    )
    alteryx_medio__union_159 = Process(
        name = "alteryx_medio__Union_159",
        properties = ModelTransform(modelName = "alteryx_medio__Union_159"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    textinput_285 = Process(
        name = "TextInput_285",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_285", sourceType = "Seed")
        ),
        input_ports = None
    )
    jupytercode_286 = Process(
        name = "JupyterCode_286",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0"
        ),
        is_custom_output_schema = True
    )
    alteryx_medio__join_172_left = Process(
        name = "alteryx_medio__Join_172_left",
        properties = ModelTransform(modelName = "alteryx_medio__Join_172_left"),
        input_ports = ["in_0", "in_1"]
    )
    alteryx_medio__filter_160 = Process(
        name = "alteryx_medio__Filter_160",
        properties = ModelTransform(modelName = "alteryx_medio__Filter_160")
    )
    email_226 = Process(
        name = "Email_226",
        properties = Email(
          body = "Layout",
          bodyColumn = "Layout",
          subject = "Atencion!! Encuesta nueva recibida corregir codiguera",
          subjectFromColumn = False,
          showCcBcc = False,
          to = "lista_distribucion",
          toFromColumn = True,
          bodyFromColumn = True,
          toColumn = "lista_distribucion",
          ccFromColumn = False,
          bccFromColumn = False,
          subjectColumn = "Atencion!! Encuesta nueva recibida corregir codiguera",
          connection = "transpiled_connection"
        )
    )
    alteryx_medio__filter_160_reject = Process(
        name = "alteryx_medio__Filter_160_reject",
        properties = ModelTransform(modelName = "alteryx_medio__Filter_160_reject")
    )
    textinput_224 >> alteryx_medio__union_159._in(1)
    alteryx_medio__appendfields_227 >> email_226
    alteryx_medio__union_159._out(0) >> [alteryx_medio__summarize_158._in(0), alteryx_medio__appendfields_227._in(0)]
    alteryx_medio__summarize_158._out(0) >> [alteryx_medio__filter_160._in(0), alteryx_medio__filter_160_reject._in(0)]
    alteryx_medio__textinput_285_cast >> jupytercode_286
    textinput_285 >> alteryx_medio__textinput_285_cast
    alteryx_medio__union_170._out(0) >> [alteryx_medio__aka_server_uydb_192._in(6), alteryx_medio__join_172_left._in(0)]
    (
        alteryx_medio__join_172_left._out(0)
        >> [alteryx_medio__aka_server_uydb_192._in(11), alteryx_medio__union_159._in(0)]
    )
