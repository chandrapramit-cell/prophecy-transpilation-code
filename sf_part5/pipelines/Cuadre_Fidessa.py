from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Cuadre_Fidessa",
    version = 1,
    auto_layout = False,
    params = Parameters(
      FICHERO_HYDRA = "''",
      FICHERO_NTPA = "''",
      USERNAME_FICHERONTPA_XLS_4 = "''",
      PASSWORD_FICHERONTPA_XLS_4 = "''",
      USERNAME_FICHEROHYDRA_XL_3 = "''",
      PASSWORD_FICHEROHYDRA_XL_3 = "''",
      JDBCURL_CUADRE_XLSX_TAB_90 = "''",
      USERNAME_CUADRE_XLSX_TAB_90 = "''",
      PASSWORD_CUADRE_XLSX_TAB_90 = "''",
      JDBCURL_OPSHYDRANOFIDES_83 = "''",
      USERNAME_OPSHYDRANOFIDES_83 = "''",
      PASSWORD_OPSHYDRANOFIDES_83 = "''",
      JDBCURL_OPSNTPANOHYDRA__86 = "''",
      USERNAME_OPSNTPANOHYDRA__86 = "''",
      PASSWORD_OPSNTPANOHYDRA__86 = "''",
      WORKFLOW_NAME = "'Cuadre_Fidessa'",
      QUESTION__FILE_BROWSE_72 = "''",
      QUESTION__FILE_BROWSE_73 = "''"
    )
)

with Pipeline(args) as pipeline:
    cuadre_xlsx_tab_90 = Process(
        name = "CUADRE_xlsx_Tab_90",
        properties = SFTPTarget(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = SFTPTarget.SFTPTargetInternal(filePath = ".\\CUADRE.xlsx"),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    cuadre_fidessa__alteryxselect_42 = Process(
        name = "Cuadre_Fidessa__AlteryxSelect_42",
        properties = ModelTransform(modelName = "Cuadre_Fidessa__AlteryxSelect_42")
    )
    cuadre_fidessa__appendfields_82 = Process(
        name = "Cuadre_Fidessa__AppendFields_82",
        properties = ModelTransform(modelName = "Cuadre_Fidessa__AppendFields_82"),
        input_ports = ["in_0", "in_1"]
    )
    cuadre_fidessa__appendfields_85 = Process(
        name = "Cuadre_Fidessa__AppendFields_85",
        properties = ModelTransform(modelName = "Cuadre_Fidessa__AppendFields_85"),
        input_ports = ["in_0", "in_1"]
    )
    cuadre_fidessa__appendfields_89 = Process(
        name = "Cuadre_Fidessa__AppendFields_89",
        properties = ModelTransform(modelName = "Cuadre_Fidessa__AppendFields_89"),
        input_ports = ["in_0", "in_1"]
    )
    cuadre_fidessa__formula_46_0 = Process(
        name = "Cuadre_Fidessa__Formula_46_0",
        properties = ModelTransform(modelName = "Cuadre_Fidessa__Formula_46_0")
    )
    ficherohydra_xl_3 = Process(
        name = "FICHEROHYDRA_xl_3",
        properties = SFTPSource(
          connector = {
            "id": "transpiled_connection",
            "kind": "sftp",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Cuadre_Fidessa/FICHEROHYDRA_xl_3.yml"),
          properties = SFTPSource.SFTPSourceInternal(filePath = ".\\FICHERO HYDRA.xlsx")
        ),
        input_ports = None
    )
    ficherontpa_xls_4 = Process(
        name = "FICHERONTPA_xls_4",
        properties = SFTPSource(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = SFTPSource.SFTPSourceInternal(filePath = ".\\FICHERO NTPA.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Cuadre_Fidessa/FICHERONTPA_xls_4.yml")
        ),
        input_ports = None
    )
    opshydranofides_83 = Process(
        name = "OPSHYDRANOFIDES_83",
        properties = SFTPTarget(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = SFTPTarget.SFTPTargetInternal(filePath = ".\\OPS HYDRA NO FIDESSA.xlsx"),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    opsntpanohydra__86 = Process(
        name = "OPSNTPANOHYDRA__86",
        properties = SFTPTarget(
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {
              "authMethod": "password",
              "username": "transpiled_username",
              "host": "sftp.prophecy.io",
              "id": "transpiled_connection",
              "port": 22,
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = SFTPTarget.SFTPTargetInternal(filePath = ".\\OPS NTPA NO HYDRA.xlsx"),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    cuadre_fidessa__appendfields_85 >> opsntpanohydra__86
    (
        cuadre_fidessa__alteryxselect_42._out(0)
        >> [cuadre_fidessa__appendfields_82._in(0), cuadre_fidessa__appendfields_85._in(0),
              cuadre_fidessa__appendfields_89._in(0)]
    )
    cuadre_fidessa__appendfields_82 >> opshydranofides_83
    ficherontpa_xls_4 >> cuadre_fidessa__formula_46_0
    (
        cuadre_fidessa__formula_46_0._out(0)
        >> [cuadre_fidessa__appendfields_82._in(1), cuadre_fidessa__appendfields_85._in(1),
              cuadre_fidessa__appendfields_89._in(1)]
    )
    ficherohydra_xl_3 >> cuadre_fidessa__alteryxselect_42
    cuadre_fidessa__appendfields_89 >> cuadre_xlsx_tab_90
