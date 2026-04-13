from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Resumo_de_CFOP_SAPXSATI",
    version = 1,
    auto_layout = False,
    params = Parameters(
      INSERIR_LIVRO_DE_ENTRADAS_SAP = "''",
      INSERIR_LIVRO_DE_SAIDAS_SAP = "''",
      INSERIR_RESUMO_DE_CFOP_SATI = "''",
      USERNAME_ENTRADASSAPF001_1 = "''",
      PASSWORD_ENTRADASSAPF001_1 = "''",
      USERNAME_SAIDASSAP02_22__26 = "''",
      PASSWORD_SAIDASSAP02_22__26 = "''",
      USERNAME_RESUMOCFOPSATI0_41 = "''",
      PASSWORD_RESUMOCFOPSATI0_41 = "''",
      JDBCURL_ENTRADASRESUMOS_61 = "''",
      USERNAME_ENTRADASRESUMOS_61 = "''",
      PASSWORD_ENTRADASRESUMOS_61 = "''",
      JDBCURL_RESUMOCFOPSAPES_60 = "''",
      USERNAME_RESUMOCFOPSAPES_60 = "''",
      PASSWORD_RESUMOCFOPSAPES_60 = "''",
      WORKFLOW_NAME = "'Resumo_de_CFOP_SAPXSATI'",
      QUESTION__FILE_BROWSE_63 = "''",
      QUESTION__FILE_BROWSE_65 = "''",
      QUESTION__FILE_BROWSE_67 = "''"
    )
)

with Pipeline(args) as pipeline:
    entradassapf001_1 = Process(
        name = "ENTRADASSAPF001_1",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "C:\\Users\\silvagu4\\Downloads\\ENTRADAS SAP F001 02.2022.xlsx"),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Resumo_de_CFOP_SAPXSATI/ENTRADASSAPF001_1.yml")
        ),
        input_ports = None
    )
    entradasresumos_61 = Process(
        name = "EntradasResumoS_61",
        properties = SFTPTarget(
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
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "C:\\Users\\silvagu4\\OneDrive - Mars Inc\\Desktop\\Entradas Resumo SAP.xlsx"
          )
        )
    )
    resumocfopsapes_60 = Process(
        name = "RESUMOCFOPSAPES_60",
        properties = SFTPTarget(
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
          format = SFTPTarget.XLSXWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "C:\\Users\\silvagu4\\OneDrive - Mars Inc\\Desktop\\RESUMO CFOP SAP E SATI 02.22.xlsx"
          )
        )
    )
    resumocfopsati0_41 = Process(
        name = "RESUMOCFOPSATI0_41",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "C:\\Users\\silvagu4\\OneDrive - Mars Inc\\Desktop\\RESUMO CFOP SATI 02.22.XLSX",
              "username": "${username_RESUMOCFOPSATI0_41}",
              "id": "transpiled_connection",
              "port": "1521",
              "password": {
                "kind": "prophecy",
                "properties": {"name" : "transpiled_secret", "value" : "transpiled_secret"},
                "subKind": "text",
                "type": "secret"
              }
            },
            "type": "connector"
          },
          properties = OracleSource.OracleSourceInternal(
            pathSelection = "warehouseQuery",
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "RESUMOCFOPSATI0_41"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "`Sheet1$`")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/Resumo_de_CFOP_SAPXSATI/RESUMOCFOPSATI0_41.yml")
        ),
        input_ports = None
    )
    resumo_de_cfop_sapxsati__filter_11_reject = Process(
        name = "Resumo_de_CFOP_SAPXSATI__Filter_11_reject",
        properties = ModelTransform(modelName = "Resumo_de_CFOP_SAPXSATI__Filter_11_reject")
    )
    resumo_de_cfop_sapxsati__formula_57_0 = Process(
        name = "Resumo_de_CFOP_SAPXSATI__Formula_57_0",
        properties = ModelTransform(modelName = "Resumo_de_CFOP_SAPXSATI__Formula_57_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    resumo_de_cfop_sapxsati__multirowformula_10_row_id_drop_0 = Process(
        name = "Resumo_de_CFOP_SAPXSATI__MultiRowFormula_10_row_id_drop_0",
        properties = ModelTransform(modelName = "Resumo_de_CFOP_SAPXSATI__MultiRowFormula_10_row_id_drop_0")
    )
    saidassap02_22__26 = Process(
        name = "SAIDASSAP02_22__26",
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
          properties = SFTPSource.SFTPSourceInternal(
            filePath = "C:\\Users\\silvagu4\\OneDrive - Mars Inc\\Desktop\\SAIDAS SAP 02.22.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/Resumo_de_CFOP_SAPXSATI/SAIDASSAP02_22__26.yml")
        ),
        input_ports = None
    )
    (
        resumo_de_cfop_sapxsati__multirowformula_10_row_id_drop_0._out(0)
        >> [resumo_de_cfop_sapxsati__formula_57_0._in(1), resumo_de_cfop_sapxsati__filter_11_reject._in(0)]
    )
    resumo_de_cfop_sapxsati__filter_11_reject >> entradasresumos_61
    entradassapf001_1 >> resumo_de_cfop_sapxsati__multirowformula_10_row_id_drop_0
    saidassap02_22__26 >> resumo_de_cfop_sapxsati__formula_57_0._in(2)
    resumo_de_cfop_sapxsati__formula_57_0 >> resumocfopsapes_60
    resumocfopsati0_41 >> resumo_de_cfop_sapxsati__formula_57_0._in(0)
