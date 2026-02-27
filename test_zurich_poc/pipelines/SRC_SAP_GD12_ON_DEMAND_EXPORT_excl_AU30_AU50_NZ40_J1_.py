from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_",
    version = 1,
    auto_layout = False,
    params = Parameters(
      jdbcUrl_DSN_SnowflakePR_20 = "''",
      username_aka_SnowflakePR_29 = "''",
      username_DSN_SnowflakePR_20 = "''",
      workflow_name = "'SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_'",
      jdbcUrl_aka_SnowflakePR_29 = "''",
      password_aka_SnowflakePR_29 = "''",
      password_DSN_SnowflakePR_20 = "''"
    )
)

with Pipeline(args) as pipeline:
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___sample_73 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73")
    )
    dynamicinput_40 = Process(
        name = "DynamicInput_40",
        properties = DynamicInput(
          xlsxFileIntegration = "sftp",
          tableIntegration = "oracle",
          tableConnector = "transpiled_connection",
          fileType = "fileType_XLSX",
          sqlQuery = "_externals\\1\\check.csv",
          sheetNameColumnName = "Sheet Names",
          header = False,
          fileConnector = "transpiled_connection"
        ),
        is_custom_output_schema = True
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___union_44 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Union_44",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Union_44"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___textinput_70_cast = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___TextInput_70_cast",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___TextInput_70_cast")
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___runcommand_37 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RunCommand_37",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RunCommand_37")
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___regex_68 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RegEx_68",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RegEx_68")
    )
    test_5 = Process(
        name = "Test_5",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "assert ( in0.count()  == 0 )"
        )
    )
    emailrecipients_59 = Process(
        name = "Emailrecipients_59",
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
            filePath = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Post Allocation Reconciliation\\01- Source\\Email recipients.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_/Emailrecipients_59.yml"
          )
        ),
        input_ports = None
    )
    directory_43 = Process(
        name = "Directory_43",
        properties = Directory(
          path = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Post Allocation Reconciliation\\01- Source\\SRC_SAP_GD12_ON_DEMAND_EXPORT_J0",
          pattern = "*.*",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    directory_1 = Process(
        name = "Directory_1",
        properties = Directory(
          path = "\\\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Post Allocation Reconciliation\\01- Source\\SRC_SAP_GD12_ON_DEMAND_EXPORT_J1",
          pattern = "*.*",
          integration = "sftp",
          recursive = True,
          connector = {
            "kind": "sftp",
            "id": "transpiled_connection",
            "properties": {"id" : "transpiled_connection"},
            "type": "connector"
          }
        ),
        input_ports = None
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___alteryxselect_25 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AlteryxSelect_25",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AlteryxSelect_25"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___appendfields_64 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AppendFields_64",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AppendFields_64"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___dsn_snowflakepr_20 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___DSN_SnowflakePR_20",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___DSN_SnowflakePR_20")
    )
    textinput_70 = Process(
        name = "TextInput_70",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_70", sourceType = "Seed")
        ),
        input_ports = None
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___filter_7 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Filter_7",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Filter_7")
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___aka_snowflakepr_29 = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___aka_SnowflakePR_29",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___aka_SnowflakePR_29")
    )
    email_52 = Process(
        name = "Email_52",
        properties = Email(
          body = "Table",
          bodyColumn = "Table",
          subject = "SRC_SAP_GD12_ON_DEMAND_EXPORT workflow ran successfully",
          subjectFromColumn = False,
          showCcBcc = True,
          to = "To_Email_Address",
          toFromColumn = True,
          bodyFromColumn = True,
          toColumn = "To_Email_Address",
          ccFromColumn = True,
          bccFromColumn = False,
          ccColumn = "Cc_Email_Address",
          cc = "Cc_Email_Address",
          subjectColumn = "SRC_SAP_GD12_ON_DEMAND_EXPORT workflow ran successfully",
          connection = "transpiled_connection"
        )
    )
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___join_71_inner = Process(
        name = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Join_71_inner",
        properties = ModelTransform(modelName = "SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Join_71_inner"),
        input_ports = ["in_0", "in_1"]
    )
    textinput_70 >> src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___textinput_70_cast
    (
        src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___alteryxselect_25._out(0)
        >> [src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___appendfields_64._in(2),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___dsn_snowflakepr_20._in(0),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___aka_snowflakepr_29._in(0)]
    )
    dynamicinput_40 >> src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___alteryxselect_25._in(1)
    (
        src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___textinput_70_cast._out(0)
        >> [src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___union_44._in(1),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___join_71_inner._in(1)]
    )
    directory_1 >> src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___regex_68
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___filter_7 >> test_5
    src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___appendfields_64 >> email_52
    (
        src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___union_44._out(0)
        >> [src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___filter_7._in(0),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___sample_73._in(0)]
    )
    (
        emailrecipients_59._out(0)
        >> [src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___appendfields_64._in(0),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___appendfields_64._in(1)]
    )
    (
        src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___regex_68._out(0)
        >> [src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___union_44._in(0),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___join_71_inner._in(0)]
    )
    directory_43 >> src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___union_44._in(2)
    (
        src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___sample_73._out(0)
        >> [dynamicinput_40._in(0), src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___alteryxselect_25._in(0),
              src_sap_gd12_on_demand_export_excl_au30_au50_nz40_j1___runcommand_37._in(0)]
    )
