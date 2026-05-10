from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "challenge_350_solution",
    version = 1,
    auto_layout = False,
    params = Parameters(
      TO_CARD_RECIPIENT = "'Header'",
      WRITE_YOUR_PERSONAL_MESSAGE_OPTIONAL = "'Personal message'",
      CHOOSE_YOUR_CARD_BACKGROUND = "''",
      SIGN_THE_CARD = "'Sign'",
      CHOOSE_YOUR_MESSAGE_REQUIRED = "''",
      VARIABLE32_FORMULAFIELDS_FORMULAFIELDFIELDPERSONALMESSAGE_EXPRESSION = Expr("{{ var('Write_your_personal_message_Optional') }}"),
      VARIABLE40_FILENAME = Expr("{{ var('Choose_your_card_background') }}"),
      VARIABLE2_SIMPLE_OPERANDS_OPERAND = Expr("{{ var('Choose_your_message_Required') }}"),
      VARIABLE32_FORMULAFIELDS_FORMULAFIELDFIELDHEADER_EXPRESSION = Expr("${{ var('TO_CARD_RECIPIENT') }}"),
      VARIABLE32_FORMULAFIELDS_FORMULAFIELDFIELDFOOTER_EXPRESSION = Expr("{{ var('SIGN_THE_CARD') }}"),
      USERNAME_TYPE_YXDB_6 = "''",
      PASSWORD_TYPE_YXDB_6 = "''",
      WORKFLOW_NAME = "'challenge_350_solution'",
      QUESTION__TEXT_BOX_30 = "''",
      QUESTION__TEXT_BOX_33 = "''",
      QUESTION__FILE_BROWSE_39 = "''",
      QUESTION__TREE_54 = "''",
      QUESTION__TEXT_BOX_55 = "''"
    )
)

with Pipeline(args) as pipeline:
    textinput_72 = Process(
        name = "TextInput_72",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_72", sourceType = "Seed")
        ),
        input_ports = None
    )
    type_yxdb_6 = Process(
        name = "Type_yxdb_6",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": ".\\Type.yxdb",
              "username": "${username_Type_yxdb_6}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Type_yxdb_6"),
            warehouseQuery = OracleSource.WarehouseQuery(query = ".\\Type.yxdb")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/challenge_350_solution/Type_yxdb_6.yml")
        ),
        input_ports = None
    )
    challenge_350_solution__portfoliocomposerrender_49 = Process(
        name = "challenge_350_solution__PortfolioComposerRender_49",
        properties = ModelTransform(modelName = "challenge_350_solution__PortfolioComposerRender_49")
    )
    challenge_350_solution__textinput_72_cast = Process(
        name = "challenge_350_solution__TextInput_72_cast",
        properties = ModelTransform(modelName = "challenge_350_solution__TextInput_72_cast")
    )
    type_yxdb_6 >> challenge_350_solution__portfoliocomposerrender_49
    textinput_72 >> challenge_350_solution__textinput_72_cast
