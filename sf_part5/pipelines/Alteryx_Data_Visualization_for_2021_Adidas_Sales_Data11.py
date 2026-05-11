from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11",
    version = 1,
    auto_layout = False,
    params = Parameters(
      USERNAME_ADIDASUSSALESDA_1 = "''",
      PASSWORD_ADIDASUSSALESDA_1 = "''",
      WORKFLOW_NAME = "'Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11'"
    )
)

with Pipeline(args) as pipeline:
    adidasussalesda_1 = Process(
        name = "AdidasUSSalesDa_1",
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
          properties = SFTPSource.SFTPSourceInternal(filePath = "Adidas US Sales Datasets.xlsx"),
          format = SFTPSource.XLSXReadFormat(
            schema = "external_sources/Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11/AdidasUSSalesDa_1.yml"
          )
        ),
        input_ports = None
    )
    alteryx_data_visualization_for_2021_adidas_sales_data11__portfoliocomposerrender_81 = Process(
        name = "Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11__PortfolioComposerRender_81",
        properties = ModelTransform(
          modelName = "Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11__PortfolioComposerRender_81"
        )
    )
    adidasussalesda_1 >> alteryx_data_visualization_for_2021_adidas_sales_data11__portfoliocomposerrender_81
