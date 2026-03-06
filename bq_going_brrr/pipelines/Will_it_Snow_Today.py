from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Will_it_Snow_Today",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_produkt_klima_t_4 = "''",
      password_produkt_klima_t_4 = "''",
      workflow_name = "'Will_it_Snow_Today'"
    )
)

with Pipeline(args) as pipeline:
    will_it_snow_today__filter_23 = Process(
        name = "Will_it_Snow_Today__Filter_23",
        properties = ModelTransform(modelName = "Will_it_Snow_Today__Filter_23")
    )
    produkt_klima_t_4 = Process(
        name = "produkt_klima_t_4",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "C:\\Users\\markoliver.maeder\\OneDrive - alteryx.com\\Documents\\OnBoarding & Training\\Wetter Beispiel\\Munchen Stadt\\produkt_klima_tag_19540601_20201231_03379.txt",
              "username": "${username_produkt_klima_t_4}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "produkt_klima_t_4"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "C:\\Users\\markoliver.maeder\\OneDrive - alteryx.com\\Documents\\OnBoarding & Training\\Wetter Beispiel\\Munchen Stadt\\produkt_klima_tag_19540601_20201231_03379.txt"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/Will_it_Snow_Today/produkt_klima_t_4.yml")
        ),
        input_ports = None
    )
    will_it_snow_today__alteryxselect_15 = Process(
        name = "Will_it_Snow_Today__AlteryxSelect_15",
        properties = ModelTransform(modelName = "Will_it_Snow_Today__AlteryxSelect_15")
    )
    will_it_snow_today__filter_23_reject = Process(
        name = "Will_it_Snow_Today__Filter_23_reject",
        properties = ModelTransform(modelName = "Will_it_Snow_Today__Filter_23_reject")
    )
    will_it_snow_today__portfoliocomposertext_29 = Process(
        name = "Will_it_Snow_Today__PortfolioComposerText_29",
        properties = ModelTransform(modelName = "Will_it_Snow_Today__PortfolioComposerText_29")
    )
    produkt_klima_t_4 >> will_it_snow_today__alteryxselect_15
    (
        will_it_snow_today__alteryxselect_15._out(0)
        >> [will_it_snow_today__portfoliocomposertext_29._in(0), will_it_snow_today__filter_23._in(0),
              will_it_snow_today__filter_23_reject._in(0)]
    )
