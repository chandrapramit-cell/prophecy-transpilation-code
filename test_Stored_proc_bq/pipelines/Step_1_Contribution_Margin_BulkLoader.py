from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Step_1_Contribution_Margin_BulkLoader",
    version = 1,
    auto_layout = False,
    params = Parameters(
      password_Source_File_C___24 = "''",
      username_Source_File_C___24 = "''",
      workflow_name = "'Step_1_Contribution_Margin_BulkLoader'",
      password_Source_File_C___15 = "''",
      username_Source_File_C___175 = "''",
      password_Source_File_C___175 = "''",
      username_Source_File_C___15 = "''"
    )
)

with Pipeline(args) as pipeline:
    source_file_c___175 = Process(
        name = "Source_File_C___175",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "jdbc://<PLEASE_EDIT>(File:C:\\Users\\THOMPSOA\\OneDrive - MSC Industrial Direct Co., Inc\\Tasks\\In Progress\\AlteryxServerConnectionFile\\Alteryx Server from Working.indbc)",
              "username": "${username_Source_File_C___175}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Source_File_C___175"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "\nSELECT mscfiscalyearmonthdss,mscfiscalyearmonth,MSCFiscalYear,MSCFiscalMonth FROM `da-edw-prd-mscdirect.prepared_view.date_dim_vw` \nGROUP BY 1,2,3,4"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Step_1_Contribution_Margin_BulkLoader/Source_File_C___175.yml"
          )
        ),
        input_ports = None
    )
    source_file_c___15 = Process(
        name = "Source_File_C___15",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "jdbc://<PLEASE_EDIT>(File:C:\\Users\\THOMPSOA\\OneDrive - MSC Industrial Direct Co., Inc\\Tasks\\In Progress\\AlteryxServerConnectionFile\\Alteryx Server from Working.indbc)",
              "username": "${username_Source_File_C___15}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Source_File_C___15"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT h.hierarchycode,h.hierarchycodedesc,z.hierarchycode,z.hierarchycodedesc,reportingchannel FROM `da-edw-prd-mscdirect.prepared_view.salesrep_dim_vw` s \nINNER JOIN `da-edw-prd-mscdirect.prepared_view.vw_customerreportingchannel` r on s.salesrepbkey = r.salesrepbkey \nINNER JOIN `da-edw-prd-mscdirect.prepared_view.salesterritoryhierarchy_dim_vw` h on s.salesterritoryhierarchybkey = h.salesterritoryhierarchybkey and h.hierarchylevel = 1 and h.activeind = 'Y'\nINNER JOIN `da-edw-prd-mscdirect.prepared_view.salesterritoryhierarchy_dim_vw` z on h.parenthierarchycodebkey = z.salesterritoryhierarchybkey and z.hierarchylevel = 2 and z.activeind = 'Y'\nWHERE s.activeind = 'Y'\nGROUP BY 1,2,3,4,5\nORDER BY 1,2,3,4,5\n"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Step_1_Contribution_Margin_BulkLoader/Source_File_C___15.yml"
          )
        ),
        input_ports = None
    )
    source_file_c___24 = Process(
        name = "Source_File_C___24",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "jdbc://<PLEASE_EDIT>(File:C:\\Users\\THOMPSOA\\OneDrive - MSC Industrial Direct Co., Inc\\Tasks\\In Progress\\AlteryxServerConnectionFile\\Alteryx Server from Working.indbc)",
              "username": "${username_Source_File_C___24}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Source_File_C___24"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT ccsgsalesterritorycode,districtcode FROM `da-edw-prd-mscdirect.prepared_view.ccsgsalesterritoryhierarchyxref_dim_vw` "
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Step_1_Contribution_Margin_BulkLoader/Source_File_C___24.yml"
          )
        ),
        input_ports = None
    )
    step_1_contribution_margin_bulkloader__summarize_43 = Process(
        name = "Step_1_Contribution_Margin_BulkLoader__Summarize_43",
        properties = ModelTransform(modelName = "Step_1_Contribution_Margin_BulkLoader__Summarize_43")
    )
    step_1_contribution_margin_bulkloader__join_26_inner = Process(
        name = "Step_1_Contribution_Margin_BulkLoader__Join_26_inner",
        properties = ModelTransform(modelName = "Step_1_Contribution_Margin_BulkLoader__Join_26_inner"),
        input_ports = ["in_0", "in_1"]
    )
    (
        source_file_c___15._out(0)
        >> [step_1_contribution_margin_bulkloader__join_26_inner._in(0),
              step_1_contribution_margin_bulkloader__summarize_43._in(0)]
    )
    source_file_c___24 >> step_1_contribution_margin_bulkloader__join_26_inner._in(1)
