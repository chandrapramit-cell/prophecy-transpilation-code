from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "productos_part1",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_Server_UYDB_1043 = "''",
      password_aka_Server_UYDB_1043 = "''",
      username_aka_Server_UYDB_1035 = "''",
      password_aka_Server_UYDB_1035 = "''",
      username_aka_Server_UYDB_1116 = "''",
      password_aka_Server_UYDB_1116 = "''",
      username_aka_Server_UYDB_1045 = "''",
      password_aka_Server_UYDB_1045 = "''",
      username_aka_Server_UYDB_1031 = "''",
      password_aka_Server_UYDB_1031 = "''",
      username_aka_Server_UYDB_1030 = "''",
      password_aka_Server_UYDB_1030 = "''",
      username_aka_Server_UYDB_1036 = "''",
      password_aka_Server_UYDB_1036 = "''",
      username_aka_Server_UYDB_1128 = "''",
      password_aka_Server_UYDB_1128 = "''",
      username_aka_Server_UYDB_1038 = "''",
      password_aka_Server_UYDB_1038 = "''",
      username_aka_Server_UYDB_1037 = "''",
      password_aka_Server_UYDB_1037 = "''",
      username_aka_Server_UYDB_1032 = "''",
      password_aka_Server_UYDB_1032 = "''",
      username_aka_Server_UYDB_1041 = "''",
      password_aka_Server_UYDB_1041 = "''",
      username_aka_Server_UYDB_1042 = "''",
      password_aka_Server_UYDB_1042 = "''",
      username_aka_Server_UYDB_1034 = "''",
      password_aka_Server_UYDB_1034 = "''",
      username_aka_Server_UYDB_1046 = "''",
      password_aka_Server_UYDB_1046 = "''",
      username_aka_Server_UYDB_1044 = "''",
      password_aka_Server_UYDB_1044 = "''",
      username_aka_Server_UYDB_1047 = "''",
      password_aka_Server_UYDB_1047 = "''",
      username_aka_Server_UYDB_1048 = "''",
      password_aka_Server_UYDB_1048 = "''",
      username_aka_Server_UYDB_1033 = "''",
      password_aka_Server_UYDB_1033 = "''",
      username_aka_Server_UYDB_1039 = "''",
      password_aka_Server_UYDB_1039 = "''",
      username_aka_Server_UYDB_1040 = "''",
      password_aka_Server_UYDB_1040 = "''",
      jdbcUrl_aka_Server_UYDB_900 = "''",
      username_aka_Server_UYDB_900 = "''",
      password_aka_Server_UYDB_900 = "''",
      jdbcUrl_aka_Server_UYDB_898 = "''",
      username_aka_Server_UYDB_898 = "''",
      password_aka_Server_UYDB_898 = "''",
      workflow_name = "'productos'"
    )
)

with Pipeline(args) as pipeline:

    with visual_group("CODIGUERAS"):
        pass

    with visual_group("CONTRATOS"):
        pass

    with visual_group("CalculoProductosMDP"):

        with visual_group("CalculoTCCodigoProducto1436373839"):
            pass

        with visual_group("CalculoTDCodigoProducto15"):
            pass

        with visual_group("CalculoTrilogyCodigoProducto1"):
            pass

        with visual_group("TarjetasCorporativasCodigoProducto21"):
            pass

    with visual_group("CobrodeSueldosProd9"):

        with visual_group("Estoesparaponertodoslosmesesaunqueesteen0"):
            pass

        with visual_group("PagodeNominasProd18"):

            with visual_group("Estoesparaponertodoslosmesesaunqueesteen0"):
                pass

            with visual_group("Estoesparaponertodoslosmesesaunqueesteen0"):
                pass

    with visual_group("INVENTARIOS"):
        pass

    with visual_group("PagodeImpuestosProd33"):

        with visual_group("Estoesparaponertodoslosmesesaunqueesteen0"):
            pass

        with visual_group("GetnetProd34"):
            pass

    with visual_group("TPVProd19"):
        with visual_group("Estoesparaponertodoslosmesesaunqueesteen0"):
            pass

    aka_server_uydb_1030 = Process(
        name = "aka_Server_UYDB_1030",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1030}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1030"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[idf_pers_ods_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[es_titular],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[codigo_producto],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[codigo_subproducto],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[numero_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[condicion_economica],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[bloqueo_soft_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[bloqueo_duro_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[codigo_bloqueo_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[motivo_baja_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[fecha_alta_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[fecha_baja_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[tipo_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[bloqueo_soft_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[bloqueo_duro_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[limite_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[tarjeta_transaccional_mes],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[cantidad_compras_totales],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[idf_pers_ods_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_diario].[pan_hash] \nfrom [CORE].[dbo].[PRODUCTOS_mdp_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1030.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1031 = Process(
        name = "aka_Server_UYDB_1031",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1031}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1031"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[idf_pers_ods_tarjeta],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[numero_contrato],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[cantidad_compras_totales],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[AAAAMM],\n\t[CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[pan_hash] \nfrom [CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses] \nwhere [CORE].[dbo].[PRODUCTOS_mdp_inventario_25meses].[AAAAMM] >= Format(DateAdd(MONTH, -3, GetDate()), 'yyyyMM')"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1031.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1032 = Process(
        name = "aka_Server_UYDB_1032",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1032}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1032"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select CORE.dbo.PRODUCTOS_inversiones_inventario_diario.idf_pers_ods,\n\tCORE.dbo.PRODUCTOS_inversiones_inventario_diario.codigo_valor \nfrom CORE.dbo.PRODUCTOS_inversiones_inventario_diario"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1032.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1033 = Process(
        name = "aka_Server_UYDB_1033",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1033}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1033"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select CORE.dbo.PRODUCTOS_inversiones_valores.nombre_especie_valor,\n\tCORE.dbo.PRODUCTOS_inversiones_valores.nombre_tipo_valor,\n\tCORE.dbo.PRODUCTOS_inversiones_valores.codigo_valor \nfrom CORE.dbo.PRODUCTOS_inversiones_valores"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1033.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1034 = Process(
        name = "aka_Server_UYDB_1034",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1034}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1034"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[fecha_alta],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[grupo_producto],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[tipo_producto],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[sub_tipo_producto],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[codigo_plan],\n\t[CORE].[dbo].[PRODUCTOS_seguros_inventario_diario].[cantidad_empleados] \nfrom [CORE].[dbo].[PRODUCTOS_seguros_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1034.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1035 = Process(
        name = "aka_Server_UYDB_1035",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1035}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1035"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[tipo_servicio],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[servicio],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[fecha_alta],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[esta_activo],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[cargo_cuenta_dolares],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[cargo_cuenta_pesos],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[nombre_proveedor],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[numero_referencia],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[importe_abonado_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario].[cantidad_de_pagos] \nfrom [CORE].[dbo].[PRODUCTOS_debitos_automaticos_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1035.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1036 = Process(
        name = "aka_Server_UYDB_1036",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1036}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1036"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[AAAAMM],\n\t[CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[idf_pers_ods_pagador],\n\t[CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[idf_pers_ods_cobrador],\n\t[CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[importe_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[banco_cobrador],\n\t[CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico].[cuenta_cobrador] \nfrom [CORE].[dbo].[PRODUCTOS_nominas_movimientos_historico]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1036.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1037 = Process(
        name = "aka_Server_UYDB_1037",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1037}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1037"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[tipo_transaccion],\n\t[CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[AAAAMM],\n\t[CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[monto_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[pan_hash] \nfrom [CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad] \nwhere [CORE].[dbo].[PRODUCTOS_mdp_cuentas_transaccionalidad].[AAAAMM] = Format(DateAdd(MONTH, -1, GetDate()), 'yyyyMM')"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1037.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1038 = Process(
        name = "aka_Server_UYDB_1038",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1038}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1038"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_pago_proveedores_movimientos_historico].[idf_pers_ods_pagador],\n\t[CORE].[dbo].[PRODUCTOS_pago_proveedores_movimientos_historico].[idf_pers_ods_cobrador],\n\t[CORE].[dbo].[PRODUCTOS_pago_proveedores_movimientos_historico].[importe_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_pago_proveedores_movimientos_historico].[AAAAMM] \nfrom [CORE].[dbo].[PRODUCTOS_pago_proveedores_movimientos_historico]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1038.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1039 = Process(
        name = "aka_Server_UYDB_1039",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1039}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1039"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_descuento_documentos_inventario_diario].[idf_pers_ods] \nfrom [CORE].[dbo].[PRODUCTOS_descuento_documentos_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1039.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1040 = Process(
        name = "aka_Server_UYDB_1040",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1040}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1040"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_cuentas_inventario_diario].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_cuentas_inventario_diario].[motivo_cancelacion],\n\t[CORE].[dbo].[PRODUCTOS_cuentas_inventario_diario].[fecha_cancelacion],\n\t[CORE].[dbo].[PRODUCTOS_cuentas_inventario_diario].[fecha_cierre] \nfrom [CORE].[dbo].[PRODUCTOS_cuentas_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1040.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1041 = Process(
        name = "aka_Server_UYDB_1041",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1041}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1041"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_tpv_transacciones_25meses].[AAAAMM],\n\t[CORE].[dbo].[PRODUCTOS_tpv_transacciones_25meses].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_tpv_transacciones_25meses].[merchant],\n\t[CORE].[dbo].[PRODUCTOS_tpv_transacciones_25meses].[monto_arbitrado_dolares] \nfrom [CORE].[dbo].[PRODUCTOS_tpv_transacciones_25meses]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1041.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1042 = Process(
        name = "aka_Server_UYDB_1042",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1042}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1042"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_valores_al_cobro_inventario_diario].[idf_pers_ods],\n\t[CORE].[dbo].[PRODUCTOS_valores_al_cobro_inventario_diario].[saldo_arbitrado_dolares] \nfrom [CORE].[dbo].[PRODUCTOS_valores_al_cobro_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1042.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1043 = Process(
        name = "aka_Server_UYDB_1043",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1043}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1043"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[idf_pers_ods],\n\t[core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[AAAAMM],\n\t[core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[datos_adicionales],\n\t[core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[monto_arbitrado_dolares] \nfrom [core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses] \nwhere [core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[codigo_tipo_transaccion_cuenta] = '0034' \n\tand ([core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[datos_adicionales] Like '%PRIFSUUR_BPS       APJ PAGO%' or [core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[datos_adicionales] Like '%PRIFSUUR_DGI       IMP PAGO%' or [core].[dbo].[PRODUCTOS_cuentas_transacciones_13meses].[datos_adicionales] Like '%PRIFSUUR_DGI       IMP2PAGO%')"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1043.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1044 = Process(
        name = "aka_Server_UYDB_1044",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1044}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1044"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select * \nfrom [CORE].[dbo].[PRODUCTOS_sobregiros_inventario_diario]")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1044.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1045 = Process(
        name = "aka_Server_UYDB_1045",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1045}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1045"),
            warehouseQuery = OracleSource.WarehouseQuery(query = "select * \nfrom [RESTRINGIDO].[dbo].[PRODUCTOS_mdp_equivalencia_pan]")
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1045.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1046 = Process(
        name = "aka_Server_UYDB_1046",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1046}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1046"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [core].[dbo].[PRODUCTOS_getnet_inventario_diario].[documento_comercio],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[banco_cuenta],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[fecha_alta],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[fecha_activacion],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[fecha_primera_transaccion],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[fecha_ultima_transaccion],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[monto_transacciones_mes_actual],\n\t[core].[dbo].[PRODUCTOS_getnet_inventario_diario].[idf_pers_ods] \nfrom [core].[dbo].[PRODUCTOS_getnet_inventario_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1046.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1047 = Process(
        name = "aka_Server_UYDB_1047",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1047}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1047"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[numero_garantia],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[descripcion_corta],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[fecha_alta],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[fecha_vencimiento],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[descripcion_estado],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[descripcion_subestado],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[limite_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[aplicado_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[disponible_arbitrado_dolares],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[idf_pers_ods_garantizante],\n\t[CORE].[dbo].[PRODUCTOS_garantias_inventario_mes].[activa] \nfrom [CORE].[dbo].[PRODUCTOS_garantias_inventario_mes]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1047.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1048 = Process(
        name = "aka_Server_UYDB_1048",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1048}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1048"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[numero_garantia],\n\t[CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[codigo_relacion],\n\t[CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[numero_persona],\n\t[CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[numero_secuencial_cliente],\n\t[CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[AAAAMM],\n\t[CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes].[idf_pers_ods_garantizado] \nfrom [CORE].[dbo].[PRODUCTOS_garantias_garantizado_mes]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1048.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1116 = Process(
        name = "aka_Server_UYDB_1116",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1116}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1116"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [DATA].[dbo].[CODIGUERA_productos_comerciales].[codigo_producto],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[codigo_subproducto],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[familia_producto_comercial],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[grupo_producto_comercial],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[tipo_producto_comercial],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[sub_tipo_producto_comercial],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[descripcion_crm],\n\t[DATA].[dbo].[CODIGUERA_productos_comerciales].[codigo_destino] \nfrom [DATA].[dbo].[CODIGUERA_productos_comerciales]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1116.yml")
        ),
        input_ports = None
    )
    aka_server_uydb_1128 = Process(
        name = "aka_Server_UYDB_1128",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:63efd8ff70376dbd726b6ce6",
              "username": "${username_aka_Server_UYDB_1128}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_Server_UYDB_1128"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select [CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[codigo_producto],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[codigo_destino],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[idf_pers_ods],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[rubro_cuenta_contable],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[subrubro_cuenta_contable],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[saldo_arbitrado_dolares],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[divisa],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[sector_cuenta_contable],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[capitulo_cuenta_contable],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[numero_contrato],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[codigo_subproducto],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[saldo_moneda_origen],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[perimetro_gestion],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[codigo_agrupacion_contable],\n\t[CORE].[dbo].[CONTRATOS_inventario_clientes_diario].[descripcion_producto] \nfrom [CORE].[dbo].[CONTRATOS_inventario_clientes_diario]"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/productos_part1/aka_Server_UYDB_1128.yml")
        ),
        input_ports = None
    )
    intermediate_productos_aka_server_uydb_1032 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1032",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1032",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1033 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1033",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1033",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1034 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1034",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1034",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1035 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1035",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1035",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1037 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1037",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1037",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1038 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1038",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1038",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1039 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1039",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1039",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1040 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1040",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1040",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1042 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1042",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1042",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1044 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1044",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1044",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1045 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1045",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1045",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1047 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1047",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1047",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1048 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1048",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1048",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1116 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1116",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1116",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    intermediate_productos_aka_server_uydb_1128 = Process(
        name = "intermediate_productos_aka_Server_UYDB_1128",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "intermediate_productos_aka_Server_UYDB_1128",
            sourceType = "Table",
            sourceName = "transpiled_sources"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        )
    )
    productos_part1__filter_989 = Process(
        name = "productos_part1__Filter_989",
        properties = ModelTransform(modelName = "productos_part1__Filter_989")
    )
    productos_part1__formula_990_0 = Process(
        name = "productos_part1__Formula_990_0",
        properties = ModelTransform(modelName = "productos_part1__Formula_990_0")
    )
    productos_part1__formula_996_0 = Process(
        name = "productos_part1__Formula_996_0",
        properties = ModelTransform(modelName = "productos_part1__Formula_996_0")
    )
    productos_part1__join_988_inner = Process(
        name = "productos_part1__Join_988_inner",
        properties = ModelTransform(modelName = "productos_part1__Join_988_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    productos_part1__intermediate_productos_alteryxselect_1012 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1012",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1012"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    productos_part1__intermediate_productos_alteryxselect_1025 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1025",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1025")
    )
    productos_part1__intermediate_productos_alteryxselect_1098 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1098",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1098"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    productos_part1__intermediate_productos_alteryxselect_1210 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1210",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1210")
    )
    productos_part1__intermediate_productos_alteryxselect_1241 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1241",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1241")
    )
    productos_part1__intermediate_productos_alteryxselect_1288 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_1288",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_1288"),
        input_ports = ["in_0", "in_1"]
    )
    productos_part1__intermediate_productos_alteryxselect_991 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_991",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_991")
    )
    productos_part1__intermediate_productos_alteryxselect_994 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_994",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_994")
    )
    productos_part1__intermediate_productos_alteryxselect_997 = Process(
        name = "productos_part1__intermediate_productos_AlteryxSelect_997",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_AlteryxSelect_997")
    )
    productos_part1__intermediate_productos_formula_1161 = Process(
        name = "productos_part1__intermediate_productos_Formula_1161",
        properties = ModelTransform(modelName = "productos_part1__intermediate_productos_Formula_1161"),
        input_ports = ["in_0", "in_1"]
    )
    aka_server_uydb_1032 >> intermediate_productos_aka_server_uydb_1032
    aka_server_uydb_1044 >> intermediate_productos_aka_server_uydb_1044
    aka_server_uydb_1042 >> intermediate_productos_aka_server_uydb_1042
    aka_server_uydb_1039 >> intermediate_productos_aka_server_uydb_1039
    aka_server_uydb_1047 >> intermediate_productos_aka_server_uydb_1047
    (
        aka_server_uydb_1041._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_1098._in(0),
              productos_part1__intermediate_productos_alteryxselect_1098._in(1),
              productos_part1__intermediate_productos_alteryxselect_1098._in(2)]
    )
    aka_server_uydb_1046 >> productos_part1__intermediate_productos_alteryxselect_1241
    (
        productos_part1__filter_989._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_994._in(0), productos_part1__formula_990_0._in(0)]
    )
    (
        productos_part1__join_988_inner._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_1012._in(0),
              productos_part1__intermediate_productos_alteryxselect_1012._in(1),
              productos_part1__intermediate_productos_alteryxselect_1012._in(2),
              productos_part1__intermediate_productos_alteryxselect_1025._in(0),
              productos_part1__formula_996_0._in(0), productos_part1__filter_989._in(0)]
    )
    aka_server_uydb_1043 >> productos_part1__intermediate_productos_alteryxselect_1210
    (
        productos_part1__formula_996_0._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_1012._in(3),
              productos_part1__intermediate_productos_alteryxselect_997._in(0)]
    )
    aka_server_uydb_1031 >> productos_part1__join_988_inner._in(3)
    aka_server_uydb_1034 >> intermediate_productos_aka_server_uydb_1034
    aka_server_uydb_1040 >> intermediate_productos_aka_server_uydb_1040
    aka_server_uydb_1030 >> productos_part1__join_988_inner._in(0)
    aka_server_uydb_1038 >> intermediate_productos_aka_server_uydb_1038
    (
        aka_server_uydb_1045._out(0)
        >> [intermediate_productos_aka_server_uydb_1045._in(0), productos_part1__join_988_inner._in(1)]
    )
    aka_server_uydb_1037 >> intermediate_productos_aka_server_uydb_1037
    aka_server_uydb_1128 >> intermediate_productos_aka_server_uydb_1128
    (
        aka_server_uydb_1116._out(0)
        >> [intermediate_productos_aka_server_uydb_1116._in(0), productos_part1__join_988_inner._in(2)]
    )
    aka_server_uydb_1033 >> intermediate_productos_aka_server_uydb_1033
    aka_server_uydb_1035 >> intermediate_productos_aka_server_uydb_1035
    aka_server_uydb_1048 >> intermediate_productos_aka_server_uydb_1048
    (
        aka_server_uydb_1036._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_1288._in(0),
              productos_part1__intermediate_productos_alteryxselect_1288._in(1),
              productos_part1__intermediate_productos_formula_1161._in(0),
              productos_part1__intermediate_productos_formula_1161._in(1)]
    )
    (
        productos_part1__formula_990_0._out(0)
        >> [productos_part1__intermediate_productos_alteryxselect_1012._in(4),
              productos_part1__intermediate_productos_alteryxselect_991._in(0)]
    )
