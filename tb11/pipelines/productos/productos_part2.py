from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "productos_part2",
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

    with visual_group("AcuerdodeSobregiro"):
        pass

    with visual_group("AvalesProd29"):
        pass

    with visual_group("ComprasconTCCodigoProducto4yComprasconTDCodigoProducto5"):
        pass

    with visual_group("CuentasActivasProd26"):
        pass

    with visual_group("DebitosAutomaticosServiciosenCuentayTCProd3"):
        pass

    with visual_group("DescuentodeChequesProd23"):
        pass

    with visual_group("DomiciliacionTCCodigoProducto2"):
        pass

    with visual_group("FondosdeInversionProd8"):
        pass

    with visual_group("GarantiasProd35"):
        pass

    with visual_group("InversionesProd7"):
        pass

    with visual_group("Outputs"):
        pass

    with visual_group("PagoProveedoresProd22"):
        pass

    with visual_group("Prestamos"):

        with visual_group("CreditoGTMProd25"):
            pass

        with visual_group("CreditosComercialesCodigoProducto32"):
            pass

        with visual_group("LeasingCodigoProducto28"):
            pass

        with visual_group("PrestamoCocheProd12"):
            pass

        with visual_group("PrestamoConsumoCodigoProducto17"):
            pass

        with visual_group("PrestamoHipotecarioProd13"):
            pass

    with visual_group("Seguros"):
        pass

    with visual_group("ValoresalCobro"):
        pass

    productos_part2__union_895 = Process(
        name = "productos_part2__Union_895",
        properties = ModelTransform(modelName = "productos_part2__Union_895"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12",
         "in_13", "in_14", "in_15", "in_16", "in_17", "in_18", "in_19", "in_20", "in_21", "in_22", "in_23",
         "in_24", "in_25", "in_26", "in_27", "in_28", "in_29"]
    )
    productos_part2__aka_server_uydb_898 = Process(
        name = "productos_part2__aka_Server_UYDB_898",
        properties = ModelTransform(modelName = "productos_part2__aka_Server_UYDB_898")
    )
    productos_part2__aka_server_uydb_900 = Process(
        name = "productos_part2__aka_Server_UYDB_900",
        properties = ModelTransform(modelName = "productos_part2__aka_Server_UYDB_900")
    )
    (
        productos_part2__union_895._out(0)
        >> [productos_part2__aka_server_uydb_900._in(0), productos_part2__aka_server_uydb_898._in(0)]
    )
