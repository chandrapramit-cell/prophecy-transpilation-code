from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Explain_ELA_with_checks",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_ELACalibration2_20 = "''",
      password_ELACalibration2_20 = "''",
      username_Counterparty_CV_28 = "''",
      password_Counterparty_CV_28 = "''",
      username_Counterparty_CV_29 = "''",
      password_Counterparty_CV_29 = "''",
      username_ELA_results_202_16 = "''",
      password_ELA_results_202_16 = "''",
      jdbcUrl_RatesFOMoMmove__78 = "''",
      username_RatesFOMoMmove__78 = "''",
      password_RatesFOMoMmove__78 = "''",
      jdbcUrl_RatesMoMmove_xl_68 = "''",
      username_RatesMoMmove_xl_68 = "''",
      password_RatesMoMmove_xl_68 = "''",
      jdbcUrl_DbFileOutput_87_87 = "''",
      username_DbFileOutput_87_87 = "''",
      password_DbFileOutput_87_87 = "''",
      jdbcUrl_CEMMoMmove_xlsx_36 = "''",
      username_CEMMoMmove_xlsx_36 = "''",
      password_CEMMoMmove_xlsx_36 = "''",
      jdbcUrl_CommodsMoMmove__69 = "''",
      username_CommodsMoMmove__69 = "''",
      password_CommodsMoMmove__69 = "''",
      workflow_name = "'Explain_ELA_with_checks'"
    )
)

with Pipeline(args) as pipeline:
    cemmommove_xlsx_36 = Process(
        name = "CEMMoMmove_xlsx_36",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\CEM MoM move.xlsx"
          )
        )
    )
    commodsmommove__69 = Process(
        name = "CommodsMoMmove__69",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\Commods MoM move.xlsx"
          )
        )
    )
    counterparty_cv_28 = Process(
        name = "Counterparty_CV_28",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\Counterparty_CVA_and_Exposures_FebME.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Explain_ELA_with_checks/Counterparty_CV_28.yml",
            sheetName = "Counterparty CVA and Exposures",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    counterparty_cv_29 = Process(
        name = "Counterparty_CV_29",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202601\\ELA\\Counterparty_CVA_and_Exposures_JanME.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Explain_ELA_with_checks/Counterparty_CV_29.yml",
            sheetName = "Counterparty CVA and Exposures",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    dbfileoutput_87_87 = Process(
        name = "DbFileOutput_87_87",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202601\\ELA\\CP CVA Check.xlsx"
          )
        )
    )
    elacalibration2_20 = Process(
        name = "ELACalibration2_20",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202601\\ELA\\ELA Calibration 20260130.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Explain_ELA_with_checks/ELACalibration2_20.yml",
            sheetName = "Sheet1",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    ela_results_202_16 = Process(
        name = "ELA_results_202_16",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\ELA_results_20260227.xlsx"
          ),
          format = DatabricksVolumeSource.XLSXReadFormat(
            ignoreCellFormatting = True,
            allNullRowsPeekLimit = 10000,
            schema = "external_sources/Explain_ELA_with_checks/ELA_results_202_16.yml",
            sheetName = "Sheet1",
            allowAllNullRows = True,
            defaultColumnPrefix = "F"
          )
        ),
        input_ports = None
    )
    explain_ela_with_checks__alteryxselect_62 = Process(
        name = "Explain_ELA_with_checks__AlteryxSelect_62",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__AlteryxSelect_62")
    )
    explain_ela_with_checks__alteryxselect_73 = Process(
        name = "Explain_ELA_with_checks__AlteryxSelect_73",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__AlteryxSelect_73")
    )
    explain_ela_with_checks__alteryxselect_74 = Process(
        name = "Explain_ELA_with_checks__AlteryxSelect_74",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__AlteryxSelect_74")
    )
    explain_ela_with_checks__alteryxselect_81 = Process(
        name = "Explain_ELA_with_checks__AlteryxSelect_81",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__AlteryxSelect_81")
    )
    explain_ela_with_checks__alteryxselect_88 = Process(
        name = "Explain_ELA_with_checks__AlteryxSelect_88",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__AlteryxSelect_88")
    )
    explain_ela_with_checks__formula_60_2 = Process(
        name = "Explain_ELA_with_checks__Formula_60_2",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__Formula_60_2")
    )
    explain_ela_with_checks__join_59_inner = Process(
        name = "Explain_ELA_with_checks__Join_59_inner",
        properties = ModelTransform(modelName = "Explain_ELA_with_checks__Join_59_inner"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6"]
    )
    ratesfomommove__78 = Process(
        name = "RatesFOMoMmove__78",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\Rates FO MoM move.xlsx"
          )
        )
    )
    ratesmommove_xl_68 = Process(
        name = "RatesMoMmove_xl_68",
        properties = SFTPTarget(
          compression = SFTPTarget.Compression(kind = "uncompressed"),
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
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\Rates MoM move.xlsx"
          )
        )
    )
    bd_27_feb_2026__42 = Process(
        name = "bd_27_Feb_2026__42",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\bd_27-Feb-2026__balancesFull_260623146398.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/Explain_ELA_with_checks/bd_27_Feb_2026__42.yml"
          )
        ),
        input_ports = None
    )
    bd_27_feb_2026__53 = Process(
        name = "bd_27_Feb_2026__53",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202602\\ELA\\bd_27-Feb-2026__pnlCalcMtdYtd_260623146399.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/Explain_ELA_with_checks/bd_27_Feb_2026__53.yml"
          )
        ),
        input_ports = None
    )
    bd_30_jan_2026__47 = Process(
        name = "bd_30_Jan_2026__47",
        properties = DatabricksVolumeSource(
          connector = "transpiled_connection",
          properties = DatabricksVolumeSource.DatabricksVolumeSourceInternal(
            filePath = "\\\\Asiapac.ad.jpmorganchase.com\\corp$\\INCORP\\CorpShare02\\VCG BLR\\CPG\\IB Risk\\2026\\202601\\ELA\\bd_30-Jan-2026__balancesFull_260341796580.csv"
          ),
          format = DatabricksVolumeSource.CsvReadFormat(
            schema = "external_sources/Explain_ELA_with_checks/bd_30_Jan_2026__47.yml"
          )
        ),
        input_ports = None
    )
    explain_ela_with_checks__alteryxselect_74 >> commodsmommove__69
    explain_ela_with_checks__alteryxselect_88 >> dbfileoutput_87_87
    counterparty_cv_29 >> explain_ela_with_checks__join_59_inner._in(3)
    bd_27_feb_2026__42 >> explain_ela_with_checks__join_59_inner._in(4)
    bd_30_jan_2026__47 >> explain_ela_with_checks__join_59_inner._in(5)
    bd_27_feb_2026__53 >> explain_ela_with_checks__join_59_inner._in(6)
    explain_ela_with_checks__alteryxselect_81 >> ratesfomommove__78
    (
        explain_ela_with_checks__join_59_inner._out(0)
        >> [explain_ela_with_checks__formula_60_2._in(0), explain_ela_with_checks__alteryxselect_81._in(0)]
    )
    explain_ela_with_checks__alteryxselect_73 >> ratesmommove_xl_68
    counterparty_cv_28 >> explain_ela_with_checks__join_59_inner._in(2)
    ela_results_202_16 >> explain_ela_with_checks__join_59_inner._in(0)
    (
        explain_ela_with_checks__formula_60_2._out(0)
        >> [explain_ela_with_checks__alteryxselect_62._in(0), explain_ela_with_checks__alteryxselect_73._in(0),
              explain_ela_with_checks__alteryxselect_74._in(0),
              explain_ela_with_checks__alteryxselect_88._in(0)]
    )
    explain_ela_with_checks__alteryxselect_62 >> cemmommove_xlsx_36
    elacalibration2_20 >> explain_ela_with_checks__join_59_inner._in(1)
