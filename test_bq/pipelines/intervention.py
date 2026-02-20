from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "intervention",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'intervention'")
)

with Pipeline(args) as pipeline:
    experianmappedt_1673 = Process(
        name = "experianMappedT_1673",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\experianMappedTransformed.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/experianMappedT_1673.yml")
        ),
        input_ports = None
    )
    intervention__filter_874 = Process(
        name = "intervention__Filter_874",
        properties = ModelTransform(modelName = "intervention__Filter_874")
    )
    intervention__union_817 = Process(
        name = "intervention__Union_817",
        properties = ModelTransform(modelName = "intervention__Union_817"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__summarize_777 = Process(
        name = "intervention__Summarize_777",
        properties = ModelTransform(modelName = "intervention__Summarize_777")
    )
    providerdetailr_1677 = Process(
        name = "ProviderDetailR_1677",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\Provider Detail Report_Live.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/ProviderDetailR_1677.yml")
        ),
        input_ports = None
    )
    intervention__filter_1020_reject = Process(
        name = "intervention__Filter_1020_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1020_reject")
    )
    total_inf_condi_1662 = Process(
        name = "TOTAL_INF_CONDI_1662",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\TOTAL_INF_CONDITIONS.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/TOTAL_INF_CONDI_1662.yml")
        ),
        input_ports = None
    )
    intervention__join_834_left_unionleftouter = Process(
        name = "intervention__Join_834_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_834_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_1656_reject = Process(
        name = "intervention__Filter_1656_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1656_reject")
    )
    intervention__filter_869 = Process(
        name = "intervention__Filter_869",
        properties = ModelTransform(modelName = "intervention__Filter_869")
    )
    intervention__join_850_left_unionleftouter = Process(
        name = "intervention__Join_850_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_850_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    providerdetailr_1661 = Process(
        name = "ProviderDetailR_1661",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\Provider Detail Report_Live.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/ProviderDetailR_1661.yml")
        ),
        input_ports = None
    )
    intervention__filter_1020 = Process(
        name = "intervention__Filter_1020",
        properties = ModelTransform(modelName = "intervention__Filter_1020")
    )
    enrollment_expe_1672 = Process(
        name = "ENROLLMENT_EXPE_1672",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\ENROLLMENT_EXPERIAN_MASTER_Enriched_FINAL.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/ENROLLMENT_EXPE_1672.yml")
        ),
        input_ports = None
    )
    intervention__join_822_left_unionleftouter = Process(
        name = "intervention__Join_822_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_822_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_870 = Process(
        name = "intervention__Filter_870",
        properties = ModelTransform(modelName = "intervention__Filter_870")
    )
    intervention__filter_876_reject = Process(
        name = "intervention__Filter_876_reject",
        properties = ModelTransform(modelName = "intervention__Filter_876_reject")
    )
    intervention__summarize_825 = Process(
        name = "intervention__Summarize_825",
        properties = ModelTransform(modelName = "intervention__Summarize_825")
    )
    intervention__filter_869_reject = Process(
        name = "intervention__Filter_869_reject",
        properties = ModelTransform(modelName = "intervention__Filter_869_reject")
    )
    intervention__filter_1019_reject = Process(
        name = "intervention__Filter_1019_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1019_reject")
    )
    intervention__join_800_left_unionleftouter = Process(
        name = "intervention__Join_800_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_800_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__union_866 = Process(
        name = "intervention__Union_866",
        properties = ModelTransform(modelName = "intervention__Union_866"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__filter_1649_reject = Process(
        name = "intervention__Filter_1649_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1649_reject")
    )
    intervention__filter_1650_reject = Process(
        name = "intervention__Filter_1650_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1650_reject")
    )
    intervention__filter_884 = Process(
        name = "intervention__Filter_884",
        properties = ModelTransform(modelName = "intervention__Filter_884")
    )
    selectdistinct__1663 = Process(
        name = "selectdistinct__1663",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\selectdistinct.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/selectdistinct__1663.yml")
        ),
        input_ports = None
    )
    selectsum_csv_1667 = Process(
        name = "selectsum_csv_1667",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\selectsum.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/selectsum_csv_1667.yml")
        ),
        input_ports = None
    )
    intervention_li_765 = Process(
        name = "INTERVENTION_LI_765",
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
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\OUTPUT/INTERVENTION_LIVE.csv"
          ),
          format = SFTPTarget.CsvWriteFormat()
        )
    )
    intervention__filter_877 = Process(
        name = "intervention__Filter_877",
        properties = ModelTransform(modelName = "intervention__Filter_877")
    )
    total_inf_condi_1674 = Process(
        name = "TOTAL_INF_CONDI_1674",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\TOTAL_INF_CONDITIONS.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/TOTAL_INF_CONDI_1674.yml")
        ),
        input_ports = None
    )
    intervention__join_864_inner = Process(
        name = "intervention__Join_864_inner",
        properties = ModelTransform(modelName = "intervention__Join_864_inner"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_873_reject = Process(
        name = "intervention__Filter_873_reject",
        properties = ModelTransform(modelName = "intervention__Filter_873_reject")
    )
    intervention__join_792_left_unionleftouter = Process(
        name = "intervention__Join_792_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_792_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_816 = Process(
        name = "intervention__Filter_816",
        properties = ModelTransform(modelName = "intervention__Filter_816"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__formula_814_0 = Process(
        name = "intervention__Formula_814_0",
        properties = ModelTransform(modelName = "intervention__Formula_814_0"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__multifieldformula_772 = Process(
        name = "intervention__MultiFieldFormula_772",
        properties = ModelTransform(modelName = "intervention__MultiFieldFormula_772")
    )
    intervention__union_784 = Process(
        name = "intervention__Union_784",
        properties = ModelTransform(modelName = "intervention__Union_784"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__filter_873 = Process(
        name = "intervention__Filter_873",
        properties = ModelTransform(modelName = "intervention__Filter_873")
    )
    intervention__filter_877_reject = Process(
        name = "intervention__Filter_877_reject",
        properties = ModelTransform(modelName = "intervention__Filter_877_reject")
    )
    intervention__summarize_801 = Process(
        name = "intervention__Summarize_801",
        properties = ModelTransform(modelName = "intervention__Summarize_801")
    )
    intervention__alteryxselect_890 = Process(
        name = "intervention__AlteryxSelect_890",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_890"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__join_815_inner = Process(
        name = "intervention__Join_815_inner",
        properties = ModelTransform(modelName = "intervention__Join_815_inner"),
        input_ports = ["in_0", "in_1"]
    )
    intervention__filter_870_reject = Process(
        name = "intervention__Filter_870_reject",
        properties = ModelTransform(modelName = "intervention__Filter_870_reject")
    )
    past_120_rx_cou_1678 = Process(
        name = "Past_120_Rx_cou_1678",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\Past_120_Rx_count.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/Past_120_Rx_cou_1678.yml")
        ),
        input_ports = None
    )
    intervention__summarize_826 = Process(
        name = "intervention__Summarize_826",
        properties = ModelTransform(modelName = "intervention__Summarize_826")
    )
    pcmh_csv_1671 = Process(
        name = "PCMH_csv_1671",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\PCMH.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/PCMH_csv_1671.yml")
        ),
        input_ports = None
    )
    intervention__alteryxselect_860 = Process(
        name = "intervention__AlteryxSelect_860",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_860")
    )
    intervention__unique_1018 = Process(
        name = "intervention__Unique_1018",
        properties = ModelTransform(modelName = "intervention__Unique_1018"),
        input_ports = ["in_0", "in_1", "in_2", "in_3"]
    )
    intervention__summarize_820 = Process(
        name = "intervention__Summarize_820",
        properties = ModelTransform(modelName = "intervention__Summarize_820")
    )
    intervention__filter_876 = Process(
        name = "intervention__Filter_876",
        properties = ModelTransform(modelName = "intervention__Filter_876")
    )
    intervention__filter_883 = Process(
        name = "intervention__Filter_883",
        properties = ModelTransform(modelName = "intervention__Filter_883")
    )
    intervention__filter_1659 = Process(
        name = "intervention__Filter_1659",
        properties = ModelTransform(modelName = "intervention__Filter_1659")
    )
    intervention__summarize_785 = Process(
        name = "intervention__Summarize_785",
        properties = ModelTransform(modelName = "intervention__Summarize_785")
    )
    intervention__filter_878 = Process(
        name = "intervention__Filter_878",
        properties = ModelTransform(modelName = "intervention__Filter_878")
    )
    intervention__join_798_left_unionleftouter = Process(
        name = "intervention__Join_798_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_798_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__summarize_771 = Process(
        name = "intervention__Summarize_771",
        properties = ModelTransform(modelName = "intervention__Summarize_771")
    )
    intervention__filter_872_reject = Process(
        name = "intervention__Filter_872_reject",
        properties = ModelTransform(modelName = "intervention__Filter_872_reject")
    )
    ckd_risk_list_c_1675 = Process(
        name = "CKD_Risk_List_c_1675",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\CKD_Risk_List.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/CKD_Risk_List_c_1675.yml")
        ),
        input_ports = None
    )
    intervention__filter_872 = Process(
        name = "intervention__Filter_872",
        properties = ModelTransform(modelName = "intervention__Filter_872")
    )
    edw_prod_csv_1664 = Process(
        name = "EDW_PROD_csv_1664",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\EDW_PROD.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/EDW_PROD_csv_1664.yml")
        ),
        input_ports = None
    )
    intervention__summarize_819 = Process(
        name = "intervention__Summarize_819",
        properties = ModelTransform(modelName = "intervention__Summarize_819")
    )
    intervention__filter_880_reject = Process(
        name = "intervention__Filter_880_reject",
        properties = ModelTransform(modelName = "intervention__Filter_880_reject")
    )
    intervention__filter_1659_reject = Process(
        name = "intervention__Filter_1659_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1659_reject")
    )
    intervention__alteryxselect_1022 = Process(
        name = "intervention__AlteryxSelect_1022",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_1022")
    )
    intervention__filter_1652 = Process(
        name = "intervention__Filter_1652",
        properties = ModelTransform(modelName = "intervention__Filter_1652")
    )
    intervention__filter_879 = Process(
        name = "intervention__Filter_879",
        properties = ModelTransform(modelName = "intervention__Filter_879")
    )
    vulnerability_s_1670 = Process(
        name = "vulnerability_s_1670",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\vulnerability_score_32_cnty_Infection_ALL_v1.2 - Copy_.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/vulnerability_s_1670.yml")
        ),
        input_ports = None
    )
    hrhcreportlive__1679 = Process(
        name = "HRHCReportLive__1679",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\HRHC Report Live.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/HRHCReportLive__1679.yml")
        ),
        input_ports = None
    )
    intervention__filter_874_reject = Process(
        name = "intervention__Filter_874_reject",
        properties = ModelTransform(modelName = "intervention__Filter_874_reject")
    )
    production_xlsx_1668 = Process(
        name = "Production_xlsx_1668",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\Production.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/intervention/Production_xlsx_1668.yml")
        ),
        input_ports = None
    )
    intervention__filter_880 = Process(
        name = "intervention__Filter_880",
        properties = ModelTransform(modelName = "intervention__Filter_880")
    )
    intervention__filter_1655_reject = Process(
        name = "intervention__Filter_1655_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1655_reject")
    )
    intervention__filter_871_reject = Process(
        name = "intervention__Filter_871_reject",
        properties = ModelTransform(modelName = "intervention__Filter_871_reject")
    )
    intervention__filter_1656 = Process(
        name = "intervention__Filter_1656",
        properties = ModelTransform(modelName = "intervention__Filter_1656")
    )
    intervention__join_776_left_unionfullouter = Process(
        name = "intervention__Join_776_left_UnionFullOuter",
        properties = ModelTransform(modelName = "intervention__Join_776_left_UnionFullOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_875_reject = Process(
        name = "intervention__Filter_875_reject",
        properties = ModelTransform(modelName = "intervention__Filter_875_reject")
    )
    intervention__formula_782_0 = Process(
        name = "intervention__Formula_782_0",
        properties = ModelTransform(modelName = "intervention__Formula_782_0")
    )
    productioned_xl_1669 = Process(
        name = "Productioned_xl_1669",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\Productioned.xlsx"
          ),
          format = SFTPSource.XLSXReadFormat(schema = "external_sources/intervention/Productioned_xl_1669.yml")
        ),
        input_ports = None
    )
    intervention__filter_871 = Process(
        name = "intervention__Filter_871",
        properties = ModelTransform(modelName = "intervention__Filter_871")
    )
    intervention__filter_1650 = Process(
        name = "intervention__Filter_1650",
        properties = ModelTransform(modelName = "intervention__Filter_1650")
    )
    intervention__filter_1019 = Process(
        name = "intervention__Filter_1019",
        properties = ModelTransform(modelName = "intervention__Filter_1019")
    )
    intervention__filter_1652_reject = Process(
        name = "intervention__Filter_1652_reject",
        properties = ModelTransform(modelName = "intervention__Filter_1652_reject")
    )
    intervention__alteryxselect_838 = Process(
        name = "intervention__AlteryxSelect_838",
        properties = ModelTransform(modelName = "intervention__AlteryxSelect_838")
    )
    intervention__formula_1021_0 = Process(
        name = "intervention__Formula_1021_0",
        properties = ModelTransform(modelName = "intervention__Formula_1021_0")
    )
    intervention__summarize_824 = Process(
        name = "intervention__Summarize_824",
        properties = ModelTransform(modelName = "intervention__Summarize_824")
    )
    intervention__filter_875 = Process(
        name = "intervention__Filter_875",
        properties = ModelTransform(modelName = "intervention__Filter_875")
    )
    intervention__summarize_818 = Process(
        name = "intervention__Summarize_818",
        properties = ModelTransform(modelName = "intervention__Summarize_818")
    )
    intervention__filter_878_reject = Process(
        name = "intervention__Filter_878_reject",
        properties = ModelTransform(modelName = "intervention__Filter_878_reject")
    )
    intervention__filter_884_reject = Process(
        name = "intervention__Filter_884_reject",
        properties = ModelTransform(modelName = "intervention__Filter_884_reject")
    )
    intervention__filter_1649 = Process(
        name = "intervention__Filter_1649",
        properties = ModelTransform(modelName = "intervention__Filter_1649")
    )
    intervention__summarize_786 = Process(
        name = "intervention__Summarize_786",
        properties = ModelTransform(modelName = "intervention__Summarize_786")
    )
    total_inf_condi_1676 = Process(
        name = "TOTAL_INF_CONDI_1676",
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
            filePath = "O:\\library\\Alteryx\\Advanced Analytics\\AA011RISING_RISK_CLIENT\\Prophecy\\INPUT\\TOTAL_INF_CONDITIONS.csv"
          ),
          format = SFTPSource.CsvReadFormat(schema = "external_sources/intervention/TOTAL_INF_CONDI_1676.yml")
        ),
        input_ports = None
    )
    intervention__summarize_779 = Process(
        name = "intervention__Summarize_779",
        properties = ModelTransform(modelName = "intervention__Summarize_779")
    )
    intervention__filter_879_reject = Process(
        name = "intervention__Filter_879_reject",
        properties = ModelTransform(modelName = "intervention__Filter_879_reject")
    )
    intervention__filter_883_reject = Process(
        name = "intervention__Filter_883_reject",
        properties = ModelTransform(modelName = "intervention__Filter_883_reject")
    )
    intervention__join_805_left_unionleftouter = Process(
        name = "intervention__Join_805_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "intervention__Join_805_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    intervention__filter_1655 = Process(
        name = "intervention__Filter_1655",
        properties = ModelTransform(modelName = "intervention__Filter_1655")
    )
    (
        intervention__join_805_left_unionleftouter._out(0)
        >> [intervention__summarize_801._in(0), intervention__filter_873._in(0),
              intervention__filter_873_reject._in(0), intervention__join_822_left_unionleftouter._in(1)]
    )
    (
        intervention__alteryxselect_838._out(0)
        >> [intervention__alteryxselect_890._in(2), intervention__filter_878._in(0),
              intervention__filter_878_reject._in(0)]
    )
    (
        intervention__alteryxselect_890._out(0)
        >> [intervention__filter_1020._in(0), intervention__filter_1020_reject._in(0),
              intervention__formula_1021_0._in(0)]
    )
    (
        intervention__join_800_left_unionleftouter._out(0)
        >> [intervention__filter_874._in(0), intervention__filter_874_reject._in(0),
              intervention__join_805_left_unionleftouter._in(2)]
    )
    past_120_rx_cou_1678 >> intervention__alteryxselect_890._in(0)
    (
        intervention__formula_782_0._out(0)
        >> [intervention__union_784._in(2), intervention__union_784._in(3), intervention__summarize_786._in(0)]
    )
    pcmh_csv_1671 >> intervention__join_798_left_unionleftouter._in(0)
    (
        intervention__formula_1021_0._out(0)
        >> [intervention__alteryxselect_1022._in(0), intervention__filter_1659._in(0),
              intervention__filter_1659_reject._in(0)]
    )
    (
        intervention__join_792_left_unionleftouter._out(0)
        >> [intervention__filter_871._in(0), intervention__filter_871_reject._in(0),
              intervention__join_798_left_unionleftouter._in(2)]
    )
    intervention__alteryxselect_1022 >> intervention_li_765
    (
        intervention__join_850_left_unionleftouter._out(0)
        >> [intervention__filter_1649._in(0), intervention__filter_1649_reject._in(0),
              intervention__multifieldformula_772._in(0)]
    )
    providerdetailr_1677 >> intervention__formula_814_0._in(1)
    (
        intervention__union_817._out(0)
        >> [intervention__summarize_826._in(0), intervention__summarize_820._in(0),
              intervention__summarize_819._in(0), intervention__summarize_818._in(0),
              intervention__filter_877._in(0), intervention__filter_877_reject._in(0),
              intervention__filter_1656._in(0), intervention__filter_1656_reject._in(0),
              intervention__alteryxselect_838._in(0)]
    )
    (
        intervention__union_866._out(0)
        >> [intervention__filter_883._in(0), intervention__filter_883_reject._in(0),
              intervention__join_776_left_unionfullouter._in(1)]
    )
    (
        intervention__join_864_inner._out(0)
        >> [intervention__union_866._in(3), intervention__filter_884._in(0), intervention__filter_884_reject._in(0)]
    )
    enrollment_expe_1672 >> intervention__join_800_left_unionleftouter._in(0)
    (
        intervention__multifieldformula_772._out(0)
        >> [intervention__unique_1018._in(2), intervention__summarize_771._in(0)]
    )
    vulnerability_s_1670 >> intervention__join_792_left_unionleftouter._in(0)
    experianmappedt_1673 >> intervention__join_805_left_unionleftouter._in(0)
    total_inf_condi_1674 >> intervention__join_834_left_unionleftouter._in(0)
    productioned_xl_1669 >> intervention__formula_782_0
    (
        ckd_risk_list_c_1675._out(0)
        >> [intervention__join_822_left_unionleftouter._in(0), intervention__summarize_824._in(0)]
    )
    (
        intervention__unique_1018._out(0)
        >> [intervention__filter_1019._in(0), intervention__filter_1019_reject._in(0),
              intervention__filter_1650._in(0), intervention__filter_1650_reject._in(0),
              intervention__union_866._in(1), intervention__join_864_inner._in(1)]
    )
    providerdetailr_1661 >> intervention__join_850_left_unionleftouter._in(2)
    intervention__alteryxselect_860._out(0) >> [intervention__union_866._in(0), intervention__join_864_inner._in(0)]
    (
        intervention__join_822_left_unionleftouter._out(0)
        >> [intervention__summarize_825._in(0), intervention__filter_875._in(0),
              intervention__filter_875_reject._in(0), intervention__join_834_left_unionleftouter._in(1)]
    )
    (
        intervention__join_798_left_unionleftouter._out(0)
        >> [intervention__filter_872._in(0), intervention__filter_872_reject._in(0),
              intervention__join_800_left_unionleftouter._in(2)]
    )
    edw_prod_csv_1664 >> intervention__unique_1018._in(0)
    production_xlsx_1668 >> intervention__join_776_left_unionfullouter._in(0)
    total_inf_condi_1676 >> intervention__formula_814_0._in(0)
    hrhcreportlive__1679 >> intervention__unique_1018._in(1)
    (
        intervention__union_784._out(0)
        >> [intervention__summarize_785._in(0), intervention__filter_870._in(0),
              intervention__filter_870_reject._in(0), intervention__join_792_left_unionleftouter._in(2)]
    )
    (
        intervention__join_815_inner._out(0)
        >> [intervention__union_817._in(1), intervention__filter_880._in(0), intervention__filter_880_reject._in(0)]
    )
    (
        intervention__join_776_left_unionfullouter._out(0)
        >> [intervention__summarize_779._in(0), intervention__summarize_777._in(0), intervention__filter_869._in(0),
              intervention__filter_869_reject._in(0), intervention__union_784._in(0),
              intervention__union_784._in(1)]
    )
    selectsum_csv_1667 >> intervention__alteryxselect_860
    (
        intervention__join_834_left_unionleftouter._out(0)
        >> [intervention__filter_876._in(0), intervention__filter_876_reject._in(0), intervention__filter_816._in(0),
              intervention__join_815_inner._in(0)]
    )
    total_inf_condi_1662 >> intervention__join_850_left_unionleftouter._in(0)
    (
        intervention__filter_816._out(0)
        >> [intervention__union_817._in(0), intervention__filter_879._in(0), intervention__filter_879_reject._in(0),
              intervention__filter_1655._in(0), intervention__filter_1655_reject._in(0)]
    )
    (
        intervention__formula_814_0._out(0)
        >> [intervention__filter_816._in(1), intervention__join_815_inner._in(1), intervention__filter_1652._in(0),
              intervention__filter_1652_reject._in(0)]
    )
    selectdistinct__1663 >> intervention__join_850_left_unionleftouter._in(1)
