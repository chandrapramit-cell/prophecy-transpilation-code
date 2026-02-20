from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Household_Clustering_1_Household_Creation_Priority_2",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_aka_alxaa2_Quer_1 = "''",
      password_aka_alxaa2_Quer_1 = "''",
      workflow_name = "'Household_Clustering_1_Household_Creation_Priority_2'"
    )
)

with Pipeline(args) as pipeline:
    household_clustering_1_household_creation_priority_2__filter_27_reject = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27_reject",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27_reject")
    )
    household_clustering_1_household_creation_priority_2__summarize_12 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_12",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_12")
    )
    household_clustering_1_household_creation_priority_2__summarize_19 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_19",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_19")
    )
    household_clustering_1_household_creation_priority_2__unique_4 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Unique_4",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Unique_4")
    )
    household_clustering_1_household_creation_priority_2__cleanse_11 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11")
    )
    household_clustering_1_household_creation_priority_2__summarize_22 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_22",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_22"),
        input_ports = ["in_0", "in_1"]
    )
    household_clustering_1_household_creation_priority_2__summarize_26 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_26",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_26")
    )
    household_clustering_1_household_creation_priority_2__filter_13 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_13",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_13")
    )
    household_clustering_1_household_creation_priority_2__summarize_24 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_24",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_24")
    )
    household_clustering_1_household_creation_priority_2__summarize_21 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_21",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_21"),
        input_ports = ["in_0", "in_1"]
    )
    household_clustering_1_household_creation_priority_2__join_14_left = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Join_14_left",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Join_14_left"),
        input_ports = ["in_0", "in_1"]
    )
    aka_alxaa2_quer_1 = Process(
        name = "aka_alxaa2_Quer_1",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "aka:5e5e7e6749b7fb13a082f9a7",
              "username": "${username_aka_alxaa2_Quer_1}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "aka_alxaa2_Quer_1"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select\n\tm.mbr_indv_be_key,\n\trcst.mbr_sk,\n\tm.sub_id,\n\trcst.actvty_yr_mo_sk,\n\tm.mbr_relshp_cd,\n\tm.mbr_first_nm,\n\tm.mbr_last_nm,\n\tm.mbr_home_addr_ln_1,\n\tm.mbr_home_addr_ln_2,\n\tm.mbr_home_addr_city_nm,\n\tm.mbr_home_addr_st_cd,\n\tm.mbr_home_addr_cnty_nm,\n\tm.mbr_home_addr_zip_cd_5,\n\trcst.grp_id,\n\trcst.mbr_age_at_actvty_yr_mo,\n\tcase\n\t\twhen p.exprnc_cat_cd = 'FEP' then 1\n\t\telse 0\n\tend as FEP_IN,\n\tcase\n\t\twhen rcst.prod_bill_cmpnt_cov_cat_cd = 'DNTL' then 2\n\t\twhen rcst.prod_bill_cmpnt_cov_cat_cd = 'MED' then 1\n\t\twhen rcst.prod_bill_cmpnt_cov_cat_cd = 'LIFE' then 3\n\t\telse 4\n\tend as prod_rank\n\n\nfrom\n\tprod.mbr_rcst_ct_f rcst\n\tinner join\n\t\tprod.mbr_d m on m.mbr_sk = rcst.mbr_sk\n\tinner join\n\t\tprod.prod_d p on p.prod_sk = rcst.prod_sk\n\tinner join\n\t\tprod.yr_mo_d yr_mo on yr_mo.yr_mo_sk = rcst.actvty_yr_mo_sk\n\nwhere\n\tm.mbr_indv_be_key != '0'\n\tand yr_mo.first_dt_of_mo >= char(current date - 15 month,iso)\n\tand yr_mo.first_dt_of_mo < char(current date - 1 month, iso)\n\tand m.mbr_relshp_cd in ('SUB','SPOUSE','DPNDT')\n\tand m.host_mbr_in = 'N'\n\t--Plan information \n\tand p.prod_sh_nm_dlvry_meth_cd in ('EPO','HPN','PPO', 'HMO','TRAD')\n\t--Group exclusions\n\tand left(rcst.grp_id, 2) != '65'"
            )
          ),
          format = OracleSource.OracleReadFormat(
            schema = "external_sources/Household_Clustering_1_Household_Creation_Priority_2/aka_alxaa2_Quer_1.yml"
          )
        ),
        input_ports = None
    )
    household_clustering_1_household_creation_priority_2__filter_17 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_17",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_17")
    )
    household_clustering_1_household_creation_priority_2__summarize_25 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_25",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_25")
    )
    household_clustering_1_household_creation_priority_2__summarize_20 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_20",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Summarize_20")
    )
    household_clustering_1_household_creation_priority_2__filter_27 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Filter_27")
    )
    recast_cache_cs_2 = Process(
        name = "recast_cache_cs_2",
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
          format = SFTPTarget.CsvWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\corpdata\\files\\library\\library\\Alteryx\\Advanced Analytics\\Call Model\\Households\\Cache\\recast_cache.csv"
          )
        )
    )
    household_clustering_1_household_creation_priority_2__formula_32_0 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Formula_32_0",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Formula_32_0")
    )
    address_for_geo_18 = Process(
        name = "address_for_geo_18",
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
          format = SFTPTarget.CsvWriteFormat(),
          properties = SFTPTarget.SFTPTargetInternal(
            filePath = "\\\\corpdata\\files\\library\\library\\Alteryx\\Advanced Analytics\\Call Model\\Households\\Cache\\address_for_geoencoding.csv"
          )
        )
    )
    household_clustering_1_household_creation_priority_2__formula_31_0 = Process(
        name = "Household_Clustering_1_Household_Creation_Priority_2__Formula_31_0",
        properties = ModelTransform(modelName = "Household_Clustering_1_Household_Creation_Priority_2__Formula_31_0")
    )
    (
        household_clustering_1_household_creation_priority_2__unique_4._out(0)
        >> [household_clustering_1_household_creation_priority_2__cleanse_11._in(0),
              household_clustering_1_household_creation_priority_2__summarize_20._in(0),
              household_clustering_1_household_creation_priority_2__summarize_24._in(0),
              household_clustering_1_household_creation_priority_2__formula_31_0._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__cleanse_11._out(0)
        >> [household_clustering_1_household_creation_priority_2__join_14_left._in(0),
              household_clustering_1_household_creation_priority_2__summarize_21._in(0),
              household_clustering_1_household_creation_priority_2__summarize_12._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__join_14_left._out(0)
        >> [household_clustering_1_household_creation_priority_2__summarize_22._in(1),
              household_clustering_1_household_creation_priority_2__filter_17._in(0),
              household_clustering_1_household_creation_priority_2__summarize_25._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__filter_17._out(0)
        >> [household_clustering_1_household_creation_priority_2__summarize_26._in(0),
              household_clustering_1_household_creation_priority_2__summarize_22._in(0),
              household_clustering_1_household_creation_priority_2__formula_32_0._in(0)]
    )
    household_clustering_1_household_creation_priority_2__formula_32_0 >> address_for_geo_18
    aka_alxaa2_quer_1 >> household_clustering_1_household_creation_priority_2__unique_4
    household_clustering_1_household_creation_priority_2__formula_31_0 >> recast_cache_cs_2
    (
        household_clustering_1_household_creation_priority_2__summarize_12._out(0)
        >> [household_clustering_1_household_creation_priority_2__filter_13._in(0),
              household_clustering_1_household_creation_priority_2__summarize_19._in(0)]
    )
    (
        household_clustering_1_household_creation_priority_2__filter_13._out(0)
        >> [household_clustering_1_household_creation_priority_2__filter_27._in(0),
              household_clustering_1_household_creation_priority_2__filter_27_reject._in(0),
              household_clustering_1_household_creation_priority_2__join_14_left._in(1),
              household_clustering_1_household_creation_priority_2__summarize_21._in(1)]
    )
