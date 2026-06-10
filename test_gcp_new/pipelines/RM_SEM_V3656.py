from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "RM_SEM_V3656",
    version = 1,
    auto_layout = False,
    params = Parameters(
      username_Database__LOADI_280 = "''",
      password_Database__LOADI_280 = "''",
      username_Database__LOADI_363 = "''",
      password_Database__LOADI_363 = "''",
      username_Database__LOADI_292 = "''",
      password_Database__LOADI_292 = "''",
      username_Database__LOADI_283 = "''",
      password_Database__LOADI_283 = "''",
      username_Database__LOADI_394 = "''",
      password_Database__LOADI_394 = "''",
      username_Database__LOADI_395 = "''",
      password_Database__LOADI_395 = "''",
      username_Database__REPOR_351 = "''",
      password_Database__REPOR_351 = "''",
      username_Database__LOADI_341 = "''",
      password_Database__LOADI_341 = "''",
      username_Database__LOADI_313 = "''",
      password_Database__LOADI_313 = "''",
      username_Database__LOADI_403 = "''",
      password_Database__LOADI_403 = "''",
      username_Database__LOADI_330 = "''",
      password_Database__LOADI_330 = "''",
      jdbcUrl_SEM_Support_Lis_475 = "''",
      username_SEM_Support_Lis_475 = "''",
      password_SEM_Support_Lis_475 = "''",
      workflow_name = "'RM_SEM_V3656'"
    )
)

with Pipeline(args) as pipeline:
    database__loadi_280 = Process(
        name = "Database__LOADI_280",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_280}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_280"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\nMD.OD\n, MD.NDOD\n, FND.FLT_NBR\n, DDD.DPTR_DATE\n, DDD.DPTR_WEEK_START_DATE\n, DDD.DPTR_DAY_OF_WEEK\n, MD.MILES\n, MD.PLNG_REG_SHORT_NAME\n, LF.LEG_CABIN_CD\n, NVL(SUM(LF.LEG_CAB_RPM_CNT ),0) RPMS\n, NVL(SUM(LF.LEG_CAB_ASM_CNT ),0) ASMS\n, nvl(sum(LF.LEG_CAB_BKG_CNT), 0) BKGS\n, nvl(sum(LF.LEG_CAB_CAP_CNT), 0) CAP\n, nvl(sum(LF.CLS_AVAIL_MAX_CNT), 0) AVAIL\n\nFROM\nABK_DW.INV_FLT_LEG_CAB_MILES_FACT LF\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON LF.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.FLIGHT_NBR_DIM FND ON LF.MKTG_FLT_NBR_KEY = FND.FLIGHT_NBR_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DDD ON LF.LEG_DPTR_DATE_KEY = DDD.DPTR_DATE_KEY\nJOIN ABK_DW.MARKET_DIM MD ON LF.LEG_MARKET_KEY = MD.MARKET_KEY\n\nWHERE SDD.SNAPSHOT_DATE BETWEEN SYSDATE-1 AND SYSDATE\nand FND.SVC_TYPE IN ('LEASE', 'SCHEDULED')\nAND FND.FLT_TYPE = 'P'\nAND (FND.FLT_NBR BETWEEN 1 AND 1499 OR FND.FLT_NBR BETWEEN 2000 AND 2959 OR FND.FLT_NBR BETWEEN 3300 AND 3499)\n--AND MD.OD = 'GSTJNU'\n\nGROUP BY \nMD.OD\n, MD.NDOD\n, FND.FLT_NBR\n, DDD.DPTR_DATE\n, DDD.DPTR_WEEK_START_DATE\n, DDD.DPTR_DAY_OF_WEEK\n, MD.MILES\n, MD.PLNG_REG_SHORT_NAME\n, LF.LEG_CABIN_CD"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_280.yml")
        ),
        input_ports = None
    )
    database__loadi_283 = Process(
        name = "Database__LOADI_283",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_283}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_283"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select\nLMD.LEG_NDOD\n, TTMD.TT_NDOD\n, TDD.LEG_TK_DPTR_WEEK_START_DATE +364 DPTR_WEEK\n, SUM(REV.LEG_PAX_CNT)\n\nfrom        \nREVENUE_DW.PAX_LEG_FUTURE_REVENUE_FACT REV\nJOIN REVENUE_DW.LEG_MARKET_DIM LMD ON REV.LEG_MARKET_KEY = LMD.LEG_MARKET_KEY\nJOIN REVENUE_DW.TRUE_TRIP_MARKET_DIM TTMD ON REV.TRUE_TRIP_MARKET_KEY = TTMD.TRUE_TRIP_MARKET_KEY\nJOIN REVENUE_DW.TKT_ISSUE_DATE_DIM TID ON REV.TKT_ISSUE_DATE_KEY = TID.TKT_ISSUE_DATE_KEY\nJOIN REVENUE_DW.LEG_TKTD_DPTR_DATE_DIM TDD ON REV.LEG_TKTD_DPTR_DATE_KEY = TDD.LEG_TKTD_DPTR_DATE_KEY\n\nwhere\nTDD.LEG_TK_DPTR_WEEK_START_DATE BETWEEN SYSDATE-380 AND SYSDATE-15\nAND TID.TKT_ISSUE_WEEK_START_DATE BETWEEN ADD_MONTHS(LAST_DAY(SYSDATE),-12) AND ADD_MONTHS(LAST_DAY(SYSDATE),-11)\n--AND LMD.LEG_NDOD= 'GSTJNU'\n\nGROUP BY LMD.LEG_NDOD, TTMD.TT_NDOD, TDD.LEG_TK_DPTR_WEEK_START_DATE +364"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_283.yml")
        ),
        input_ports = None
    )
    database__loadi_292 = Process(
        name = "Database__LOADI_292",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_292}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_292"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\nA.NDOD\n, count(1) CARRIER_CNT\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'AS' then 1 else 0 end) AS_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'DL' then 1 else 0 end) DL_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'UA' then 1 else 0 end) UA_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'AA' then 1 else 0 end) AA_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'B6' then 1 else 0 end) B6_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'WN' then 1 else 0 end) WN_IND\n, sum(case when A.MKTG_CARRIER_AIRLINE_CODE = 'HA' then 1 else 0 end) HA_IND\n\nFROM\n    (\n    select\n    md.NDOD\n    , mcd.MKTG_CARRIER_AIRLINE_CODE\n    , sum(isf.ECONOMY_CLASS_ASM_CNT) ASMS\n\n    from\n    ABK_DW.INDUSTRY_SCHEDULE_FACT isf\n    join ABK_DW.MARKET_DIM md on isf.MARKET_KEY = md.MARKET_KEY\n    join ABK_DW.MKTG_CARRIER_DIM mcd on isf.MKTG_CARRIER_KEY = mcd.MKTG_CARRIER_KEY\n\n    where\n    isf.SNAPSHOT_DATE between sysdate-1 and sysdate\n    and md.YIELD_USER_ID != 'UNUSED'\n--AND MD.NDOD = 'GSTJNU'\n\n    group by  md.NDOD, mcd.MKTG_CARRIER_AIRLINE_CODE\n    ) A\nGROUP BY A.NDOD"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_292.yml")
        ),
        input_ports = None
    )
    database__loadi_313 = Process(
        name = "Database__LOADI_313",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_313}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_313"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\nMD.OD, MD.ORIG, MD.ORIG_CITY_NAME, MD.ORIG_AIRPORT_NAME, MD.ORIG_STATE, MD.DEST, MD.DEST_CITY_NAME, MD.DEST_AIRPORT_NAME, MD.DEST_STATE\n\nFROM\nABK_dW.MARKET_DIM MD\n\n\nORDER BY 1"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_313.yml")
        ),
        input_ports = None
    )
    database__loadi_330 = Process(
        name = "Database__LOADI_330",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_330}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_330"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\nMD.OD\n, round(count(1)) flt_cnt_next_3_mth\n, round(count(1) / 3,0) flt_cnt\n\nFROM\nABK_DW.SCHEDULE_FACT BAAF\nJOIN ABK_DW.MARKET_DIM MD ON BAAF.MARKET_KEY = MD.MARKET_KEY\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON BAAF.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DDD ON BAAF.DPTR_DATE_KEY = DDD.DPTR_DATE_KEY\njoin ABK_DW.FLIGHT_NBR_DIM fnd on BAAF.FLIGHT_NBR_KEY = fnd.FLIGHT_NBR_KEY\n\n\nWHERE\nDDD.DPTR_DATE between last_day(sysdate) and add_months(last_day(sysdate),3)\nand SDD.SNAPSHOT_DATE between sysdate-1 and sysdate\nand FND.SVC_TYPE IN ('LEASE', 'SCHEDULED')\nAND FND.FLT_TYPE = 'P'\nAND (FND.FLT_NBR BETWEEN 1 AND 1499 OR FND.FLT_NBR BETWEEN 2000 AND 2959 OR FND.FLT_NBR BETWEEN 3300 AND 3499)\n--AND MD.OD = 'GSTJNU'\n\ngroup by MD.OD"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_330.yml")
        ),
        input_ports = None
    )
    database__loadi_341 = Process(
        name = "Database__LOADI_341",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_341}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_341"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "select\nmd.NDOD\n, sum(lc.LEG_CAB_ASM_CNT) / sys.TTL_ASM PCT_SYS_ASMS\n\nfrom ABK_DW.INV_FLT_LEG_CAB_MILES_FACT lc\njoin ABK_DW.MKTG_FLT_NBR_DIM fnd on fnd.MKTG_FLT_NBR_KEY = lc.MKTG_FLT_NBR_KEY\njoin ABK_DW.MARKET_DIM md on lc.LEG_MARKET_KEY = md.MARKET_KEY\njoin ABK_DW.SNAPSHOT_DATE_DIM sdd on lc.SNAPSHOT_DATE_KEY = sdd.SNAPSHOT_DATE_KEY\njoin\n(\nselect\nsum(lc.LEG_CAB_ASM_CNT) TTL_ASM\nfrom ABK_DW.INV_FLT_LEG_CAB_MILES_FACT lc\njoin ABK_DW.MKTG_FLT_NBR_DIM fnd on fnd.MKTG_FLT_NBR_KEY = lc.MKTG_FLT_NBR_KEY\njoin ABK_DW.SNAPSHOT_DATE_DIM sdd on lc.SNAPSHOT_DATE_KEY = sdd.SNAPSHOT_DATE_KEY\n\n\nwhere \nSDD.SNAPSHOT_DATE BETWEEN SYSDATE-1 AND SYSDATE\nand fnd.SVC_TYPE_DESC IN ('LEASE', 'SCHEDULED')\nAND fnd.FLT_TYPE_CD = 'P'\nAND ( fnd.MKTG_FLT_NBR BETWEEN 1 AND 1499 OR  fnd.MKTG_FLT_NBR BETWEEN 2000 AND 2959 OR  fnd.MKTG_FLT_NBR BETWEEN 3300 AND 3499)\n) sys on 1=1\n\nwhere SDD.SNAPSHOT_DATE BETWEEN SYSDATE-1 AND SYSDATE\nand fnd.SVC_TYPE_DESC IN ('LEASE', 'SCHEDULED')\nAND fnd.FLT_TYPE_CD = 'P'\nAND ( fnd.MKTG_FLT_NBR BETWEEN 1 AND 1499 OR  fnd.MKTG_FLT_NBR BETWEEN 2000 AND 2959 OR  fnd.MKTG_FLT_NBR BETWEEN 3300 AND 3499)\n--AND MD.NDOD = 'GSTJNU'\n\nGROUP BY MD.NDOD, SYS.TTL_ASM"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_341.yml")
        ),
        input_ports = None
    )
    database__loadi_363 = Process(
        name = "Database__LOADI_363",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_363}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_363"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT \nMD.OD\n, MD.NDOD\n, DDD.DPTR_DATE\n, FND.FLT_NBR\n, L.CABIN_CODE\n, L.SEAT_INDEX_CURR\n, DFF.CAP\n, L.SEAT_INDEX_CURR - COUNT(LP.BID_PRICE) AS NBR_ZERO_BP\n\nFROM ABK_DW.RMS_LEG_CABIN_BP LP\nJOIN ABK_DW.RMS_LEG_CABIN L ON LP.LEG_CABIN_KEY = L.LEG_CABIN_KEY\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON L.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.MARKET_DIM MD ON L.MARKET_KEY = MD.MARKET_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DDD ON L.DPTR_DATE_KEY = DDD.DPTR_DATE_KEY\nJOIN ABK_DW.RMS_DATED_FLIGHT_FACT DFF ON L.DATED_FLIGHT_KEY = DFF.DATED_FLIGHT_KEY AND L.SNAPSHOT_DATE_KEY = DFF.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.FLIGHT_NBR_DIM FND ON DFF.FLIGHT_NBR_KEY = FND.FLIGHT_NBR_KEY\n\nWHERE SDD.SNAPSHOT_DATE = TRUNC(SYSDATE)\nAND LP.SEAT_INDEX <= L.SEAT_INDEX_CURR\n--AND DDD.DPTR_DATE BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE+180)\nAND (FND.FLT_NBR BETWEEN 1 AND 1499 OR FND.FLT_NBR BETWEEN 2000 AND 2959 OR FND.FLT_NBR BETWEEN 3300 AND 3499)\n--AND MD.OD = 'GSTJNU'\n\nGROUP BY\nMD.OD\n,  MD.NDOD\n, DDD.DPTR_DATE\n, FND.FLT_NBR\n, L.CABIN_CODE\n, L.SEAT_INDEX_CURR\n, DFF.CAP"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_363.yml")
        ),
        input_ports = None
    )
    database__loadi_394 = Process(
        name = "Database__LOADI_394",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_394}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_394"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT \n  MD.OD\n, DD.DPTR_DATE\n, SUM(LP.BID_PRICE) TOTAL_BP\n\nFROM ABK_DW.RMS_LEG_CABIN_BP LP\nJOIN ABK_DW.RMS_DATED_FLIGHT_FACT DFF ON LP.DATED_FLIGHT_KEY = DFF.DATED_FLIGHT_KEY AND LP.SNAPSHOT_DATE_KEY = DFF.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DD ON DFF.DPTR_DATE_KEY = DD.DPTR_DATE_KEY\nJOIN ABK_DW.RMS_LEG_CABIN L ON LP.LEG_CABIN_KEY = L.LEG_CABIN_KEY\nJOIN ABK_DW.MARKET_DIM MD ON L.MARKET_KEY = MD.MARKET_KEY\nJOIN ABK_DW.FLIGHT_NBR_DIM F ON DFF.FLIGHT_NBR_KEY = F.FLIGHT_NBR_KEY\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON LP.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\n\nWHERE SDD.SNAPSHOT_DATE = TRUNC(SYSDATE)\n\n--AND DD.DPTR_DATE BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE+180)\nAND (LP.SEAT_INDEX BETWEEN (L.SEAT_INDEX_CURR - ROUND((L.ACH_DMD - L.CAB_BKG_CNT),0)) AND (L.SEAT_INDEX_CURR) or LP.SEAT_INDEX > L.SEAT_INDEX_CURR)\n--AND MD.OD = 'GSTJNU'\n\nGROUP BY\n  MD.OD\n, DD.DPTR_DATE"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_394.yml")
        ),
        input_ports = None
    )
    database__loadi_395 = Process(
        name = "Database__LOADI_395",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_395}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_395"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "WITH CR AS (SELECT \n  MD.OD\n, DD.DPTR_DATE\n, C.CLASS_OF_SERVICE_CODE\n, SUM(SCC.ACHIEVABLE_DMD * SCC.YIELD_AMT) CLASS_REVENUE\n\nFROM ABK_DW.RMS_SEG_CABIN_CLASS SCC\nJOIN ABK_DW.RMS_SEG_CABIN SC ON SCC.SEG_CABIN_KEY = SC.SEG_CABIN_KEY\nJOIN ABK_DW.RMS_DATED_FLIGHT_FACT DFF ON SCC.DATED_FLIGHT_KEY = DFF.DATED_FLIGHT_KEY AND SCC.SNAPSHOT_DATE_KEY = DFF.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DD ON DFF.DPTR_DATE_KEY = DD.DPTR_DATE_KEY\nJOIN ABK_DW.MARKET_DIM MD ON SC.MARKET_KEY = MD.MARKET_KEY\nJOIN ABK_DW.CLASS_OF_SERVICE_DIM C ON SCC.CLASS_OF_SERVICE_KEY = C.CLASS_OF_SERVICE_KEY\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON SCC.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.FLIGHT_NBR_DIM F ON DFF.FLIGHT_NBR_KEY = F.FLIGHT_NBR_KEY\n\nWHERE SDD.SNAPSHOT_DATE = TRUNC(SYSDATE)\n--AND DD.DPTR_DATE BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE+180)\n--AND MD.OD = 'GSTJNU'\nAND F.FLT_TYPE = 'P'\nAND (F.FLT_NBR BETWEEN 1 AND 1499 OR F.FLT_NBR BETWEEN 2000 AND 2959 OR F.FLT_NBR BETWEEN 3300 AND 3499)\n\nGROUP BY\n  MD.OD\n, DD.DPTR_DATE\n, C.CLASS_OF_SERVICE_CODE)\n\nSELECT \n  CR.OD\n, CR.DPTR_DATE\n, SUM(CR.CLASS_REVENUE) REVENUE\n\nFROM CR\n\nGROUP BY\n  CR.OD\n, CR.DPTR_DATE"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_395.yml")
        ),
        input_ports = None
    )
    database__loadi_403 = Process(
        name = "Database__LOADI_403",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:RMOR_PROD/__EncPwd1__@LOADING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__LOADI_403}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__LOADI_403"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\n MD.OD\n, DDD.DPTR_MTH_YEAR_NAME\n, COUNT(FLI.FLT_NBR) FLT_CNT_NEXT_MTH\n\nFROM \nABK_DW.INV_FLT_LEG_CAB_MILES_FACT LF\nJOIN ABK_DW.MARKET_DIM MD ON LF.LEG_MARKET_KEY = MD.MARKET_KEY\nJOIN ABK_DW.SNAPSHOT_DATE_DIM SDD ON LF.SNAPSHOT_DATE_KEY = SDD.SNAPSHOT_DATE_KEY\nJOIN ABK_DW.DPTR_DATE_DIM DDD ON LF.LEG_DPTR_DATE_KEY = DDD.DPTR_DATE_KEY\nJOIN ABK_DW.FLIGHT_NBR_DIM FLI ON LF.MKTG_FLT_NBR_KEY = FLI.FLIGHT_NBR_KEY\n\nWHERE \nSDD.SNAPSHOT_DATE between sysdate-1 and sysdate\nAND DDD.DPTR_DATE BETWEEN last_day(add_months(sysdate,0))+1 AND last_day(add_months(sysdate,1))\nAND (FLI.FLT_NBR BETWEEN 1 AND 1499 OR FLI.FLT_NBR BETWEEN 2000 AND 2959 OR FLI.FLT_NBR BETWEEN 3300 AND 3499)\n--AND MD.OD = 'GSTJNU'\n\nGROUP BY \n MD.OD\n , DDD.DPTR_MTH_YEAR_NAME"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__LOADI_403.yml")
        ),
        input_ports = None
    )
    database__repor_351 = Process(
        name = "Database__REPOR_351",
        properties = OracleSource(
          connector = {
            "kind": "oracle",
            "id": "transpiled_connection",
            "properties": {
              "database": "dbName",
              "server": "oci:qsi_query/__EncPwd1__@REPORTING.DATAWAREHOUSE.DB.INSIDEAAG.COM:1522/EDWPROD",
              "username": "${username_Database__REPOR_351}",
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
            tableFullName = OracleSource.WarehouseTableName(schema = "schema", name = "Database__REPOR_351"),
            warehouseQuery = OracleSource.WarehouseQuery(
              query = "SELECT\n\nMD.NDOD \n, SUM(CASE WHEN MCD.MKTG_CARR_AIRLINE_CODE = 'AS' THEN QF.QSI_CNT END) AS_QSI_PTS\n, SUM(QF.QSI_CNT) TTL_QSI_PTS\n\nFROM\nQSI_DW.QUALITY_OF_SERVICE_FACT QF\nJOIN QSI_DW.MARKET_DIM MD ON QF.MARKET_KEY = MD.MARKET_KEY\nJOIN QSI_DW.SERVICE_MONTH_DIM SMD ON QF.SERVICE_MONTH_KEY = SMD.SERVICE_MONTH_KEY\nJOIN QSI_DW.MKTG_CARRIER_DIM MCD ON QF.MKTG_CARRIER_KEY = MCD.MKTG_CARRIER_KEY\n\n\nWHERE\nMD.YIELD_USER_ID != 'UNUSED'\nAND SMD.SERVICE_MTH_START_DATE BETWEEN LAST_DAY(ADD_MONTHS(SYSDATE, -1)) AND LAST_DAY(ADD_MONTHS(SYSDATE, 2))\n--AND MD.NDOD = 'GSTJNU'\n\nGROUP BY  MD.NDOD"
            )
          ),
          format = OracleSource.OracleReadFormat(schema = "external_sources/RM_SEM_V3656/Database__REPOR_351.yml")
        ),
        input_ports = None
    )
    multirowformula_440 = Process(
        name = "MultiRowFormula_440",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\n\n\nimport pandas as pd\n\ndef safe_len(v):\n    return 0 if pd.isna(v) else len(v)\n\ndef calculate(pdf):\n    FLOW_RANK_lag1 = 0\n    rows = []\n    for _, row in pdf.iterrows():\n\n\n        FLOW_RANK_new = int(FLOW_RANK_lag1 + 1)\n\n        FLOW_RANK_lag1 = FLOW_RANK_new\n        newRow = row.copy()\n        newRow[\"FLOW_RANK\"] = FLOW_RANK_new\n        rows.append(newRow)\n\n    if not rows:\n        empty = pdf.iloc[0:0].copy()\n        if \"FLOW_RANK\" not in empty.columns:\n            empty[\"FLOW_RANK\"] = pd.Series(dtype=\"object\")\n        return empty\n\n    return pd.DataFrame(rows)\n\n\n\ndf = in0.copy()\nsort_columns = [\"LEG_NDOD\", \"prophecy_row_id\"]\ngroup_columns = [\"LEG_NDOD\"]\n\nif sort_columns:\n    df = df.sort_values(sort_columns, kind=\"mergesort\")\n\nif group_columns:\n    out0 = (\n        df.groupby(group_columns, sort=False, group_keys=False, dropna=False)\n        .apply(calculate)\n        .reset_index(drop=True)\n    )\nelse:\n    out0 = calculate(df).reset_index(drop=True)\n\n\n"
        ),
        is_custom_output_schema = True
    )
    rm_sem_v3656__alteryxselect_338 = Process(
        name = "RM_SEM_V3656__AlteryxSelect_338",
        properties = ModelTransform(modelName = "RM_SEM_V3656__AlteryxSelect_338"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5"]
    )
    rm_sem_v3656__filter_288 = Process(
        name = "RM_SEM_V3656__Filter_288",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Filter_288")
    )
    rm_sem_v3656__formula_321_0 = Process(
        name = "RM_SEM_V3656__Formula_321_0",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Formula_321_0")
    )
    rm_sem_v3656__join_311_left_unionleftouter = Process(
        name = "RM_SEM_V3656__Join_311_left_UnionLeftOuter",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Join_311_left_UnionLeftOuter"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8", "in_9", "in_10", "in_11", "in_12"]
    )
    rm_sem_v3656__join_329_inner = Process(
        name = "RM_SEM_V3656__Join_329_inner",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Join_329_inner"),
        input_ports = ["in_0", "in_1"]
    )
    rm_sem_v3656__join_373_inner = Process(
        name = "RM_SEM_V3656__Join_373_inner",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Join_373_inner"),
        input_ports = ["in_0", "in_1"]
    )
    rm_sem_v3656__join_384_inner = Process(
        name = "RM_SEM_V3656__Join_384_inner",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Join_384_inner"),
        input_ports = ["in_0", "in_1"]
    )
    rm_sem_v3656__multirowformula_440_row_id_0 = Process(
        name = "RM_SEM_V3656__MultiRowFormula_440_row_id_0",
        properties = ModelTransform(modelName = "RM_SEM_V3656__MultiRowFormula_440_row_id_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    rm_sem_v3656__sort_377 = Process(
        name = "RM_SEM_V3656__Sort_377",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Sort_377")
    )
    rm_sem_v3656__summarize_323 = Process(
        name = "RM_SEM_V3656__Summarize_323",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Summarize_323")
    )
    rm_sem_v3656__summarize_367 = Process(
        name = "RM_SEM_V3656__Summarize_367",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Summarize_367")
    )
    rm_sem_v3656__summarize_370 = Process(
        name = "RM_SEM_V3656__Summarize_370",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Summarize_370")
    )
    rm_sem_v3656__summarize_379 = Process(
        name = "RM_SEM_V3656__Summarize_379",
        properties = ModelTransform(modelName = "RM_SEM_V3656__Summarize_379")
    )
    rm_sem_v3656__googlesheetsoutput_476 = Process(
        name = "RM_SEM_V3656__googlesheetsoutput_476",
        properties = ModelTransform(modelName = "RM_SEM_V3656__googlesheetsoutput_476")
    )
    sem_support_lis_475 = Process(
        name = "SEM_Support_Lis_475",
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
          properties = SFTPTarget.SFTPTargetInternal(filePath = "_externals\\1\\SEM_Support_List.xlsx")
        )
    )
    rm_sem_v3656__multirowformula_440_row_id_0 >> multirowformula_440
    database__loadi_330 >> rm_sem_v3656__alteryxselect_338._in(1)
    database__repor_351 >> rm_sem_v3656__join_311_left_unionleftouter._in(9)
    database__loadi_395 >> rm_sem_v3656__join_311_left_unionleftouter._in(8)
    (
        rm_sem_v3656__join_311_left_unionleftouter._out(0)
        >> [rm_sem_v3656__summarize_323._in(0), rm_sem_v3656__formula_321_0._in(0)]
    )
    (
        rm_sem_v3656__join_373_inner._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(6), rm_sem_v3656__join_311_left_unionleftouter._in(11),
              rm_sem_v3656__join_384_inner._in(1)]
    )
    rm_sem_v3656__summarize_370._out(0) >> [rm_sem_v3656__join_373_inner._in(0), rm_sem_v3656__sort_377._in(0)]
    (
        database__loadi_363._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(1), rm_sem_v3656__join_311_left_unionleftouter._in(4)]
    )
    database__loadi_394 >> rm_sem_v3656__join_311_left_unionleftouter._in(7)
    (
        rm_sem_v3656__filter_288._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(2), rm_sem_v3656__multirowformula_440_row_id_0._in(2)]
    )
    multirowformula_440 >> rm_sem_v3656__join_311_left_unionleftouter._in(12)
    rm_sem_v3656__summarize_323._out(0) >> [rm_sem_v3656__alteryxselect_338._in(0), rm_sem_v3656__join_329_inner._in(0)]
    (
        database__loadi_292._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(10), rm_sem_v3656__summarize_379._in(0)]
    )
    (
        rm_sem_v3656__formula_321_0._out(0)
        >> [rm_sem_v3656__alteryxselect_338._in(3), rm_sem_v3656__alteryxselect_338._in(4),
              rm_sem_v3656__join_329_inner._in(1)]
    )
    (
        database__loadi_283._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(0), rm_sem_v3656__join_311_left_unionleftouter._in(5),
              rm_sem_v3656__join_384_inner._in(0), rm_sem_v3656__multirowformula_440_row_id_0._in(0),
              rm_sem_v3656__filter_288._in(0)]
    )
    database__loadi_403 >> rm_sem_v3656__alteryxselect_338._in(2)
    database__loadi_313 >> rm_sem_v3656__alteryxselect_338._in(5)
    (
        rm_sem_v3656__alteryxselect_338._out(0)
        >> [sem_support_lis_475._in(0), rm_sem_v3656__summarize_367._in(0),
              rm_sem_v3656__googlesheetsoutput_476._in(0)]
    )
    (
        database__loadi_341._out(0)
        >> [rm_sem_v3656__join_311_left_unionleftouter._in(3), rm_sem_v3656__multirowformula_440_row_id_0._in(1)]
    )
    database__loadi_280._out(0) >> [rm_sem_v3656__join_373_inner._in(1), rm_sem_v3656__summarize_370._in(0)]
