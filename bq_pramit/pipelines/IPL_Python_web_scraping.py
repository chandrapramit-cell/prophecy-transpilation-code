from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "IPL_Python_web_scraping",
    version = 1,
    auto_layout = False,
    params = Parameters(
      Question__Text_Box_67 = "''",
      Question__Text_Box_72 = "''",
      workflow_name = "'IPL_Python_web_scraping'",
      Question__Drop_Down_76 = "''",
      Question__Drop_Down_78 = "''"
    )
)

with Pipeline(args) as pipeline:
    iplfantasystats_80 = Process(
        name = "IPLFantasyStats_80",
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
          properties = SFTPTarget.SFTPTargetInternal(filePath = "IPL Fantasy Stats.xlsx"),
          format = SFTPTarget.XLSXWriteFormat()
        )
    )
    jupytercode_1 = Process(
        name = "JupyterCode_1",
        properties = Script(
          ports = None,
          scriptMethodHeader = "def Script(in0: DataFrame) -> (DataFrame, DataFrame):",
          scriptMethodFooter = "return (out0, out1)",
          script = "# List all non-standard packages to be imported by your \n\n# script here (only missing packages will be installed)\n\nfrom ayx import Package\n\n#Package.installPackages(['pandas','numpy'])\nfrom ayx import Alteryx\n\nimport requests\n\nfrom bs4 import BeautifulSoup\n\nimport pandas as pd\n\nimport re\n\nimport numpy as np\n\n\n\ndef extract_batting_data(game,series_id, match_id):\n\n\n\n    URL = 'https://www.espncricinfo.com/series/indian-premier-league-2023-'+ str(series_id) + game + str(match_id) + '/full-scorecard'\n\n    page = requests.get(URL)\n\n    bs = BeautifulSoup(page.content, 'lxml')\n\n\n\n    table_body=bs.find_all('tbody')\n\n    batsmen_df = pd.DataFrame(columns=[\"Name\",\"Desc\",\"Runs\", \"Balls\", \"4s\", \"6s\", \"SR\", \"Team\"])\n\n    \n\n    for i, table in enumerate(table_body[0:4:2]):\n\n        rows = table.find_all('tr')\n\n        \n\n        \n\n        for row in rows[::1]:\n\n            \n\n            \n\n            cols=row.find_all('td')\n\n            cols=[x.text.strip() for x in cols]\n\n            \n\n           \n\n            if cols[0] == 'Extras':\n\n                continue\n\n            if len(cols) > 7:\n\n                \n\n                batsmen_df = pd.concat([batsmen_df, pd.DataFrame([pd.Series(\n\n                [re.sub(r\"\\W+\", ' ', cols[0].split(\"(c)\")[0]).strip(), cols[1], \n\n                cols[2], cols[3], cols[5], cols[6], cols[7], i+1], \n\n                index=batsmen_df.columns )])], ignore_index=True)\n\n                \n\n            if len(cols) == 1 and len(cols[0]) > 1:\n\n                \n\n                if cols[0][0:11] == \"Did not bat\":\n\n                     txt = cols[0]\n\n                     dnb = txt.split(\": \")\n\n                     \n\n                     dnb2 = dnb[1].split(\",\\xa0\")\n\n                     \n\n                     for cols in dnb2:\n\n                        batsmen_df = pd.concat([batsmen_df, pd.DataFrame([pd.Series(\n\n                        [re.sub(r\"\\W+\", ' ', cols), \"\", \n\n                        0, 0, 0, 0, 0, i+1], \n\n                        index=batsmen_df.columns )])], ignore_index=True)\n\n           \n\n   \n\n    return batsmen_df\n\ndef extract_bowling_data(series_id, match_id):\n\n\n\n    URL = 'https://www.espncricinfo.com/series/indian-premier-league-2023-'+ str(series_id) + '/sunrisers-hyderabad-vs-kolkata-knight-riders-47th-match-' + str(match_id) + '/full-scorecard'\n\n    page = requests.get(URL)\n\n    bs = BeautifulSoup(page.content, 'lxml')\n\n\n\n    table_body=bs.find_all('tbody')\n\n    bowler_df = pd.DataFrame(columns=['Name', 'Overs', 'Maidens', 'Runs', 'Wickets',\n\n                                      'Econ', 'Dots', '4s', '6s', 'Wd', 'Nb','Team'])\n\n    for i, table in enumerate(table_body[1:4:2]):\n\n        rows = table.find_all('tr')\n\n        for row in rows[::1]:\n\n            cols=row.find_all('td')\n\n            cols=[x.text.strip() for x in cols]\n\n            if len(cols[0]) > 50:\n\n                continue\n\n            elif len(cols) >= 11:\n\n                bowler_df = pd.concat([bowler_df, pd.DataFrame([pd.Series([cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], \n\n                                                    cols[6], cols[7], cols[8], cols[9], cols[10], (i==0)+1], \n\n                                                   index=bowler_df.columns )])], ignore_index=True)\n\n            \n\n    return(bowler_df)\ndat = in0.toPandas()\n\nmatch_id = dat['MatchID'].iloc[0]\n\ngame = dat['GameLink'].iloc[0]\n\nAlteryx.write(extract_batting_data(game, 1345038, match_id),1)\nAlteryx.write(extract_bowling_data(1345038, match_id),2)"
        ),
        output_ports = 2,
        is_custom_output_schema = True
    )
    ipl_python_web_scraping__formula_74_1 = Process(
        name = "IPL_Python_web_scraping__Formula_74_1",
        properties = ModelTransform(modelName = "IPL_Python_web_scraping__Formula_74_1")
    )
    textinput_63 = Process(
        name = "TextInput_63",
        properties = Dataset(
          writeOptions = {"writeMode" : "overwrite"},
          table = Dataset.DBTSource(name = "seed_63", sourceType = "Seed")
        ),
        input_ports = None
    )
    ipl_python_web_scraping__formula_54_0 = Process(
        name = "IPL_Python_web_scraping__Formula_54_0",
        properties = ModelTransform(modelName = "IPL_Python_web_scraping__Formula_54_0"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4", "in_5", "in_6", "in_7", "in_8"]
    )
    (
        jupytercode_1._out(1)
        >> [ipl_python_web_scraping__formula_54_0._in(6), ipl_python_web_scraping__formula_54_0._in(7),
              ipl_python_web_scraping__formula_54_0._in(8)]
    )
    textinput_63 >> ipl_python_web_scraping__formula_74_1
    ipl_python_web_scraping__formula_74_1 >> jupytercode_1
    ipl_python_web_scraping__formula_54_0 >> iplfantasystats_80
    (
        jupytercode_1._out(0)
        >> [ipl_python_web_scraping__formula_54_0._in(0), ipl_python_web_scraping__formula_54_0._in(1),
              ipl_python_web_scraping__formula_54_0._in(2)]
    )
