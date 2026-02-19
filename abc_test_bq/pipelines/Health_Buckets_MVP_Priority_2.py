from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(
    label = "Health_Buckets_MVP_Priority_2",
    version = 1,
    auto_layout = False,
    params = Parameters(workflow_name = "'Health_Buckets_MVP_Priority_2'")
)

with Pipeline(args) as pipeline:
    health_buckets_mvp_priority_2__sort_153 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sort_153",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sort_153")
    )
    health_buckets_mvp_priority_2__sample_142 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sample_142",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sample_142")
    )
    health_buckets_mvp_priority_2__sort_135 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sort_135",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sort_135"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    generaterows_128 = Process(
        name = "GenerateRows_128",
        properties = Script(
          scriptMethodHeader = "def Script(spark: SparkSession, in0: DataFrame) -> DataFrame:",
          scriptMethodFooter = "return out0",
          script = "\"\"\"\nAlteryx Generate Rows Tool - Python/Pandas Implementation\n\nReplicates the Alteryx Generate Rows tool which generates new rows \nbased on initialization, condition, and loop expressions.\n\nBehavior:\n- If no incoming dataframe: Acts as a sequence generator\n- If incoming dataframe: Executes loop for EACH input row (row-level processing)\n\"\"\"\n\nimport pandas as pd\nfrom typing import Optional, Any, Dict, Callable\n\n\n# =============================================================================\n# ALTERYX-COMPATIBLE EXPRESSION FUNCTIONS (Alteryx-specific)\n# =============================================================================\n\nclass AlteryxExpressionFunctions:\n    \"\"\"\n    Alteryx-compatible expression functions for use in eval().\n    \n    This class provides all the functions available in Alteryx expressions:\n    - DateTime: DateTimeAdd, DateTimeDiff, DateTimeToday, etc.\n    - Math: Abs, Ceil, Floor, Round, Sqrt, etc.\n    - String: Length, Upper, Lower, Left, Right, etc.\n    - Conversion: ToNumber, ToString, ToInteger\n    - Constants: True, False, None, Null\n    \n    Reference: https://help.alteryx.com/current/en/designer/functions.html\n    \"\"\"\n    \n    def __init__(self):\n        self._functions = self._build_all_functions()\n    \n    def _get_datetime_functions(self) -> Dict[str, Callable]:\n        \"\"\"DateTime functions for expression evaluation.\"\"\"\n        \n        def DateTimeAdd(dt, interval: int, unit: str):\n            \"\"\"Add interval to datetime. Alteryx-compatible.\"\"\"\n            unit = unit.lower().rstrip('s')\n            unit_map = {\n                'day': 'days', 'hour': 'hours', 'minute': 'minutes',\n                'second': 'seconds', 'week': 'weeks', 'month': 'months', 'year': 'years',\n            }\n            pd_unit = unit_map.get(unit, unit + 's')\n            if pd_unit in ['months', 'years']:\n                return dt + pd.DateOffset(**{pd_unit: interval})\n            return dt + pd.Timedelta(**{pd_unit: interval})\n        \n        def DateTimeDiff(dt1, dt2, unit: str) -> float:\n            \"\"\"Difference between two datetimes. Returns dt1 - dt2.\"\"\"\n            unit = unit.lower().rstrip('s')\n            diff = pd.Timestamp(dt1) - pd.Timestamp(dt2)\n            unit_divisors = {\n                'day': lambda d: d.days,\n                'hour': lambda d: d.total_seconds() / 3600,\n                'minute': lambda d: d.total_seconds() / 60,\n                'second': lambda d: d.total_seconds(),\n                'week': lambda d: d.days / 7,\n            }\n            return unit_divisors.get(unit, lambda d: d.days)(diff)\n        \n        return {\n            \"DateTimeAdd\": DateTimeAdd,\n            \"DateTimeDiff\": DateTimeDiff,\n            \"DateTimeToday\": lambda: pd.Timestamp.today().normalize(),\n            \"DateTimeNow\": lambda: pd.Timestamp.now(),\n            \"DateTimeYear\": lambda dt: pd.Timestamp(dt).year,\n            \"DateTimeMonth\": lambda dt: pd.Timestamp(dt).month,\n            \"DateTimeDay\": lambda dt: pd.Timestamp(dt).day,\n            \"Timedelta\": pd.Timedelta,\n            \"Timestamp\": pd.Timestamp,\n        }\n    \n    def _get_math_functions(self) -> Dict[str, Callable]:\n        \"\"\"Math functions for expression evaluation.\"\"\"\n        import math\n        return {\n            \"Abs\": abs,\n            \"Ceil\": math.ceil,\n            \"Floor\": math.floor,\n            \"Round\": round,\n            \"Pow\": pow,\n            \"Mod\": lambda x, y: x % y,\n            \"Sqrt\": math.sqrt,\n            \"Log\": math.log,\n            \"Log10\": math.log10,\n            \"Exp\": math.exp,\n            \"PI\": math.pi,\n        }\n    \n    def _get_string_functions(self) -> Dict[str, Callable]:\n        \"\"\"String functions for expression evaluation.\"\"\"\n        return {\n            \"Length\": len,\n            \"len\": len,\n            \"Upper\": lambda s: str(s).upper(),\n            \"Lower\": lambda s: str(s).lower(),\n            \"Trim\": lambda s: str(s).strip(),\n            \"Left\": lambda s, n: str(s)[:n],\n            \"Right\": lambda s, n: str(s)[-n:],\n            \"Substring\": lambda s, start, length: str(s)[start:start+length],\n            \"Contains\": lambda s, sub: sub in str(s),\n        }\n    \n    def _get_conversion_functions(self) -> Dict[str, Callable]:\n        \"\"\"Type conversion functions for expression evaluation.\"\"\"\n        return {\n            \"ToNumber\": float,\n            \"ToString\": str,\n            \"ToInteger\": int,\n            \"int\": int,\n            \"float\": float,\n            \"str\": str,\n        }\n    \n    def _get_constants(self) -> Dict[str, Any]:\n        \"\"\"Constants for expression evaluation.\"\"\"\n        return {\n            \"True\": True,\n            \"False\": False,\n            \"None\": None,\n            \"Null\": None,\n        }\n    \n    def _build_all_functions(self) -> Dict[str, Any]:\n        \"\"\"Build complete dictionary of all available functions.\"\"\"\n        functions = {\"__builtins__\": {}}\n        functions.update(self._get_datetime_functions())\n        functions.update(self._get_math_functions())\n        functions.update(self._get_string_functions())\n        functions.update(self._get_conversion_functions())\n        functions.update(self._get_constants())\n        return functions\n    \n    def get_functions(self) -> Dict[str, Any]:\n        \"\"\"Return the functions dictionary for use in eval().\"\"\"\n        return self._functions\n    \n    def get_available_function_names(self) -> list:\n        \"\"\"Return list of available function names.\"\"\"\n        return [k for k in self._functions.keys() if k != \"__builtins__\"]\n    \n    def eval_expression(self, expr: str, context: dict) -> Any:\n        \"\"\"\n        Evaluate a string expression with given context variables.\n        \n        Parameters\n        ----------\n        expr : str\n            Expression to evaluate (e.g., \"RowCount + 1\", \"date <= end_date\")\n        context : dict\n            Variables available in the expression\n        \n        Returns\n        -------\n        Any\n            Result of the expression evaluation.\n        \"\"\"\n        return eval(expr, self._functions, context)\n\n\n# =============================================================================\n# GENERATE ROWS CLASS (Generic logic)\n# =============================================================================\n\nclass GenerateRows:\n    \"\"\"\n    Generate Rows tool implementation.\n    \n    This class handles the row generation logic and can work with\n    different expression function providers (default: AlteryxExpressionFunctions).\n    \n    Parameters\n    ----------\n    expression_functions : object, optional\n        An object that provides eval_expression(expr, context) method.\n        Defaults to AlteryxExpressionFunctions().\n    \n    Example\n    -------\n    >>> generator = GenerateRows()\n    >>> result = generator.generate(\n    ...     output_column_name=\"RowCount\",\n    ...     init_expr=1,\n    ...     condition_expr=\"RowCount <= 10\",\n    ...     loop_expr=\"RowCount + 1\"\n    ... )\n    \"\"\"\n    \n    def __init__(self, expression_functions=None):\n        \"\"\"Initialize with expression functions provider.\"\"\"\n        if expression_functions is None:\n            expression_functions = AlteryxExpressionFunctions()\n        self._expr_funcs = expression_functions\n    \n    def _validate(\n        self,\n        output_column_name: str,\n        init_expr: Any,\n        condition_expr: str,\n        loop_expr: str\n    ) -> None:\n        \"\"\"\n        Validate mandatory parameters.\n        \n        Raises\n        ------\n        ValueError\n            If any mandatory parameter is missing or None.\n        \"\"\"\n        if output_column_name is None or output_column_name == \"\":\n            raise ValueError(\"output_column_name is mandatory and cannot be empty\")\n        \n        if init_expr is None:\n            raise ValueError(\"init_expr is mandatory\")\n        \n        if condition_expr is None or condition_expr == \"\":\n            raise ValueError(\"condition_expr is mandatory and cannot be empty\")\n        \n        if loop_expr is None or loop_expr == \"\":\n            raise ValueError(\"loop_expr is mandatory and cannot be empty\")\n    \n    def _eval_expr(self, expr: str, context: dict) -> Any:\n        \"\"\"Evaluate expression using the expression functions provider.\"\"\"\n        return self._expr_funcs.eval_expression(expr, context)\n    \n    def generate(\n        self,\n        df: Optional[pd.DataFrame] = None,\n        output_column_name: str = None,\n        init_expr: Any = None,\n        condition_expr: str = None,\n        loop_expr: str = None,\n        max_iterations: int = 100000\n    ) -> pd.DataFrame:\n        \"\"\"\n        Generate rows based on initialization, condition, and loop expressions.\n        \n        Parameters\n        ----------\n        df : pd.DataFrame, optional\n            Input dataframe. If None, generates rows as a standalone sequence.\n            If provided, generates rows for EACH input record.\n        \n        output_column_name : str (mandatory)\n            Name of the output column / loop variable (e.g., \"RowCount\", \"date\")\n        \n        init_expr : Any (mandatory)\n            Initialization expression. Can be:\n            - Direct value: 1, pd.Timestamp('2024-01-01')\n            - String expression: \"start_date\", \"DateTimeAdd(start_date, 1, 'day')\"\n        \n        condition_expr : str (mandatory)\n            Condition expression (loop continues while True).\n            Example: \"RowCount <= 10\", \"date <= end_date\"\n        \n        loop_expr : str (mandatory)\n            Loop/increment expression.\n            Example: \"RowCount + 1\", \"DateTimeAdd(date, 1, 'day')\"\n        \n        max_iterations : int, default 100000\n            Safety limit to prevent infinite loops.\n        \n        Returns\n        -------\n        pd.DataFrame\n            DataFrame with generated rows.\n        \"\"\"\n        # Validate mandatory parameters\n        self._validate(output_column_name, init_expr, condition_expr, loop_expr)\n        \n        output_rows = []\n        \n        if df is None or len(df) == 0:\n            # No-input mode: Sequence generator\n            current_value = init_expr\n            iteration = 0\n            \n            while iteration < max_iterations:\n                context = {output_column_name: current_value}\n                \n                if not self._eval_expr(condition_expr, context):\n                    break\n                \n                output_rows.append({output_column_name: current_value})\n                current_value = self._eval_expr(loop_expr, context)\n                iteration += 1\n            \n            if iteration >= max_iterations:\n                print(f\"Warning: Reached max_iterations ({max_iterations}). Loop terminated.\")\n        \n        else:\n            # Row-level processing mode: Loop for EACH input row\n            for idx, row in df.iterrows():\n                row_dict = row.to_dict()\n                \n                # Evaluate init_expr (can be a value, column reference, or expression)\n                if isinstance(init_expr, str):\n                    current_value = self._eval_expr(init_expr, row_dict)\n                else:\n                    current_value = init_expr\n                \n                iteration = 0\n                \n                while iteration < max_iterations:\n                    context = {**row_dict, output_column_name: current_value}\n                    \n                    if not self._eval_expr(condition_expr, context):\n                        break\n                    \n                    new_row = row_dict.copy()\n                    new_row[output_column_name] = current_value\n                    output_rows.append(new_row)\n                    \n                    current_value = self._eval_expr(loop_expr, context)\n                    iteration += 1\n                \n                if iteration >= max_iterations:\n                    print(f\"Warning: Reached max_iterations ({max_iterations}) for row {idx}.\")\n        \n        # Return result dataframe\n        if len(output_rows) == 0:\n            if df is not None:\n                return pd.DataFrame(columns=list(df.columns) + [output_column_name])\n            else:\n                return pd.DataFrame(columns=[output_column_name])\n        \n        return pd.DataFrame(output_rows)\n\n\n# =============================================================================\n# CONVENIENCE FUNCTION (for simple usage)\n# =============================================================================\n\n# Default instance\n_default_generator = GenerateRows()\n\n\ndef generate_rows(\n    df: Optional[pd.DataFrame] = None,\n    output_column_name: str = None,\n    init_expr: Any = None,\n    condition_expr: str = None,\n    loop_expr: str = None,\n    max_iterations: int = 100000\n) -> pd.DataFrame:\n    \"\"\"\n    Convenience function using default GenerateRows instance.\n    \n    See GenerateRows.generate() for full documentation.\n    \"\"\"\n    return _default_generator.generate(\n        df=df,\n        output_column_name=output_column_name,\n        init_expr=init_expr,\n        condition_expr=condition_expr,\n        loop_expr=loop_expr,\n        max_iterations=max_iterations\n    )\n\nout0 = generate_rows(\n            df=in0,\n            output_column_name=\"previous\",\n            init_expr=\"payload.RecordNum\",\n            condition_expr=\"(previous > (payload.RecordNum - payload.NumRows))\",\n            loop_expr=\"(previous - 1)\"\n)\n"
        ),
        is_custom_output_schema = True
    )
    health_buckets_mvp_priority_2__formula_159_0 = Process(
        name = "Health_Buckets_MVP_Priority_2__Formula_159_0",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Formula_159_0"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    health_buckets_mvp_priority_2__multirowformula_129_row_id_drop_0 = Process(
        name = "Health_Buckets_MVP_Priority_2__MultiRowFormula_129_row_id_drop_0",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__MultiRowFormula_129_row_id_drop_0")
    )
    health_buckets_mvp_priority_2__summarize_104 = Process(
        name = "Health_Buckets_MVP_Priority_2__Summarize_104",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Summarize_104")
    )
    health_buckets_mvp_priority_2__sort_141 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sort_141",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sort_141"),
        input_ports = ["in_0", "in_1", "in_2"]
    )
    health_buckets_mvp_priority_2__summarize_174 = Process(
        name = "Health_Buckets_MVP_Priority_2__Summarize_174",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Summarize_174")
    )
    health_buckets_mvp_priority_2__filter_99 = Process(
        name = "Health_Buckets_MVP_Priority_2__Filter_99",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Filter_99")
    )
    health_buckets_mvp_priority_2__join_146_inner_formula_to_formula_148_1 = Process(
        name = "Health_Buckets_MVP_Priority_2__Join_146_inner_formula_to_Formula_148_1",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Join_146_inner_formula_to_Formula_148_1")
    )
    health_buckets_mvp_priority_2__summarize_188 = Process(
        name = "Health_Buckets_MVP_Priority_2__Summarize_188",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Summarize_188")
    )
    health_buckets_mvp_priority_2__formula_127_0 = Process(
        name = "Health_Buckets_MVP_Priority_2__Formula_127_0",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Formula_127_0")
    )
    health_buckets_mvp_priority_2__join_155_inner = Process(
        name = "Health_Buckets_MVP_Priority_2__Join_155_inner",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Join_155_inner"),
        input_ports = ["in_0", "in_1"]
    )
    health_buckets_mvp_priority_2__summarize_173 = Process(
        name = "Health_Buckets_MVP_Priority_2__Summarize_173",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Summarize_173")
    )
    health_buckets_mvp_priority_2__crosstab_151 = Process(
        name = "Health_Buckets_MVP_Priority_2__CrossTab_151",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__CrossTab_151"),
        input_ports = ["in_0", "in_1"]
    )
    health_buckets_mvp_priority_2__summarize_190 = Process(
        name = "Health_Buckets_MVP_Priority_2__Summarize_190",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Summarize_190")
    )
    health_buckets_mvp_priority_2__filter_99_reject = Process(
        name = "Health_Buckets_MVP_Priority_2__Filter_99_reject",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Filter_99_reject")
    )
    health_buckets_mvp_priority_2__health_bucket_c_118 = Process(
        name = "Health_Buckets_MVP_Priority_2__Health_Bucket_c_118",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Health_Bucket_c_118"),
        input_ports = ["in_0", "in_1"]
    )
    health_buckets_mvp_priority_2__sort_125 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sort_125",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sort_125"),
        input_ports = ["in_0", "in_1", "in_2", "in_3", "in_4"]
    )
    health_buckets_mvp_priority_2__sample_143 = Process(
        name = "Health_Buckets_MVP_Priority_2__Sample_143",
        properties = ModelTransform(modelName = "Health_Buckets_MVP_Priority_2__Sample_143")
    )
    (
        health_buckets_mvp_priority_2__sort_135._out(0)
        >> [health_buckets_mvp_priority_2__sample_142._in(0), health_buckets_mvp_priority_2__sample_143._in(0)]
    )
    (
        health_buckets_mvp_priority_2__filter_99._out(0)
        >> [health_buckets_mvp_priority_2__summarize_104._in(0), health_buckets_mvp_priority_2__sort_141._in(0),
              health_buckets_mvp_priority_2__multirowformula_129_row_id_drop_0._in(0)]
    )
    (
        health_buckets_mvp_priority_2__multirowformula_129_row_id_drop_0._out(0)
        >> [health_buckets_mvp_priority_2__sort_135._in(1), health_buckets_mvp_priority_2__formula_127_0._in(0)]
    )
    (
        health_buckets_mvp_priority_2__sample_142._out(0)
        >> [health_buckets_mvp_priority_2__formula_159_0._in(1), health_buckets_mvp_priority_2__join_155_inner._in(0)]
    )
    (
        health_buckets_mvp_priority_2__summarize_173._out(0)
        >> [health_buckets_mvp_priority_2__sort_125._in(1), health_buckets_mvp_priority_2__summarize_174._in(0)]
    )
    generaterows_128 >> health_buckets_mvp_priority_2__sort_135._in(0)
    (
        health_buckets_mvp_priority_2__join_146_inner_formula_to_formula_148_1._out(0)
        >> [health_buckets_mvp_priority_2__crosstab_151._in(0),
              health_buckets_mvp_priority_2__health_bucket_c_118._in(0),
              health_buckets_mvp_priority_2__sort_153._in(0),
              health_buckets_mvp_priority_2__summarize_188._in(0)]
    )
    (
        health_buckets_mvp_priority_2__formula_159_0._out(0)
        >> [health_buckets_mvp_priority_2__crosstab_151._in(1),
              health_buckets_mvp_priority_2__health_bucket_c_118._in(1)]
    )
    (
        health_buckets_mvp_priority_2__sample_143._out(0)
        >> [health_buckets_mvp_priority_2__formula_159_0._in(2), health_buckets_mvp_priority_2__sort_141._in(2),
              health_buckets_mvp_priority_2__join_155_inner._in(1),
              health_buckets_mvp_priority_2__join_146_inner_formula_to_formula_148_1._in(0)]
    )
    (
        health_buckets_mvp_priority_2__filter_99_reject._out(0)
        >> [health_buckets_mvp_priority_2__formula_159_0._in(0), health_buckets_mvp_priority_2__summarize_190._in(0)]
    )
    (
        health_buckets_mvp_priority_2__sort_125._out(0)
        >> [health_buckets_mvp_priority_2__filter_99._in(0), health_buckets_mvp_priority_2__filter_99_reject._in(0)]
    )
    health_buckets_mvp_priority_2__formula_127_0 >> generaterows_128
