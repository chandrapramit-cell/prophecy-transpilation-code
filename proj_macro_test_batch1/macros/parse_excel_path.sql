{#
  parse_excel_path: Parses an Excel file path with sheet name from a combined string.
  Cross-dialect compatible: Databricks, Snowflake, BigQuery, DuckDB.

  Parameters:
    templatePath  - Template string like '/a/b/c.xlsx|||SheetName'
    pathColumn    - SQL column reference containing the dynamic value
    mode          - One of: 'path', 'fileName', 'suffix', 'prefix'

  Returns: STRUCT with fields 'file_path' and 'sheet_name'

  Modes:
    path     - pathColumn contains full '/x/y/z.xlsx|||Sheet'. Splits on '|||'.
    fileName - pathColumn contains a filename (e.g. 'data.xlsx').
               Uses directory + sheet from templatePath, filename from pathColumn.
    suffix   - pathColumn contains a suffix string (e.g. '_v2').
               Appends it to template filename before extension.
    prefix   - pathColumn contains a prefix string (e.g. 'new_').
               Prepends it to template filename.

  Usage:
    SELECT {{ parse_excel_path('/a/b/c.xlsx|||Sheet1', 'my_column', 'path') }} AS parsed
    -- then reference: parsed.file_path, parsed.sheet_name
#}

{#-- Helper: wraps two expressions into a dialect-appropriate struct --#}
{% macro _build_struct(file_path_expr, sheet_name_expr) %}
{%- if target.type == 'databricks' or target.type == 'spark' -%}
NAMED_STRUCT(
  'file_path', {{ file_path_expr }},
  'sheet_name', {{ sheet_name_expr }}
)
{%- elif target.type == 'snowflake' -%}
OBJECT_CONSTRUCT(
  'file_path', {{ file_path_expr }},
  'sheet_name', {{ sheet_name_expr }}
)
{%- elif target.type == 'bigquery' -%}
STRUCT(
  {{ file_path_expr }} AS file_path,
  {{ sheet_name_expr }} AS sheet_name
)
{%- elif target.type == 'duckdb' -%}
{'file_path': {{ file_path_expr }}, 'sheet_name': {{ sheet_name_expr }}}
{%- else -%}
NAMED_STRUCT(
  'file_path', {{ file_path_expr }},
  'sheet_name', {{ sheet_name_expr }}
)
{%- endif -%}
{% endmacro %}

{#-- Helper: dialect-appropriate LOCATE/STRPOS/INSTR --#}
{% macro _locate_delim(column) %}
{%- if target.type == 'bigquery' -%}
STRPOS({{ column }}, '|||')
{%- elif target.type == 'snowflake' -%}
POSITION('|||' IN {{ column }})
{%- else -%}
LOCATE('|||', {{ column }})
{%- endif -%}
{% endmacro %}

{% macro parse_excel_path(templatePath, pathColumn, mode) %}
{%- set parts = templatePath.split('|||') -%}
{%- set tpl_file = parts[0] -%}
{%- set tpl_sheet = parts[1] if parts | length > 1 else '' -%}
{%- set last_slash = tpl_file.rfind('/') -%}
{%- set tpl_dir = tpl_file[:last_slash + 1] if last_slash >= 0 else '' -%}
{%- set tpl_fullname = tpl_file[last_slash + 1:] if last_slash >= 0 else tpl_file -%}
{%- set last_dot = tpl_fullname.rfind('.') -%}
{%- set tpl_name = tpl_fullname[:last_dot] if last_dot >= 0 else tpl_fullname -%}
{%- set tpl_ext = tpl_fullname[last_dot:] if last_dot >= 0 else '' -%}

{%- if mode == 'path' -%}
{{ _build_struct(
    "SUBSTRING(" ~ pathColumn ~ ", 1, " ~ _locate_delim(pathColumn) ~ " - 1)",
    "SUBSTRING(" ~ pathColumn ~ ", " ~ _locate_delim(pathColumn) ~ " + 3)"
) }}
{%- elif mode == 'fileName' -%}
{{ _build_struct(
    "CONCAT('" ~ tpl_dir ~ "', " ~ pathColumn ~ ")",
    "'" ~ tpl_sheet ~ "'"
) }}
{%- elif mode == 'suffix' -%}
{{ _build_struct(
    "CONCAT('" ~ tpl_dir ~ "', '" ~ tpl_name ~ "', " ~ pathColumn ~ ", '" ~ tpl_ext ~ "')",
    "'" ~ tpl_sheet ~ "'"
) }}
{%- elif mode == 'prefix' -%}
{{ _build_struct(
    "CONCAT('" ~ tpl_dir ~ "', " ~ pathColumn ~ ", '" ~ tpl_name ~ "', '" ~ tpl_ext ~ "')",
    "'" ~ tpl_sheet ~ "'"
) }}
{%- endif -%}
{% endmacro %}
