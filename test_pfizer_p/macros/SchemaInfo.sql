
{% macro SchemaInfo(relation_name) %}

with sample as (

    select to_json(struct(*)) as json_row
    from {{ relation_name }}
    limit 1

),

schema_json as (

    select schema_of_json(json_row) as schema_str
    from sample
),

parsed as (
    select from_json(
        schema_str,
        'struct<fields:array<struct<
            name:string,
            type:string,
            nullable:boolean,
            metadata:map<string,string>
        >>>'
    ) as s
    from schema_json
),

exploded as (
    select explode(s.fields) as f
    from parsed
)

select
    f.name                                       as Name,
    f.type                                       as variableType,
    cast(null as int)                            as Size,
    f.metadata['description']                   as Description,
    cast(null as string)                        as Source,
    cast(null as int)                            as Scale
from exploded

{% endmacro %}
