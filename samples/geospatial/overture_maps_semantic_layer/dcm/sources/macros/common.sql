{#- Quote a validated dotted name, e.g. q('db.schema.t') -> "DB"."SCHEMA"."T".
    Mirrors identifier() in scripts/install.py, which validates every part. -#}
{% macro q(name) -%}
{%- for part in name.split('.') -%}"{{ part | upper }}"{{ "." if not loop.last }}{%- endfor -%}
{%- endmacro %}

{#- SQL string literal. Mirrors literal() in scripts/install.py. -#}
{% macro lit(value) -%}
'{{ value | replace("\\", "\\\\") | replace("'", "''") }}'
{%- endmacro %}
