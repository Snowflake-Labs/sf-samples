{%- set ns = q(database ~ '.' ~ schema) -%}

DEFINE SCHEMA {{ ns }} comment = {{ lit(owner_comment) }};
