{%- set ns = q(database ~ '.' ~ schema) -%}

{#- Recorded release and coverage. A view over deploy-time values, so a new
    release or coverage is a definition change that the next plan shows. -#}
DEFINE VIEW {{ ns }}.DATASET_INFO comment = {{ lit(owner_comment) }} as
select {{ lit(release) }} as release, {{ lit(coverage) }} as coverage,
       {{ lit(source_mode) }} as source_mode;
