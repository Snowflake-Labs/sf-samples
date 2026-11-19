{%- set ns = q(database ~ '.' ~ schema) -%}

{#- Category vocabulary for CATEGORY_SEARCH, which post-deploy creates on top of
    this table (DCM cannot define search services). A full refresh rescans
    PLACE, so the lag is long; the vocabulary changes only with a new release. -#}
DEFINE DYNAMIC TABLE {{ ns }}.CATEGORY_VOCAB
WAREHOUSE = {{ q(warehouse) }}
TARGET_LAG = {{ lit(vocab_target_lag) }}
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
COMMENT = {{ lit(owner_comment) }}
AS
select category, count(*) as place_count,
       replace(category, '_', ' ') || ' (Overture taxonomy primary tag: ' || category || ')'
           as search_text
from {{ ns }}.PLACE
where category is not null
group by category;
