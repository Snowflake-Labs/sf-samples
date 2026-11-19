{%- set ns = q(database ~ '.' ~ schema) -%}
{%- set themes = ['place', 'division', 'division_area', 'division_boundary'] + optional_themes -%}

{#- Viewer grants on objects this project manages. Database and warehouse usage,
    and grants on the agent, search service and CoWork object, are post-deploy:
    those objects are shared with other projects or not managed by DCM, and a
    privilege-grantee pair can be defined in only one DCM project. -#}
{% if viewer_role | upper != role | upper %}
GRANT USAGE ON SCHEMA {{ ns }} TO ROLE {{ q(viewer_role) }};
{% for theme in themes %}
GRANT SELECT ON VIEW {{ ns }}.{{ theme | upper }} TO ROLE {{ q(viewer_role) }};
{% endfor %}
GRANT SELECT ON VIEW {{ ns }}.DATASET_INFO TO ROLE {{ q(viewer_role) }};
GRANT SELECT ON SEMANTIC VIEW {{ ns }}.OVERTURE_MAPS_SV TO ROLE {{ q(viewer_role) }};
{% endif %}
