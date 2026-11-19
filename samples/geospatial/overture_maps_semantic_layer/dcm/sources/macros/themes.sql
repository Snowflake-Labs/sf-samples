{#- Semantic-view fragments for the optional themes, in four sections. -#}
{% macro theme_section(theme, section, ns) -%}
{%- if theme == 'address' and section == 'tables' %}    addresses as {{ ns }}.ADDRESS primary key (ID)
        comment = 'Overture address points. Very large: filter all four BBOX columns first.'
{%- elif theme == 'address' and section == 'facts' %}    addresses.geometry as GEOM
        comment = 'Point GEOGRAPHY. Apply only after the BBOX filters.',
    addresses.latitude as LATITUDE,
    addresses.longitude as LONGITUDE,
    addresses.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    addresses.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    addresses.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    addresses.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
{%- elif theme == 'address' and section == 'dimensions' %}    addresses.address_id as ID,
    addresses.street as STREET comment = 'Street name as published. Match with ILIKE.',
    addresses.house_number as HOUSE_NUMBER,
    addresses.postcode as POSTCODE,
    addresses.city as CITY comment = 'Postal city, else the most local address level.',
    addresses.country as COUNTRY comment = 'ISO 3166-1 alpha-2 code, e.g. US.'
{%- elif theme == 'address' and section == 'metrics' %}    addresses.address_count as count(addresses.ID),
    addresses.map_latitude as any_value(addresses.LATITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.',
    addresses.map_longitude as any_value(addresses.LONGITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.'
{%- elif theme == 'segment' and section == 'tables' %}    segments as {{ ns }}.SEGMENT primary key (ID)
        comment = 'Overture road, rail and water segments. Not a routing graph. Very large: filter BBOX columns first.'
{%- elif theme == 'segment' and section == 'facts' %}    segments.geometry as GEOM
        comment = 'LineString GEOGRAPHY. Apply only after the BBOX filters.',
    segments.length_km as LENGTH_KM,
    segments.speed_limit_kmh as SPEED_LIMIT_KMH
        comment = 'First unconditional posted maximum in km/h; NULL when absent or conditional.',
    segments.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    segments.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    segments.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    segments.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
{%- elif theme == 'segment' and section == 'dimensions' %}    segments.segment_id as ID comment = 'Include this ID when returning one line per row.',
    segments.name as NAME comment = 'Primary street or route name. Match with ILIKE.',
    segments.subtype as SUBTYPE comment = 'road, rail or water.',
    segments.road_class as CLASS
        comment = 'motorway, trunk, primary, secondary, tertiary, residential, service, footway...',
    segments.subclass as SUBCLASS comment = 'Refines class, e.g. sidewalk or link.'
{%- elif theme == 'segment' and section == 'metrics' %}    segments.segment_count as count(segments.ID),
    segments.total_length_km as sum(segments.LENGTH_KM),
    segments.avg_speed_limit_kmh as avg(segments.SPEED_LIMIT_KMH)
        comment = 'Only segments with an unconditional posted limit; state that coverage.',
    segments.geojson as any_value(segments.GEOJSON)
        comment = 'LineString map shape. Group by segment_id, never name alone.'
{%- elif theme == 'building' and section == 'tables' %}    buildings as {{ ns }}.BUILDING primary key (ID)
        comment = 'Overture building footprints. Largest theme: filter all four BBOX columns first.'
{%- elif theme == 'building' and section == 'facts' %}    buildings.geometry as GEOM
        comment = 'Polygon GEOGRAPHY. Apply only after the BBOX filters.',
    buildings.height_m as HEIGHT_M comment = 'Often NULL outside major cities.',
    buildings.num_floors as NUM_FLOORS comment = 'Often NULL.',
    buildings.footprint_sqm as FOOTPRINT_SQM,
    buildings.latitude as LATITUDE comment = 'Footprint centroid latitude.',
    buildings.longitude as LONGITUDE comment = 'Footprint centroid longitude.',
    buildings.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    buildings.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    buildings.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    buildings.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
{%- elif theme == 'building' and section == 'dimensions' %}    buildings.building_id as ID comment = 'Include this ID when returning one footprint per row.',
    buildings.name as NAME,
    buildings.subtype as SUBTYPE
        comment = 'residential, commercial, industrial... Mostly NULL; never treat NULL as absence.',
    buildings.building_class as CLASS
{%- elif theme == 'building' and section == 'metrics' %}    buildings.building_count as count(buildings.ID),
    buildings.total_footprint_sqm as sum(buildings.FOOTPRINT_SQM),
    buildings.max_height_m as max(buildings.HEIGHT_M),
    buildings.geojson as any_value(buildings.GEOJSON)
        comment = 'Footprint map shape. Group by building_id, never name alone.'
{%- endif %}
{%- endmacro %}
