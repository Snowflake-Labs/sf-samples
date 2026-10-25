@@tables
    segments as $ns.SEGMENT primary key (ID)
        comment = 'Overture road, rail and water segments. Not a routing graph. Very large: filter BBOX columns first.'
@@facts
    segments.geometry as GEOM
        comment = 'LineString GEOGRAPHY. Apply only after the BBOX filters.',
    segments.length_km as LENGTH_KM,
    segments.speed_limit_kmh as SPEED_LIMIT_KMH
        comment = 'First unconditional posted maximum in km/h; NULL when absent or conditional.',
    segments.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    segments.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    segments.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    segments.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
@@dimensions
    segments.segment_id as ID comment = 'Include this ID when returning one line per row.',
    segments.name as NAME comment = 'Primary street or route name. Match with ILIKE.',
    segments.subtype as SUBTYPE comment = 'road, rail or water.',
    segments.road_class as CLASS
        comment = 'motorway, trunk, primary, secondary, tertiary, residential, service, footway...',
    segments.subclass as SUBCLASS comment = 'Refines class, e.g. sidewalk or link.'
@@metrics
    segments.segment_count as count(segments.ID),
    segments.total_length_km as sum(segments.LENGTH_KM),
    segments.avg_speed_limit_kmh as avg(segments.SPEED_LIMIT_KMH)
        comment = 'Only segments with an unconditional posted limit; state that coverage.',
    segments.geojson as any_value(segments.GEOJSON)
        comment = 'LineString map shape. Group by segment_id, never name alone.'
