@@tables
    buildings as $ns.BUILDING primary key (ID)
        comment = 'Overture building footprints. Largest theme: filter all four BBOX columns first.'
@@facts
    buildings.geometry as GEOM
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
@@dimensions
    buildings.building_id as ID comment = 'Include this ID when returning one footprint per row.',
    buildings.name as NAME,
    buildings.subtype as SUBTYPE
        comment = 'residential, commercial, industrial... Mostly NULL; never treat NULL as absence.',
    buildings.building_class as CLASS
@@metrics
    buildings.building_count as count(buildings.ID),
    buildings.total_footprint_sqm as sum(buildings.FOOTPRINT_SQM),
    buildings.max_height_m as max(buildings.HEIGHT_M),
    buildings.geojson as any_value(buildings.GEOJSON)
        comment = 'Footprint map shape. Group by building_id, never name alone.'
