@@tables
    addresses as $ns.ADDRESS primary key (ID)
        comment = 'Overture address points. Very large: filter all four BBOX columns first.'
@@facts
    addresses.geometry as GEOM
        comment = 'Point GEOGRAPHY. Apply only after the BBOX filters.',
    addresses.latitude as LATITUDE,
    addresses.longitude as LONGITUDE,
    addresses.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    addresses.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    addresses.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    addresses.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
@@dimensions
    addresses.address_id as ID,
    addresses.street as STREET comment = 'Street name as published. Match with ILIKE.',
    addresses.house_number as HOUSE_NUMBER,
    addresses.postcode as POSTCODE,
    addresses.city as CITY comment = 'Postal city, else the most local address level.',
    addresses.country as COUNTRY comment = 'ISO 3166-1 alpha-2 code, e.g. US.'
@@metrics
    addresses.address_count as count(addresses.ID),
    addresses.map_latitude as any_value(addresses.LATITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.',
    addresses.map_longitude as any_value(addresses.LONGITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.'
