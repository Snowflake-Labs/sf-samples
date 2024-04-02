CREATE OR REPLACE FUNCTION PY_COLLECTION_TO_POLYGONS(geo geography)
returns geography
language python
runtime_version = 3.8
packages = ('shapely')
handler = 'udf'
AS $$
from shapely import wkt
from shapely.ops import unary_union
from shapely.validation import make_valid
from shapely.geometry import shape, mapping
def udf(geo):
    if not geo:
        return None
    try:
        collection = shape(geo)
        collection = make_valid(collection) if not collection.is_valid else collection

        # Filter out non-Polygon/MultiPolygon geometries and flatten nested GeometryCollections
        def flatten_geometries(geometry):
            if geometry.geom_type in ["Polygon", "MultiPolygon"]:
                return [geometry]
            elif geometry.geom_type == "GeometryCollection":
                return [sub_geom for geo in geometry.geoms for sub_geom in flatten_geometries(geo)]
            else:
                return []
        geometries_checked = flatten_geometries(collection)       
        if not geometries_checked:
            return None
        dest_shape = unary_union(geometries_checked)
        valid_shape = make_valid(dest_shape) if not dest_shape.is_valid else dest_shape        
        return mapping(valid_shape)
    except Exception as e:
        # Logging the exception 'e' for debugging purposes
        return e
$$;

select ST_COLLECTION_TO_POLYGONS(to_geography('GEOMETRYCOLLECTION(POINT(-91.72073364257811 41.77182378456081), 
                                              POINT(-96.57995223999022 37.66099365286695),POLYGON((-86.36249542236327 40.02077884854819,-84.54580307006836 36.46298689866559,
                                              -90.70569992065428 36.01300517087813,-86.36249542236327 40.02077884854819)))'))