/*
 * Copyright 2026 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Standalone Java UDF to repair invalid GEOGRAPHY shapes using gnomonic projection.
 * Core logic reused from jts-udf JTS.makevalid_geography, with robustness and
 * post-repair validation improvements.
 */

package com.snowflake.geo;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.Gnomonic;
import net.sf.geographiclib.GnomonicData;

public class MakeValid {

    private static final GeometryFactory GEOM_FACTORY = new GeometryFactory();

    // GeographicLib objects are immutable / thread-safe; cache as static.
    private static final Geodesic WGS84 = Geodesic.WGS84;
    private static final Gnomonic GNOMONIC = new Gnomonic(WGS84);

    // JTS GeoJSON readers/writers are not documented thread-safe.
    private static final ThreadLocal<GeoJsonReader> TL_READER =
            ThreadLocal.withInitial(GeoJsonReader::new);
    private static final ThreadLocal<GeoJsonWriter> TL_WRITER =
            ThreadLocal.withInitial(() -> {
                GeoJsonWriter w = new GeoJsonWriter();
                w.setEncodeCRS(false);
                return w;
            });

    // Tolerances tuned for WGS84 degrees / gnomonic-plane meters.
    // Pre-simplify collapses duplicate vertices spaced < ~1e-9 deg (~0.1 mm).
    private static final double PRESIMPLIFY_TOL_DEG = 1e-9;
    // Default 1 mm grid on the gnomonic tangent plane removes sub-micron jitter
    // that re-introduces spherical self-intersections after reverse projection.
    // Callers can override via the 2-arg SQL overload.
    private static final double DEFAULT_GRID_METERS = 1e-3;
    // Gnomonic projection breaks down well before 90 deg from origin; be conservative.
    private static final double MAX_EXTENT_DEG = 90.0;

    // Public API -----------------------------------------------------------

    /**
     * Lenient handler used by the SQL UDF. Returns NULL on any failure so a
     * single bad row does not abort an entire SELECT.
     */
    public static String makeValidGeography(String strGeom) {
        try {
            return repair(strGeom, DEFAULT_GRID_METERS);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Lenient handler with a user-supplied grid size (meters on the gnomonic
     * tangent plane). Use a smaller value (e.g. 1e-6) to preserve fine detail
     * on small synthetic polygons; use a larger value (e.g. 1.0) to aggressively
     * snap jittery real-world data. gridMeters <= 0 disables snapping.
     */
    public static String makeValidGeography(String strGeom, double gridMeters) {
        try {
            return repair(strGeom, gridMeters);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Strict handler that propagates exceptions. Registered as a separate SQL
     * function (makevalid_strict) for debugging bad rows.
     */
    public static String makeValidGeographyStrict(String strGeom) throws Exception {
        return repair(strGeom, DEFAULT_GRID_METERS);
    }

    public static String makeValidGeographyStrict(String strGeom, double gridMeters) throws Exception {
        return repair(strGeom, gridMeters);
    }

    // Core -----------------------------------------------------------------

    private static String repair(String strGeom, double gridMeters) throws Exception {
        if (strGeom == null) return null;
        Geometry geom = TL_READER.get().read(strGeom);
        if (geom == null || geom.isEmpty()) return null;

        // 0D / 1D inputs cannot be "invalid" in the polygon-topology sense.
        if (geom.getDimension() < 2) {
            return validString(geom);
        }

        // Pre-simplify to drop near-duplicate vertices that cause micro self-touches.
        Geometry cleaned = DouglasPeuckerSimplifier.simplify(geom, PRESIMPLIFY_TOL_DEG);
        if (cleaned.isEmpty()) cleaned = geom;

        Envelope env = cleaned.getEnvelopeInternal();
        double width = env.getWidth();
        double height = env.getHeight();

        // Antimeridian-crossing polygons typically report huge widths.
        // Split at lon=180 before projecting.
        if (width > 180.0) {
            cleaned = splitAtAntimeridian(cleaned);
            env = cleaned.getEnvelopeInternal();
            width = env.getWidth();
            height = env.getHeight();
        }

        // If still too large for gnomonic, fall back to planar fix (better than throwing).
        if (width > MAX_EXTENT_DEG || height > MAX_EXTENT_DEG) {
            Geometry planar = GeometryFixer.fix(cleaned);
            return validString(planar);
        }

        // Use envelope center; more stable than area-weighted centroid for
        // disjoint MultiPolygons.
        final double lon0 = (env.getMinX() + env.getMaxX()) / 2.0;
        final double lat0 = (env.getMinY() + env.getMaxY()) / 2.0;

        // Forward-project: (lon,lat) -> (x,y) in meters on gnomonic tangent plane.
        Geometry projected = new GeometryTransformer() {
            @Override
            protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
                Coordinate[] out = new Coordinate[coords.size()];
                for (int i = 0; i < coords.size(); i++) {
                    Coordinate c = coords.getCoordinate(i);
                    GnomonicData p = GNOMONIC.Forward(lat0, lon0, c.y, c.x);
                    out[i] = new Coordinate(p.x, p.y);
                }
                return GEOM_FACTORY.getCoordinateSequenceFactory().create(out);
            }
        }.transform(cleaned);

        // Planar fix in the projected plane.
        Geometry fixed = GeometryFixer.fix(projected);

        // Snap to a user-configurable grid to eliminate floating-point jitter
        // in the gnomonic plane. gridMeters <= 0 skips snapping.
        Geometry snapped;
        if (gridMeters > 0) {
            PrecisionModel pm = new PrecisionModel(1.0 / gridMeters);
            snapped = GeometryPrecisionReducer.reduce(fixed, pm);
            if (snapped.isEmpty()) snapped = fixed;
        } else {
            snapped = fixed;
        }

        // Reverse-project: (x,y) -> (lon,lat).
        Geometry back = new GeometryTransformer() {
            @Override
            protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
                Coordinate[] out = new Coordinate[coords.size()];
                for (int i = 0; i < coords.size(); i++) {
                    Coordinate c = coords.getCoordinate(i);
                    GnomonicData p = GNOMONIC.Reverse(lat0, lon0, c.x, c.y);
                    out[i] = new Coordinate(p.lon, p.lat);
                }
                return GEOM_FACTORY.getCoordinateSequenceFactory().create(out);
            }
        }.transform(snapped);

        // Second planar fix pass catches any self-touches introduced by the
        // reverse projection's numeric rounding.
        Geometry finalGeom = GeometryFixer.fix(back);

        return validString(finalGeom);
    }

    // Helpers --------------------------------------------------------------

    private static String validString(Geometry geom) {
        if (geom == null || geom.isEmpty()) return null;
        if (geom.getDimension() == 2) {
            geom = GeometryFixer.fix(geom);
            geom.normalize();
        }
        return TL_WRITER.get().write(geom);
    }

    /**
     * Split a geometry whose bounding box spans > 180 deg of longitude at the
     * antimeridian using a planar intersection with two half-world strips.
     * Parts east of the antimeridian are shifted by -360 deg so all coordinates
     * live on a contiguous longitude strip before gnomonic projection.
     */
    private static Geometry splitAtAntimeridian(Geometry geom) {
        // Simple heuristic: shift any vertex with lon > 0 by -360 when the
        // envelope crosses the antimeridian. This puts the whole geometry on
        // a single continuous strip for projection. Round-tripping through
        // gnomonic + reverse will still emit valid WGS84 lon/lat because
        // Gnomonic.Reverse normalizes outputs.
        return new GeometryTransformer() {
            @Override
            protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
                Coordinate[] out = new Coordinate[coords.size()];
                for (int i = 0; i < coords.size(); i++) {
                    Coordinate c = coords.getCoordinate(i);
                    double lon = c.x > 0 ? c.x - 360.0 : c.x;
                    out[i] = new Coordinate(lon, c.y);
                }
                return GEOM_FACTORY.getCoordinateSequenceFactory().create(out);
            }
        }.transform(geom);
    }
}
