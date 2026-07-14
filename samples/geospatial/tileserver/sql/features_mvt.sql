-- Martin-compatible MVT function source over public.features.
-- Signature fn(z,x,y integer) RETURNS bytea is auto-published by Martin at
-- /features_mvt/{z}/{x}/{y}. Encoding is native PostGIS ST_AsMVT.
--
-- Best practices baked in:
--   * zoom-based layering: only countries at low zoom; add regions from z>=4
--   * per-zoom geometry simplification (tolerance = tile resolution in metres)
--   * bbox filter uses the GiST index on geom

CREATE OR REPLACE FUNCTION public.features_mvt(z integer, x integer, y integer)
RETURNS bytea
AS $$
  WITH env AS (
    SELECT
      ST_TileEnvelope(z, x, y)                              AS geom_3857,
      -- metres per tile pixel at this zoom (tile extent 4096)
      (2 * 20037508.342789244) / (4096 * power(2, z))       AS tol
  )
  SELECT ST_AsMVT(t, 'features', 4096, 'geom')
  FROM (
    SELECT
      ST_AsMVTGeom(
        ST_SimplifyPreserveTopology(ST_Transform(f.geom, 3857), env.tol),
        env.geom_3857,
        4096, 64, true
      ) AS geom,
      f.id,
      f.division_id,
      f.name,
      f.subtype,
      f.country,
      f.admin_level
    FROM public.features f, env
    WHERE f.geom && ST_Transform(env.geom_3857, 4326)
      AND (z >= 4 OR f.subtype = 'country')   -- thin out low zooms
  ) AS t
  WHERE t.geom IS NOT NULL;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

COMMENT ON FUNCTION public.features_mvt(integer, integer, integer) IS
  '{"description":"Overture admin areas (countries + regions)","attribution":"Overture Maps","vector_layers":[{"id":"features","fields":{"name":"string","subtype":"string","country":"string","admin_level":"number"}}]}';
