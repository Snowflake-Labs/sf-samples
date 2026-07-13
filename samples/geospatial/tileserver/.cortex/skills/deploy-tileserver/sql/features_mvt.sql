-- Martin-compatible MVT function source over public.features.
-- Signature fn(z,x,y integer) RETURNS bytea is auto-published by Martin at
-- /features_mvt/{z}/{x}/{y}. Encoding is native PostGIS ST_AsMVT.
--
-- Best practices baked in:
--   * zoom-based layering: only the coarsest features at low zoom; finer from z>=4
--   * per-zoom geometry simplification (tolerance = tile resolution in metres)
--   * bbox filter uses the GiST index on geom
-- The low-zoom thinning is by admin_level (dataset-agnostic): it always keeps the
-- coarsest features present, so it works whether the dataset has countries,
-- regions/states, or any single subtype - never blank at z0.

CREATE OR REPLACE FUNCTION public.features_mvt(z integer, x integer, y integer)
RETURNS bytea
AS $$
  WITH env AS (
    SELECT
      ST_TileEnvelope(z, x, y)                              AS geom_3857,
      -- metres per tile pixel at this zoom (tile extent 4096)
      (2 * 20037508.342789244) / (4096 * power(2, z))       AS tol
  ),
  coarsest AS (
    SELECT COALESCE(min(admin_level), 2147483647) AS lvl FROM public.features
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
    FROM public.features f, env, coarsest c
    WHERE f.geom && ST_Transform(env.geom_3857, 4326)
      -- thin low zooms: keep only the coarsest admin level present (NULLs always shown)
      AND (z >= 4 OR f.admin_level IS NULL OR f.admin_level <= c.lvl)
  ) AS t
  WHERE t.geom IS NOT NULL;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

COMMENT ON FUNCTION public.features_mvt(integer, integer, integer) IS
  '{"description":"Admin areas (countries / regions / states)","attribution":"deploy-tileserver","vector_layers":[{"id":"features","fields":{"name":"string","subtype":"string","country":"string","admin_level":"number"}}]}';
