-- ============================================================================
-- libpostal SPCS service — deployment (international / multi-country geocoding)
-- ============================================================================
-- Provisions the infrastructure to run libpostal (worldwide address parsing)
-- inside Snowpark Container Services and exposes it to SQL as service functions.
--
-- Run by the installer's --intl / --eu flags after the image is built and
-- pushed (see this folder's README.md, Build & push section, to do it by hand).
-- Safe to re-run: existing objects are kept and the service is moved to the
-- image tag in ./VERSION.
--
-- Prerequisites: ACCOUNTADMIN (or a role with CREATE COMPUTE POOL / SERVICE /
-- IMAGE REPOSITORY), and the GEOCODING database from ../sql/00_setup.sql.
-- ============================================================================

USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- ---------------------------------------------------------------------------
-- 1. Image repository (push the built image here)
-- ---------------------------------------------------------------------------
CREATE IMAGE REPOSITORY IF NOT EXISTS GEOCODING.PUBLIC.IMAGE_REPOSITORY
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
-- Get the repo URL for `docker push`:
SHOW IMAGE REPOSITORIES IN SCHEMA GEOCODING.PUBLIC;

-- ---------------------------------------------------------------------------
-- 2. Compute pool (libpostal needs ~2-4 GB RAM -> CPU_X64_S, not XS)
--    AUTO_SUSPEND keeps cost down when idle.
-- ---------------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS GEOCODING_LIBPOSTAL_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_S
  AUTO_SUSPEND_SECS = 600
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"spcs"}}';

-- ---------------------------------------------------------------------------
-- 3. Service (internal endpoint only; spec in ./service-spec.yaml)
--    Either inline the spec or upload service-spec.yaml to a stage and use
--    FROM SPECIFICATION_FILE. Inline version shown below.
-- ---------------------------------------------------------------------------
CREATE SERVICE IF NOT EXISTS GEOCODING.PUBLIC.LIBPOSTAL_SVC
  IN COMPUTE POOL GEOCODING_LIBPOSTAL_POOL
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1
  -- Suspend after 30 min without calls. Without this the service never idles,
  -- so the pool cannot suspend either and bills around the clock. A service
  -- function call resumes it (AUTO_RESUME is on by default).
  AUTO_SUSPEND_SECS = 1800
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"spcs"}}'
  FROM SPECIFICATION $$
spec:
  containers:
    - name: libpostal
      image: /GEOCODING/PUBLIC/IMAGE_REPOSITORY/libpostal:1.0.0
      readinessProbe:
        port: 8080
        path: /healthcheck
      resources:
        requests:
          memory: 2Gi
          cpu: 1000m
        limits:
          memory: 4Gi
          cpu: 2000m
  endpoints:
    - name: api
      port: 8080
      public: false
$$;

-- The image tag comes from ./VERSION (the installer checks they match). CREATE
-- ... IF NOT EXISTS leaves an existing service on its old image, so re-apply
-- the spec to move it to this tag. Harmless on a first install.
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC FROM SPECIFICATION $$
spec:
  containers:
    - name: libpostal
      image: /GEOCODING/PUBLIC/IMAGE_REPOSITORY/libpostal:1.0.0
      readinessProbe:
        port: 8080
        path: /healthcheck
      resources:
        requests:
          memory: 2Gi
          cpu: 1000m
        limits:
          memory: 4Gi
          cpu: 2000m
  endpoints:
    - name: api
      port: 8080
      public: false
$$;

-- Existing services keep their settings; apply the idle timer on re-runs.
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SET AUTO_SUSPEND_SECS = 1800;

-- Poll until READY (check logs on failure):
--   SELECT SYSTEM$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC');
--   SELECT SYSTEM$GET_SERVICE_LOGS('GEOCODING.PUBLIC.LIBPOSTAL_SVC', 0, 'libpostal', 100);

-- ---------------------------------------------------------------------------
-- 4. Service functions bound to the container endpoints
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(addr VARCHAR)
  RETURNS OBJECT
  SERVICE  = GEOCODING.PUBLIC.LIBPOSTAL_SVC
  ENDPOINT = 'api'
  MAX_BATCH_ROWS = 500
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'
  AS '/parse';

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.EXPAND_STREET_INTL(addr VARCHAR)
  RETURNS ARRAY
  SERVICE  = GEOCODING.PUBLIC.LIBPOSTAL_SVC
  ENDPOINT = 'api'
  MAX_BATCH_ROWS = 500
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'
  AS '/expand';

-- Smoke test (multi-country):
-- SELECT PARSE_ADDRESS_INTL('Av. Paulista 1578, São Paulo, Brazil');
-- SELECT PARSE_ADDRESS_INTL('10 Downing St, London SW1A 2AA, UK');
-- SELECT EXPAND_STREET_INTL('Rue de Rivoli');

-- ---------------------------------------------------------------------------
-- Cost control: suspend when not in use
-- ---------------------------------------------------------------------------
-- ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SUSPEND;
-- ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL SUSPEND;
