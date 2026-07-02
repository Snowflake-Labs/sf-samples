-- ============================================================================
-- libpostal SPCS service — deployment (international / multi-country geocoding)
-- ============================================================================
-- Provisions the infrastructure to run libpostal (worldwide address parsing)
-- inside Snowpark Container Services and exposes it to SQL as service functions.
--
-- STATUS: template. The container image must be built & pushed before the
-- service will start (see this folder's README.md, Build & push section).
--
-- Prerequisites: ACCOUNTADMIN (or a role with CREATE COMPUTE POOL / SERVICE /
-- IMAGE REPOSITORY), and the GEOCODING database from ../sql/00_setup.sql.
-- ============================================================================

USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- ---------------------------------------------------------------------------
-- 1. Image repository (push the built image here)
-- ---------------------------------------------------------------------------
CREATE IMAGE REPOSITORY IF NOT EXISTS GEOCODING.PUBLIC.IMAGE_REPOSITORY;
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
  AUTO_SUSPEND_SECS = 600;

-- ---------------------------------------------------------------------------
-- 3. Service (internal endpoint only; spec in ./service-spec.yaml)
--    Either inline the spec or upload service-spec.yaml to a stage and use
--    FROM SPECIFICATION_FILE. Inline version shown below.
-- ---------------------------------------------------------------------------
CREATE SERVICE IF NOT EXISTS GEOCODING.PUBLIC.LIBPOSTAL_SVC
  IN COMPUTE POOL GEOCODING_LIBPOSTAL_POOL
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1
  FROM SPECIFICATION $$
spec:
  containers:
    - name: libpostal
      image: /GEOCODING/PUBLIC/IMAGE_REPOSITORY/libpostal:latest
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
  AS '/parse';

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.EXPAND_STREET_INTL(addr VARCHAR)
  RETURNS ARRAY
  SERVICE  = GEOCODING.PUBLIC.LIBPOSTAL_SVC
  ENDPOINT = 'api'
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
