-- deploy-tileserver: static SPCS infrastructure (detect-and-reuse-else-create).
-- Idempotent: every statement uses IF NOT EXISTS / CREATE OR REPLACE-safe forms.
-- PG-dependent objects (the PG_URL secret and the Postgres egress EAI) are NOT
-- here - they need the live instance host and are created inline by the
-- orchestrator after provision_pg.py resolves it. This file only creates the
-- account-static SPCS objects plus the basemap egress EAI (fixed CDN hosts).

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

CREATE DATABASE IF NOT EXISTS TILESERVER
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
CREATE SCHEMA IF NOT EXISTS TILESERVER.CORE
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

USE SCHEMA TILESERVER.CORE;

-- SPCS image repository (holds the Martin image).
CREATE IMAGE REPOSITORY IF NOT EXISTS TILESERVER.CORE.IMAGES
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- Small compute pool for the single Martin service.
CREATE COMPUTE POOL IF NOT EXISTS TILESERVER_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_RESUME = TRUE
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- Stage for the service spec.
CREATE STAGE IF NOT EXISTS TILESERVER.CORE.SPECS
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- Stage for the baked PMTiles archive (mounted into the service as /tiles).
CREATE STAGE IF NOT EXISTS TILESERVER.CORE.TILES
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- Basemap egress: the Martin Web UI (viewer) loads external raster basemaps in
-- the browser. Behind the SPCS ingress the proxy widens the page CSP connect-src
-- to include the hosts of any attached EAI, so these hosts must be listed here or
-- the Web UI basemaps stay blank (CSP "Refused to connect"). Network rules need
-- full hostnames (no wildcards); include the Carto a./b./c./d. subdomains.
CREATE OR REPLACE NETWORK RULE TILESERVER.CORE.BASEMAP_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'tile.openstreetmap.org:443',
    'basemaps.cartocdn.com:443',
    'a.basemaps.cartocdn.com:443',
    'b.basemaps.cartocdn.com:443',
    'c.basemaps.cartocdn.com:443',
    'd.basemaps.cartocdn.com:443'
  )
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS TILESERVER_BASEMAP_EAI
  ALLOWED_NETWORK_RULES = (TILESERVER.CORE.BASEMAP_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
