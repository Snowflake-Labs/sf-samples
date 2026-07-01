# libpostal SPCS service (international geocoding)

Runs [libpostal](https://github.com/openvenues/libpostal) — worldwide, multi-language
address parsing + normalization — inside Snowpark Container Services, exposed to
SQL as **service functions**. This complements the US-only `usaddress` UDF in
`../sql/` with global coverage.

> **Status:** the container image is **already built and pushed** to
> `GEOCODING.PUBLIC.IMAGE_REPOSITORY` (`.../geocoding/public/image_repository/libpostal:latest`).
> The **service and service functions are not yet created** — run `deploy.sql`
> steps 2–4 to finish. (Skip step 1 / the build unless you change the image.)
> libpostal is a C library needing its compiled `.so` + ~2 GB trained models, so
> it cannot run in a plain Python UDF — SPCS is the supported path.

## Files

| File | Purpose |
|------|---------|
| `server.py` | FastAPI app implementing the SPCS service-function protocol (`/parse`, `/expand`, `/healthcheck`). |
| `Dockerfile` | Multi-stage build: compile libpostal + download models, then a slim runtime. All from official sources (no third-party prebuilt images). |
| `requirements.txt` | `fastapi`, `uvicorn`, `postal`. |
| `service-spec.yaml` | SPCS spec: image, port 8080, resources, readiness probe, **internal** endpoint. |
| `deploy.sql` | Creates the image repo, compute pool, service, and service functions. |

## Endpoints (service-function protocol)

Request `{"data": [[row_idx, addr], ...]}` -> Response `{"data": [[row_idx, result], ...]}`.

- `POST /parse` -> `OBJECT` `{house_number, road, unit, city, state, postcode, country, parse_error}`
- `POST /expand` -> `ARRAY` of normalized street variants (for fuzzy matching)
- `GET /healthcheck` -> readiness probe

## Build & push (ARM Mac -> amd64)

```bash
# 1. Build for the SPCS platform (QEMU emulation on Apple Silicon; slow, ~2-3 GB image)
docker build --platform linux/amd64 -t libpostal:latest .

# 2. (Recommended) verify locally before pushing
docker run --rm -p 8080:8080 libpostal:latest &
curl localhost:8080/healthcheck
curl -X POST localhost:8080/parse -H 'content-type: application/json' \
  -d '{"data": [[0, "Av. Paulista 1578, São Paulo, Brazil"]]}'

# 3. Log in to the account image registry and push
snow spcs image-registry login
REPO_URL=$(snow sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA GEOCODING.PUBLIC" --format json \
  | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['repository_url'])")
docker tag libpostal:latest "$REPO_URL/libpostal:latest"
docker push "$REPO_URL/libpostal:latest"
```

Then run `deploy.sql` and poll `SYSTEM$GET_SERVICE_STATUS` until `READY`.

## International matching notes

libpostal returns normalized, lowercased components but does **not** map to
Overture's exact street naming, so non-US matching is fuzzier than the US flow:

- Filter Overture by `COUNTRY = <parsed/param country>`.
- Match `NUMBER = house_number`.
- Match `POSTCODE = postcode` **OR** `UPPER(POSTAL_CITY) = UPPER(city)`.
- Street: use `EXPAND_STREET_INTL` variants + `ILIKE` / `EDITDISTANCE` against `STREET`.
- Do **not** use `STANDARDIZE_STREET` (US/USPS-specific).

## Cost

The service holds `MIN_INSTANCES = 1`, keeping a node warm. Suspend when idle:

```sql
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SUSPEND;
ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL SUSPEND;
```
