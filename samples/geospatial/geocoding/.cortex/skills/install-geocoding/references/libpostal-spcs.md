# libpostal on SPCS (international geocoding, `--intl`)

The optional `--intl` path adds worldwide, multi-language address parsing by
running **libpostal** as an internal Snowpark Container Services (SPCS) service.
US forward geocoding uses the `usaddress` parser (US-specific); libpostal covers
non-US / non-Latin addresses.

## Why a container (not a UDF)

libpostal is a C library with ~2 GB of trained models — too large for a plain
Python UDF. It runs as an internal SPCS service exposed to SQL via service
functions.

## What `--intl` does

1. `CREATE IMAGE REPOSITORY IF NOT EXISTS GEOCODING.PUBLIC.IMAGE_REPOSITORY`.
2. Resolves the repo URL (`snow spcs image-repository url ...`).
3. Builds the image **for the installing account's own repository**:
   ```
   <runtime> build --platform linux/amd64 -t <repo_url>/libpostal:latest libpostal_service/
   ```
   The image is **not** shared between accounts — each install builds + pushes
   its own copy. This is why the installer never assumes a pre-pushed image.
4. `snow spcs image-registry login` + `<runtime> push`.
5. Runs `libpostal_service/deploy.sql`: compute pool `GEOCODING_LIBPOSTAL_POOL`
   (`CPU_X64_S`, auto-suspend 600 s), service `LIBPOSTAL_SVC`, and functions
   `PARSE_ADDRESS_INTL(VARCHAR)->OBJECT` and `EXPAND_STREET_INTL(VARCHAR)->ARRAY`.

## Build notes

- **ARM Macs:** the `--platform linux/amd64` build runs under emulation (QEMU) and
  is slow (tens of minutes). A default 2 GB Podman machine may OOM — prefer Docker
  or raise the Podman machine memory.
- Image size is ~2-3 GB.

## Runtime / cost

The service keeps one node warm while running. Suspend when idle:

```sql
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SUSPEND;
ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL SUSPEND;
```

Status / logs:

```sql
SELECT SYSTEM$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC');
SELECT SYSTEM$GET_SERVICE_LOGS('GEOCODING.PUBLIC.LIBPOSTAL_SVC', 0, 'libpostal', 100);
```

## Accuracy

libpostal parses worldwide addresses well, but non-US matching against Overture
is fuzzier than the US path — libpostal's normalized street spelling does not
always align with Overture's exact `STREET` value. Expect coverage gaps; pass an
ISO country code to narrow and speed up the join.
