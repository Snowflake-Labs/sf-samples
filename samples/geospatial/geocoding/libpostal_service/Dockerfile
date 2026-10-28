# libpostal service for Snowflake SPCS service functions.
# Multi-stage build: compile libpostal + download models, then a slim runtime.

# ---------- Stage 1: build libpostal ----------
FROM python:3.11-slim AS libpostal-build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates git build-essential autoconf automake libtool pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Compile libpostal. Data models (~2GB) are downloaded into /opt/libpostal_data.
RUN git clone --depth 1 https://github.com/openvenues/libpostal /tmp/libpostal \
    && cd /tmp/libpostal \
    && ./bootstrap.sh \
    && ./configure --datadir=/opt/libpostal_data \
    && make -j"$(nproc)" \
    && make install \
    && ldconfig \
    && rm -rf /tmp/libpostal

# The two largest model files (1.0 GB and 0.6 GB) are cut into 200 MB parts,
# spread over 8 directories that become separate image layers below. The
# registry upload of a layer is one request, and a 1 GB request often ends
# with "timeout awaiting response headers". start.sh joins the parts again.
RUN cd /opt/libpostal_data/libpostal/address_parser \
    && for f in address_parser_crf.dat address_parser_postal_codes.dat; do \
         split -b 200M -a 1 "$f" "$f.part-" && rm "$f"; \
       done \
    && i=0 && for p in *.part-*; do \
         mkdir -p /opt/parts/$((i % 8)) && mv "$p" /opt/parts/$((i % 8))/; i=$((i + 1)); \
       done

# ---------- Stage 2: runtime base (libraries + small model files) ----------
FROM python:3.11-slim AS part-base

ENV DEBIAN_FRONTEND=noninteractive
# libpostal runtime needs its shared lib present; build tools needed to compile the `postal` binding wheel.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Bring compiled libpostal shared library + headers + data from the build stage.
COPY --from=libpostal-build /usr/local/lib/libpostal* /usr/local/lib/
COPY --from=libpostal-build /usr/local/include/libpostal /usr/local/include/libpostal
COPY --from=libpostal-build /usr/local/lib/pkgconfig/libpostal.pc /usr/local/lib/pkgconfig/libpostal.pc
COPY --from=libpostal-build /opt/libpostal_data /opt/libpostal_data
RUN ldconfig

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Model parts, one stage per 200 MB layer ----------
# Each stage adds one part on top of the previous one. The installer pushes
# the stages in order, so every push uploads a single new layer (see the
# comment above the split step).
FROM part-base AS part-0
COPY --from=libpostal-build /opt/parts/0 /opt/parts/0
FROM part-0 AS part-1
COPY --from=libpostal-build /opt/parts/1 /opt/parts/1
FROM part-1 AS part-2
COPY --from=libpostal-build /opt/parts/2 /opt/parts/2
FROM part-2 AS part-3
COPY --from=libpostal-build /opt/parts/3 /opt/parts/3
FROM part-3 AS part-4
COPY --from=libpostal-build /opt/parts/4 /opt/parts/4
FROM part-4 AS part-5
COPY --from=libpostal-build /opt/parts/5 /opt/parts/5
FROM part-5 AS part-6
COPY --from=libpostal-build /opt/parts/6 /opt/parts/6
FROM part-6 AS part-7
COPY --from=libpostal-build /opt/parts/7 /opt/parts/7

# ---------- Final image ----------
FROM part-7
COPY server.py start.sh ./

EXPOSE 8080
CMD ["sh", "/app/start.sh"]
