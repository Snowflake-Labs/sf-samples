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

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim

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

COPY server.py .

EXPOSE 8080
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
