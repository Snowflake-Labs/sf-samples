# Build the felt/tippecanoe fork (supports PMTiles output) from source.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libsqlite3-dev zlib1g-dev git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 https://github.com/felt/tippecanoe.git /src
WORKDIR /src
RUN make -j"$(nproc)" && make install

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libsqlite3-0 zlib1g ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/tippecanoe /usr/local/bin/
COPY --from=build /usr/local/bin/tippecanoe-decode /usr/local/bin/
COPY --from=build /usr/local/bin/tile-join /usr/local/bin/
ENTRYPOINT ["tippecanoe"]
