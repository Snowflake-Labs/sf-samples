#!/bin/sh
# Rebuild the large libpostal model files from the 200 MB parts that ship as
# separate image layers (see Dockerfile), then start the API.
set -e
D=/opt/libpostal_data/libpostal/address_parser
for f in address_parser_crf.dat address_parser_postal_codes.dat; do
  if [ ! -f "$D/$f" ]; then
    # Parts are spread over directories; join them by part suffix (a, b, ...).
    for s in a b c d e f g h i j k l; do
      for p in /opt/parts/*/"$f.part-$s"; do
        [ -f "$p" ] && cat "$p"
      done
    done > "$D/$f"
  fi
done
rm -rf /opt/parts
exec uvicorn server:app --host 0.0.0.0 --port 8080
