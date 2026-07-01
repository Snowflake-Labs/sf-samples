"""libpostal service for Snowflake SPCS service functions.

Implements the Snowflake service-function protocol:
  request  : {"data": [[row_idx, arg1], ...]}
  response : {"data": [[row_idx, result], ...]}

Endpoints:
  POST /parse   -> RETURNS OBJECT  (libpostal parse_address, mapped to normalized fields)
  POST /expand  -> RETURNS ARRAY   (libpostal expand_address normalized variants)
  GET  /healthcheck -> readiness probe
"""
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from postal.parser import parse_address
from postal.expand import expand_address

app = FastAPI()

# libpostal parse labels we surface, mapped to friendly keys.
_LABEL_MAP = {
    "house_number": "house_number",
    "road": "road",
    "unit": "unit",
    "postcode": "postcode",
    "city": "city",
    "state": "state",
    "country": "country",
}


@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}


def _parse_one(raw):
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    result = {v: None for v in _LABEL_MAP.values()}
    result["parse_error"] = False
    try:
        for value, label in parse_address(text):
            key = _LABEL_MAP.get(label)
            if key and result.get(key) is None:
                result[key] = value
    except Exception:
        result["road"] = text
        result["parse_error"] = True
    return result


def _expand_one(raw):
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return []
    try:
        return list(expand_address(text))
    except Exception:
        return [text]


@app.post("/parse")
async def parse(request: Request):
    body = await request.json()
    out = [[row[0], _parse_one(row[1] if len(row) > 1 else None)] for row in body["data"]]
    return JSONResponse({"data": out})


@app.post("/expand")
async def expand(request: Request):
    body = await request.json()
    out = [[row[0], _expand_one(row[1] if len(row) > 1 else None)] for row in body["data"]]
    return JSONResponse({"data": out})
