-- ============================================================================
-- PARSE_ADDRESS — free-text US address -> structured fields
-- ============================================================================
-- Python UDF using the `usaddress` CRF-based parser, pulled from Snowflake's
-- shared PyPI repository (no staging required).
--
--   Input : '125 Constitution Dr, Menlo Park, CA 94025'
--   Output: { "number":"125", "street":"Constitution Dr", "city":"Menlo Park",
--             "state":"CA", "zip":"94025", "parse_error":false }
--
-- NOTE: first call may take ~30s while the PyPI package installs; later calls
--       are fast.
-- ============================================================================

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.PARSE_ADDRESS(raw_address VARCHAR)
  RETURNS OBJECT
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.11
  ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
  PACKAGES = ('usaddress')
  HANDLER = 'parse'
  COMMENT = 'oss-geocoding'
AS $$
import usaddress

def parse(raw_address):
    if not raw_address:
        return None
    try:
        tagged, addr_type = usaddress.tag(raw_address)
    except usaddress.RepeatedLabelError:
        return {
            "number": None,
            "street": raw_address,
            "city": None,
            "state": None,
            "zip": None,
            "parse_error": True
        }

    street_parts = []
    for key in ['StreetNamePreDirectional', 'StreetName', 'StreetNamePostType', 'StreetNamePostDirectional']:
        if key in tagged:
            street_parts.append(tagged[key])

    return {
        "number": tagged.get('AddressNumber'),
        "street": ' '.join(street_parts) if street_parts else None,
        "city": tagged.get('PlaceName'),
        "state": tagged.get('StateName'),
        "zip": tagged.get('ZipCode'),
        "parse_error": False
    }
$$;
