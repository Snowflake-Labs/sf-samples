-- ============================================================================
-- STANDARDIZE_STREET — expand USPS-abbreviated street names to Overture format
-- ============================================================================
-- Real-world addresses use abbreviations (E 38TH ST, JACK RABBIT TRL,
-- W ANDERSON LN); Overture stores full names (East 38th Street,
-- Jack Rabbit Trail, West Anderson Lane). This UDF bridges the gap with 230+
-- rules from USPS Publication 28 Appendix C and Nominatim variants-en.yaml.
--
--   Directional prefixes : E  -> East, NW -> Northwest
--   Street type suffixes : ST -> Street, TRL -> Trail, BLVD -> Boulevard
--   Multi-word abbrevs   : CR -> County Road, FM -> Farm to Market Road
--   Directional suffixes : MAIN ST N -> Main Street North
--   Output is Title Case.
-- ============================================================================

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.STANDARDIZE_STREET(street VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.11
  HANDLER = 'standardize'
  COMMENT = 'oss-geocoding'
AS $$
import re

# Directional expansions (used for both prefix and suffix positions)
DIRECTIONALS = {
    'N': 'North', 'S': 'South', 'E': 'East', 'W': 'West',
    'NE': 'Northeast', 'NW': 'Northwest', 'SE': 'Southeast', 'SW': 'Southwest',
    'STH': 'South',  # Australian/informal but seen in some US data
}

# Multi-word abbreviations: checked FIRST (before splitting into single words)
# These are prefix patterns that replace the first 1-2 words
MULTI_WORD_ABBREVS = {
    'CR': 'County Road',
    'CO RD': 'County Road',
    'COUNTY RD': 'County Road',
    'SR': 'State Route',
    'SH': 'State Highway',
    'STATE HWY': 'State Highway',
    'STATE RD': 'State Road',
    'US HWY': 'US Highway',
    'US RT': 'US Route',
    'US RTE': 'US Route',
    'FM': 'Farm to Market Road',
    'RM': 'Ranch to Market Road',
    'RR': 'Ranch Road',
    'FSR': 'Forest Service Road',
    'IH': 'Interstate Highway',
}

# Street type suffix expansions — comprehensive dictionary sourced from:
#   1. USPS Publication 28 Appendix C (official USPS abbreviations)
#   2. Nominatim variants-en.yaml (community-maintained OSM abbreviations)
#
# Format: ABBREVIATION -> Full Word (as Overture stores it)
# Multiple abbreviations can map to the same full word.
STREET_TYPES = {
    # --- A ---
    'AL': 'Alley', 'ALY': 'Alley', 'ALLY': 'Alley',
    'ANX': 'Anex',
    'ARC': 'Arcade',
    'AV': 'Avenue', 'AVE': 'Avenue', 'AVN': 'Avenue',
    # --- B ---
    'BCH': 'Beach',
    'BND': 'Bend',
    'BLF': 'Bluff',
    'BLFS': 'Bluffs',
    'BLVD': 'Boulevard', 'BVD': 'Boulevard',
    'BR': 'Branch',
    'BRG': 'Bridge', 'BRDG': 'Bridge', 'BDGE': 'Bridge',
    'BRK': 'Brook',
    'BRKS': 'Brooks',
    'BYP': 'Bypass', 'BYPA': 'Bypass', 'BPS': 'Bypass',
    'BWAY': 'Broadway', 'BDWY': 'Broadway', 'BWY': 'Broadway',
    # --- C ---
    'CP': 'Camp',
    'CYN': 'Canyon',
    'CPE': 'Cape',
    'CSWY': 'Causeway', 'CAUS': 'Causeway',
    'CTR': 'Center', 'CEN': 'Center', 'CNTR': 'Center',
    'CTRS': 'Centers',
    'CIR': 'Circle',
    'CIRS': 'Circles',
    'CCT': 'Circuit',
    'CLF': 'Cliff',
    'CLFS': 'Cliffs',
    'CL': 'Close',
    'CLB': 'Club',
    'CMN': 'Common', 'COMM': 'Common',
    'CMNS': 'Commons',
    'COR': 'Corner', 'CNR': 'Corner', 'CRN': 'Corner',
    'CORS': 'Corners',
    'CRSE': 'Course',
    'CT': 'Court', 'CRT': 'Court',
    'CTS': 'Courts',
    'CV': 'Cove', 'COV': 'Cove',
    'CVS': 'Coves',
    'CK': 'Creek', 'CRK': 'Creek',  # CR omitted — handled as "County Road" in MULTI_WORD_ABBREVS
    'CRES': 'Crescent',
    'CRST': 'Crest', 'CST': 'Crest',
    'XING': 'Crossing', 'CRSG': 'Crossing',
    'XRD': 'Crossroad',
    'XRDS': 'Crossroads',
    'CURV': 'Curve', 'CVE': 'Curve',
    # --- D ---
    'DL': 'Dale', 'DLE': 'Dale',
    'DM': 'Dam',
    'DV': 'Divide',
    'DR': 'Drive', 'DRV': 'Drive',
    'DRS': 'Drives',
    'DRWY': 'Driveway', 'DVWY': 'Driveway',
    # --- E ---
    'EST': 'Estate',
    'ESTS': 'Estates',
    'EXP': 'Expressway', 'EXPY': 'Expressway', 'EXPWY': 'Expressway', 'XWAY': 'Expressway',
    'EXT': 'Extension', 'EX': 'Extension',
    'EXTS': 'Extensions',
    # --- F ---
    'FLS': 'Falls',
    'FRY': 'Ferry', 'FY': 'Ferry',
    'FLD': 'Field', 'FD': 'Field',
    'FLDS': 'Fields',
    'FLT': 'Flat', 'FL': 'Flat',
    'FLTS': 'Flats',
    'FRD': 'Ford',
    'FRDS': 'Fords',
    'FRST': 'Forest',
    'FRG': 'Forge',
    'FRGS': 'Forges',
    'FRK': 'Fork',
    'FRKS': 'Forks',
    'FT': 'Fort',
    'FWY': 'Freeway', 'FRWY': 'Freeway',
    # --- G ---
    'GDN': 'Garden',
    'GDNS': 'Gardens',
    'GTWY': 'Gateway', 'GWY': 'Gateway',
    'GL': 'Glade', 'GLD': 'Glade', 'GLDE': 'Glade',
    'GLN': 'Glen',
    'GLNS': 'Glens',
    'GRN': 'Green',
    'GRNS': 'Greens',
    'GRV': 'Grove', 'GR': 'Grove', 'GRO': 'Grove',
    'GRVS': 'Groves',
    # --- H ---
    'HBR': 'Harbor',
    'HBRS': 'Harbors',
    'HVN': 'Haven',
    'HTS': 'Heights', 'HGTS': 'Heights', 'HT': 'Heights',
    'HWY': 'Highway',
    'HL': 'Hill',
    'HLS': 'Hills',
    'HOLW': 'Hollow',
    # --- I ---
    'INLT': 'Inlet',
    'IS': 'Island',
    'ISS': 'Islands',
    # --- J ---
    'JCT': 'Junction', 'JCTN': 'Junction', 'JNC': 'Junction',
    'JCTS': 'Junctions',
    # --- K ---
    'KY': 'Key',
    'KYS': 'Keys',
    'KNL': 'Knoll',
    'KNLS': 'Knolls',
    # --- L ---
    'LK': 'Lake',
    'LKS': 'Lakes',
    'LDG': 'Landing', 'LNDG': 'Landing',
    'LN': 'Lane', 'LA': 'Lane',
    'LGT': 'Light',
    'LGTS': 'Lights',
    'LF': 'Loaf',
    'LCK': 'Lock',
    'LCKS': 'Locks',
    'LP': 'Loop',
    # --- M ---
    'ML': 'Mall',
    'MNR': 'Manor',
    'MNRS': 'Manors',
    'MDW': 'Meadow',
    'MDWS': 'Meadows',
    'MWS': 'Mews',
    'MSN': 'Mission',
    'MTWY': 'Motorway',
    'MT': 'Mount',
    'MTN': 'Mountain',
    'MTNS': 'Mountains',
    # --- N ---
    'NCK': 'Neck',
    # --- O ---
    'ORCH': 'Orchard',
    'OPAS': 'Overpass',
    # --- P ---
    'PK': 'Park',
    'PKS': 'Parks',
    'PKWY': 'Parkway', 'PKY': 'Parkway', 'PWY': 'Parkway',
    'PASS': 'Pass',
    'PSGE': 'Passage',
    'PATH': 'Path',
    'PHWY': 'Pathway', 'PWAY': 'Pathway',
    'PNE': 'Pine',
    'PNES': 'Pines',
    'PL': 'Place',
    'PLN': 'Plain',
    'PLNS': 'Plains',
    'PLZ': 'Plaza', 'PLZA': 'Plaza',
    'PNT': 'Point', 'PT': 'Point',
    'PTS': 'Points',
    'PRT': 'Port',
    'PRTS': 'Ports',
    'PR': 'Prairie',
    # --- Q ---
    'QY': 'Quay',
    # --- R ---
    'RADL': 'Radial',
    'RNCH': 'Ranch',
    'RGE': 'Range', 'RNGE': 'Range',
    'RPD': 'Rapid',
    'RPDS': 'Rapids',
    'RST': 'Rest',
    'RDG': 'Ridge', 'RDGE': 'Ridge',
    'RDGS': 'Ridges',
    'RIV': 'River', 'RVR': 'River',
    'RD': 'Road',
    'RDS': 'Roads',
    'RTE': 'Route', 'RT': 'Route',
    'ROW': 'Row',
    'RUN': 'Run',
    # --- S ---
    'SHL': 'Shoal',
    'SHLS': 'Shoals',
    'SHR': 'Shore',
    'SHRS': 'Shores',
    'SKWY': 'Skyway',
    'SPG': 'Spring',
    'SPGS': 'Springs',
    'SPUR': 'Spur',
    'SQ': 'Square',
    'SQS': 'Squares',
    'STA': 'Station', 'STN': 'Station',
    'STRA': 'Stravenue',
    'STRM': 'Stream',
    'ST': 'Street',
    'STS': 'Streets',
    'SMT': 'Summit',
    # --- T ---
    'TCE': 'Terrace', 'TER': 'Terrace', 'TERR': 'Terrace',
    'THFR': 'Thoroughfare',
    'TRCE': 'Trace',
    'TRAK': 'Track', 'TRK': 'Track', 'TR': 'Track',
    'TRFY': 'Trafficway',
    'TRL': 'Trail',
    'TRLR': 'Trailer',
    'TUN': 'Tunnel', 'TUNL': 'Tunnel',
    'TPK': 'Turnpike', 'TPKE': 'Turnpike',
    # --- U ---
    'UPAS': 'Underpass',
    'UN': 'Union',
    'UNS': 'Unions',
    # --- V ---
    'VLY': 'Valley', 'VY': 'Valley',
    'VLYS': 'Valleys',
    'VIA': 'Viaduct', 'VDCT': 'Viaduct', 'VIAD': 'Viaduct',
    'VW': 'View',
    'VWS': 'Views',
    'VLG': 'Village', 'VILL': 'Village',
    'VLGS': 'Villages',
    'VL': 'Ville',
    'VIS': 'Vista', 'VST': 'Vista', 'VSTA': 'Vista',
    # --- W ---
    'WALK': 'Walk', 'WK': 'Walk', 'WLK': 'Walk',
    'WALL': 'Wall',
    'WY': 'Way',
    'WL': 'Well',
    'WLS': 'Wells',
    # --- Service road / direction markers (stripped) ---
    'SVRD': '', 'NB': '', 'SB': '', 'WB': '', 'EB': '',
}

# Directionals that can appear as suffix (last word after street type is handled)
DIRECTIONAL_SUFFIXES = {
    'N': 'North', 'S': 'South', 'E': 'East', 'W': 'West',
    'NE': 'Northeast', 'NW': 'Northwest', 'SE': 'Southeast', 'SW': 'Southwest',
}


def standardize(street):
    if not street:
        return None

    s = street.strip().upper()
    if not s:
        return None

    # --- Step 1: Multi-word abbreviation expansion (prefix match) ---
    # Check longest matches first to avoid partial matches
    for abbrev in sorted(MULTI_WORD_ABBREVS.keys(), key=len, reverse=True):
        if s == abbrev or s.startswith(abbrev + ' '):
            remainder = s[len(abbrev):].strip()
            s = MULTI_WORD_ABBREVS[abbrev] + (' ' + remainder if remainder else '')
            break

    words = s.split()
    if not words:
        return None

    # --- Step 2: Expand directional prefix (first word only) ---
    if words[0] in DIRECTIONALS:
        words[0] = DIRECTIONALS[words[0]]

    # --- Step 3: Strip trailing service road markers ---
    while len(words) > 1 and words[-1] in ('SVRD', 'NB', 'SB', 'WB', 'EB'):
        words.pop()

    # --- Step 4: Expand directional suffix (last word, if it's a bare directional) ---
    # e.g., "MAIN ST N" -> after suffix expansion: "MAIN ST North"
    # Must check BEFORE street type expansion since directional is after the type
    if len(words) > 2 and words[-1] in DIRECTIONAL_SUFFIXES:
        # Only treat as directional suffix if second-to-last is a street type
        if words[-2] in STREET_TYPES:
            words[-1] = DIRECTIONAL_SUFFIXES[words[-1]]

    # --- Step 5: Expand street type suffix (last word, or second-to-last if last is directional) ---
    if words:
        # Check if last word is now an expanded directional (from step 4)
        # In that case, the street type is second-to-last
        expanded_dirs = set(DIRECTIONAL_SUFFIXES.values())
        if len(words) > 2 and words[-1] in expanded_dirs:
            target_idx = -2
        else:
            target_idx = -1

        target_word = words[target_idx]
        if target_word in STREET_TYPES:
            expanded = STREET_TYPES[target_word]
            if expanded:
                words[target_idx] = expanded
            else:
                words.pop(target_idx)  # strip empty expansions

    # --- Step 6: Title case the result ---
    result = ' '.join(words)
    return result.title()
$$;
