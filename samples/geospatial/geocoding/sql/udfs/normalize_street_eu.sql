-- ============================================================================
-- NORMALIZE_STREET_EU / NORMALIZE_NAME_EU — comparison keys for European data
-- ============================================================================
-- Overture stores European streets in local spelling and inconsistent case
-- ("Waldstraße", "VIA SILVIO PELLICO", "chemin des chazottes"). User input adds
-- abbreviations and transliterations ("Waldstr.", "Wald Strasse", "v. Pellico").
-- Both sides are reduced to the same lowercase ASCII key with no separators,
-- so an exact or Jaro-Winkler comparison works on like-for-like strings:
--
--   Hauptstraße / Hauptstrasse / Haupt Strasse / Hauptstr.  -> hauptstrasse
--   Müllerstraße / Muellerstrasse                            -> mullerstrasse
--   Av. de la République / avenue de la republique           -> avenuedelarepublique
--   V.le Monza / Viale Monza                                 -> vialemonza
--   ul. Władysława Łokietka / Władysława Łokietka            -> wladyslawalokietka
--
-- The same function is applied to the Overture side when OVERTURE_ADDRESS_EU
-- is built, and to every libpostal expansion of the input street at query time.
--
--   NORMALIZE_STREET_EU(street, country) -> VARCHAR  (street key)
--   NORMALIZE_NAME_EU(name)              -> VARCHAR  (locality/city key)
-- ============================================================================

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.NORMALIZE_STREET_EU(
    street VARCHAR, country VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.11
  HANDLER = 'normalize'
  COMMENT = 'oss-geocoding'
AS $$
import re
import unicodedata

# Letters that NFKD does not decompose to ASCII.
SPECIAL = {'ß': 'ss', 'ẞ': 'ss', 'ł': 'l', 'Ł': 'l', 'ø': 'o', 'Ø': 'o',
           'æ': 'ae', 'Æ': 'ae', 'đ': 'd', 'Đ': 'd', 'œ': 'oe', 'Œ': 'oe',
           'º': 'o', 'ª': 'a'}

# Token-level abbreviation expansions per country (input token, dots removed).
# A token that maps to '' is dropped (e.g. the Polish "ul." street prefix,
# which Overture omits).
ABBREV = {
    'DE': {'str': 'strasse', 'pl': 'platz', 'st': 'sankt', 'chaussee': 'chaussee',
           'allee': 'allee', 'prof': 'professor', 'dr': 'doktor'},
    'AT': {'str': 'strasse', 'pl': 'platz', 'st': 'sankt', 'g': 'gasse', 'prof': 'professor', 'dr': 'doktor'},
    'CH': {'str': 'strasse', 'pl': 'platz', 'st': 'sankt', 'av': 'avenue', 'ch': 'chemin', 'r': 'rue'},
    'FR': {'r': 'rue', 'av': 'avenue', 'ave': 'avenue', 'bd': 'boulevard', 'bld': 'boulevard',
           'boul': 'boulevard', 'pl': 'place', 'ch': 'chemin', 'che': 'chemin', 'chem': 'chemin',
           'imp': 'impasse', 'all': 'allee', 'rte': 'route', 'fg': 'faubourg', 'fbg': 'faubourg',
           'st': 'saint', 'ste': 'sainte', 'sq': 'square', 'crs': 'cours', 'qu': 'quai', 'qua': 'quai',
           'res': 'residence', 'lot': 'lotissement', 'hlm': 'hlm', 'gen': 'general', 'mal': 'marechal'},
    'BE': {'r': 'rue', 'av': 'avenue', 'bd': 'boulevard', 'pl': 'place', 'str': 'straat',
           'ln': 'laan', 'st': 'sint', 'ch': 'chaussee'},
    'IT': {'v': 'via', 'vle': 'viale', 'vl': 'viale', 'pza': 'piazza', 'pzza': 'piazza', 'p': 'piazza',
           'pz': 'piazza', 'cso': 'corso', 'c': 'corso', 'lgo': 'largo', 'vco': 'vicolo', 'vic': 'vicolo',
           'loc': 'localita', 'fraz': 'frazione', 'str': 'strada', 'sp': 'strada provinciale',
           'ss': 'strada statale', 's': 'san', 'sta': 'santa', 'gen': 'generale', 'dott': 'dottor'},
    'ES': {'c': 'calle', 'cl': 'calle', 'cll': 'calle', 'av': 'avenida', 'avd': 'avenida', 'avda': 'avenida',
           'pza': 'plaza', 'pl': 'plaza', 'plz': 'plaza', 'po': 'paseo', 'pso': 'paseo', 'ps': 'paseo',
           'ctra': 'carretera', 'cra': 'carretera', 'cmno': 'camino', 'cno': 'camino', 'cam': 'camino',
           'ronda': 'ronda', 'rda': 'ronda', 'trva': 'travesia', 'trv': 'travesia', 'urb': 'urbanizacion',
           'gta': 'glorieta', 'pje': 'pasaje', 'psje': 'pasaje', 'sta': 'santa', 'sto': 'santo',
           # Overture ES inverts articles ("CALLE POZO, DEL"); drop them on both sides.
           'de': '', 'del': '', 'la': '', 'el': '', 'los': '', 'las': '', 'y': ''},
    'PT': {'r': 'rua', 'av': 'avenida', 'avda': 'avenida', 'pc': 'praca', 'pca': 'praca', 'lg': 'largo',
           'tv': 'travessa', 'trav': 'travessa', 'estr': 'estrada', 'al': 'alameda', 'cc': 'calcada',
           'pct': 'praceta', 'bco': 'beco', 'urb': 'urbanizacao', 'qta': 'quinta'},
    'PL': {'ul': '', 'al': 'aleja', 'pl': 'plac', 'os': 'osiedle', 'ks': 'ksiedza', 'gen': 'generala',
           'sw': 'swietego', 'marsz': 'marszalka'},
    'NL': {'str': 'straat', 'ln': 'laan', 'pl': 'plein', 'wg': 'weg', 'burg': 'burgemeester',
           'st': 'sint', 'kon': 'koning', 'pr': 'prins', 'v': 'van', 'gr': 'graaf'},
    'DK': {'g': 'gade', 'pl': 'plads', 'st': 'sankt', 'gl': 'gammel', 'ndr': 'nordre', 'sdr': 'sondre'},
    'NO': {'g': 'gate', 'gt': 'gate', 'v': 'vei', 'vn': 'veien', 'pl': 'plass'},
    'FI': {'k': 'katu', 'tie': 'tie'},
    'CZ': {'nam': 'namesti', 'tr': 'trida', 'ul': ''},
}

# Suffix rewrites applied to whole-word compounds (German-style languages),
# e.g. "hauptstr" -> "hauptstrasse". Applied per token after abbreviation.
SUFFIX = {
    'DE': [('str', 'strasse'), ('pl', 'platz')],
    'AT': [('str', 'strasse'), ('pl', 'platz')],
    'CH': [('str', 'strasse')],
    'NL': [('str', 'straat')],
    'BE': [('str', 'straat')],
    'DK': [('g', 'gade')],
    'NO': [('gt', 'gate')],
}

UMLAUT_LANG = {'DE', 'AT', 'CH'}


def _ascii(text):
    text = ''.join(SPECIAL.get(ch, ch) for ch in text)
    text = unicodedata.normalize('NFKD', text)
    return ''.join(ch for ch in text if not unicodedata.combining(ch))


def normalize(street, country):
    if street is None:
        return None
    country = (country or '').upper().strip()
    text = _ascii(str(street)).lower()
    if country in UMLAUT_LANG:
        # Muellerstrasse and Müllerstraße must meet: fold the transliterated
        # digraph to the bare vowel (the umlaut itself already folded to it).
        text = text.replace('ae', 'a').replace('oe', 'o').replace('ue', 'u')
    abbrev = ABBREV.get(country, {})
    suffixes = SUFFIX.get(country, [])
    out = []
    for tok in re.split(r"[\s\-/,']+", text):
        tok = re.sub(r'[^a-z0-9]', '', tok)
        if not tok:
            continue
        if tok in abbrev:
            tok = abbrev[tok]
        else:
            for short, full in suffixes:
                if tok.endswith(short) and not tok.endswith(full) and len(tok) > len(short) + 2:
                    tok = tok[: -len(short)] + full
                    break
        if tok:
            out.append(tok)
    key = ''.join(out).replace(' ', '')
    return key or None
$$;

CREATE OR REPLACE FUNCTION GEOCODING.PUBLIC.NORMALIZE_NAME_EU(name VARCHAR)
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.11
  HANDLER = 'normalize'
  COMMENT = 'oss-geocoding'
AS $$
import re
import unicodedata

SPECIAL = {'ß': 'ss', 'ẞ': 'ss', 'ł': 'l', 'Ł': 'l', 'ø': 'o', 'Ø': 'o',
           'æ': 'ae', 'Æ': 'ae', 'đ': 'd', 'Đ': 'd', 'œ': 'oe', 'Œ': 'oe'}


def normalize(name):
    if name is None:
        return None
    text = ''.join(SPECIAL.get(ch, ch) for ch in str(name))
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = text.replace('ae', 'a').replace('oe', 'o').replace('ue', 'u')
    key = re.sub(r'[^a-z0-9]', '', text)
    return key or None
$$;
