-- ============================================================================
-- IDR Demo: Jaro-Winkler UDF
-- String similarity function for fuzzy name matching
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

CREATE OR REPLACE FUNCTION JARO_WINKLER_SIMILARITY(s1 STRING, s2 STRING)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'jaro_winkler'
AS
$$
def jaro_winkler(s1: str, s2: str) -> float:
    if s1 is None or s2 is None:
        return 0.0
    
    s1 = s1.upper().strip()
    s2 = s2.upper().strip()
    
    if s1 == s2:
        return 1.0
    
    if len(s1) == 0 or len(s2) == 0:
        return 0.0
    
    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0
    
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    
    matches = 0
    transpositions = 0
    
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    
    if matches == 0:
        return 0.0
    
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    
    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    
    prefix = 0
    for i in range(min(len1, len2, 4)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    
    return jaro + prefix * 0.1 * (1 - jaro)
$$;

COMMENT ON FUNCTION JARO_WINKLER_SIMILARITY(STRING, STRING) IS 
'Calculates Jaro-Winkler similarity between two strings. Returns value between 0 and 1. 
Values >= 0.92 indicate likely same name with minor spelling variations.';
