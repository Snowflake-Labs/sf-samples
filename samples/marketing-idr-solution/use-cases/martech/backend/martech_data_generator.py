"""
Martech Synthetic Data Generator
=================================

Produces synthetic data for the martech use-case demo. Generates canonical
customers with realistic stitching opportunities baked in:
  * Nickname variants (Bob/Robert, Liz/Elizabeth)            POS 8% / Shopify 6%
  * First-name typos (single-char edit, JW ~0.92-0.96)       POS 5% / Shopify 5%
  * Last-name typos                                          POS 5% / Shopify 5%
  * Married-name override (loyalty=maiden, shopify=married)  ~2%
  * Anonymous -> logged-in clickstream transitions           40%
  * Cross-device sessions per customer                       20%
  * Address abbreviation (Street -> St, etc.)
  * Households: spouses (same surname + address)             4%
  * Households: roommates (same address, different surname)  2%
  * Borderline ML/LLM cohort (POS+Shopify intentionally noisy) 2%

All data is synthetic (Faker-based). No real PII.

NFR-1: DB name is read from MARTECH_DB env var (default: MARTECH).

CLI:
    python -m martech_data_generator --customers 500 --events 5000 \
        --connection-name aws-east1
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

# Persistent wave state directory
_STATE_DIR = Path(__file__).parent / ".generator_state"
_STATE_FILE = _STATE_DIR / "state.json"

from faker import Faker  # type: ignore
import snowflake.connector  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NICKNAME_COLLISIONS = {
    "ROBERT": ["BOB", "ROB", "ROBBIE", "BOBBY"],
    "WILLIAM": ["BILL", "WILL", "WILLY", "BILLY"],
    "JAMES": ["JIM", "JIMMY", "JAMIE"],
    "MICHAEL": ["MIKE", "MIKEY", "MICK"],
    "RICHARD": ["RICK", "RICKY", "RICH", "RICHIE", "DICK"],
    "ELIZABETH": ["LIZ", "LIZZIE", "BETH", "BETTY"],
    "MARGARET": ["MAGGIE", "MEG", "PEGGY"],
    "KATHERINE": ["KATE", "KATIE", "KATHY", "KAT"],
    "JENNIFER": ["JEN", "JENNY", "JENNIE"],
    "PATRICIA": ["PAT", "PATTY", "TRISH"],
}


@dataclass
class CanonicalCustomer:
    customer_id: str
    canonical_first_name: str
    canonical_last_name: str
    nickname_first_name: str | None
    married_last_name: str | None  # if set, Shopify emits this; Loyalty stays canonical (maiden)
    email: str
    phone: str
    street: str
    city: str
    state: str
    postal: str
    country: str
    has_loyalty: bool
    has_pos: bool
    has_shopify: bool
    has_web: bool
    loyalty_member_id: str | None
    enrolled_at: datetime | None
    cookie_id: str
    device_id: str | None
    uid2: str | None
    rampid: str | None
    cross_device_cookie_id: str | None  # second cookie if cross-device
    household_partner_id: str | None  # other customer_id sharing this address
    household_role: str | None  # 'spouse' | 'roommate' | None
    borderline: bool  # member of the engineered ML/LLM borderline cohort
    email_mismatch: bool  # member of email-mismatch cohort (POS uses different email)
    email_mismatch_variant: str | None  # 'phone_only' | 'phone_nickname' | 'phone_fuzzy_first' | 'phone_fuzzy_last'
    pos_alt_email: str | None  # alternate email for POS rows when email_mismatch is True
    ml_cross_source: bool = False  # member of ML cross-source cohort
    ml_cross_source_tier: str | None = None  # 'strong' | 'fuzzy'
    ml_alt_email: str | None = None  # same handle, different domain
    ml_alt_phone: str | None = None  # same last-7, area code omitted (7-digit local)
    near_miss: bool = False  # near-miss stranger (different person, similar features)
    near_miss_target_postal: str | None = None  # shares postal with a real customer
    near_miss_similar_first: str | None = None  # similar first name to confuse ML
    ambiguous_relative: bool = False  # relative in same household (ambiguous match)
    ambiguous_relative_of: str | None = None  # customer_id of the person they're related to
    shared_line_collision: bool = False  # DIFFERENT person sharing target's phone+address (hard negative -> LLM should reject)
    _created_wave: int = 0  # which wave this customer was introduced

    def get_first_name(self, variant: str = "canonical") -> str:
        if variant == "nickname" and self.nickname_first_name:
            return self.nickname_first_name
        return self.canonical_first_name


@dataclass
class GenerateSummary:
    customers: int
    pos_rows: int = 0
    loyalty_rows: int = 0
    web_rows: int = 0
    shopify_rows: int = 0
    households_paired: int = 0
    spouses: int = 0
    roommates: int = 0
    married_name_count: int = 0
    borderline_count: int = 0
    email_mismatch_count: int = 0
    near_miss_count: int = 0
    ambiguous_relative_count: int = 0
    collision_count: int = 0
    nickname_seeded_count: int = 0
    wave_num: int = 0
    new_customers: int = 0
    returning_customers: int = 0
    duration_sec: float = 0.0
    errors: list[str] = field(default_factory=list)


def _read_wave_state() -> int:
    if _STATE_FILE.exists():
        data = json.loads(_STATE_FILE.read_text())
        return data.get("wave_num", 0)
    return 0


def _write_wave_state(wave_num: int) -> None:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps({"wave_num": wave_num}))


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class MartechDataGenerator:
    # Wave constants
    BASE_CUSTOMERS = 5_000
    NEW_CUSTOMERS_PER_WAVE = 2_000
    RETURNING_RATE = 0.25
    RETURNING_DECAY = 0.15
    RETURNING_WINDOW = 10  # waves
    POPULATION_SEED = 42

    def __init__(
        self,
        db_name: str | None = None,
        num_customers: int = 10_000,
        num_events: int = 100_000,
        loyalty_pct: float = 0.70,
        pos_pct: float = 0.60,
        shopify_pct: float = 0.80,
        clickstream_pct: float = 0.90,
        # Variation rates (per-emitter unless noted)
        nickname_rate: float = 0.08,            # POS row-level rate (Shopify uses 0.06)
        pos_typo_rate: float = 0.05,
        shopify_typo_rate: float = 0.05,
        # Customer-level cohort flags
        married_name_rate: float = 0.02,
        household_spouse_rate: float = 0.15,
        household_roommate_rate: float = 0.05,
        borderline_rate: float = 0.005,
        email_mismatch_rate: float = 0.06,
        ml_cross_source_rate: float = 0.015,
        near_miss_rate: float = 0.005,
        ambiguous_relative_rate: float = 0.005,
        collision_rate: float = 0.01,
        anon_to_known_rate: float = 0.40,
        cross_device_rate: float = 0.20,
        seed: int | None = None,
        connection_name: str | None = None,
    ) -> None:
        self.db_name = db_name or os.getenv("MARTECH_DB", "MARTECH")
        self.num_customers = num_customers
        self.num_events = num_events
        self.loyalty_pct = loyalty_pct
        self.pos_pct = pos_pct
        self.shopify_pct = shopify_pct
        self.clickstream_pct = clickstream_pct
        self.nickname_rate = nickname_rate
        self.pos_typo_rate = pos_typo_rate
        self.shopify_typo_rate = shopify_typo_rate
        self.married_name_rate = married_name_rate
        self.household_spouse_rate = household_spouse_rate
        self.household_roommate_rate = household_roommate_rate
        self.borderline_rate = borderline_rate
        self.email_mismatch_rate = email_mismatch_rate
        self.ml_cross_source_rate = ml_cross_source_rate
        self.near_miss_rate = near_miss_rate
        self.ambiguous_relative_rate = ambiguous_relative_rate
        self.collision_rate = collision_rate
        self.anon_to_known_rate = anon_to_known_rate
        self.cross_device_rate = cross_device_rate
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME")
        # Wave state
        self.wave_num = _read_wave_state()
        # Population seed is fixed so the base population is deterministic;
        # event-level RNG uses wave-scoped seed for unique data per wave.
        self._pop_seed = self.POPULATION_SEED
        self._wave_seed = seed if seed is not None else (83 + self.wave_num)
        random.seed(self._pop_seed)
        self.faker = Faker("en_US")
        Faker.seed(self._pop_seed)
        self.summary = GenerateSummary(customers=num_customers, wave_num=self.wave_num)

    # ---- public api -------------------------------------------------------

    def run_all(self, stage_callback: Callable[[str], None] | None = None) -> GenerateSummary:
        start = datetime.utcnow()
        _stage = stage_callback or (lambda s: None)

        _stage("building_population")
        customers = self._build_wave_population()

        # Switch to wave-scoped RNG for event generation so each wave is unique
        random.seed(self._wave_seed)
        self.faker = Faker("en_US")
        Faker.seed(self._wave_seed)

        _stage("generating_events")
        pos_rows = self._generate_pos_rows(customers)
        loyalty_rows = self._generate_loyalty_rows(customers)
        web_rows = self._generate_web_rows(customers)
        shopify_rows = self._generate_shopify_rows(customers)

        self.summary.pos_rows = len(pos_rows)
        self.summary.loyalty_rows = len(loyalty_rows)
        self.summary.web_rows = len(web_rows)
        self.summary.shopify_rows = len(shopify_rows)

        _stage("writing_files")
        wave_dir = Path(tempfile.mkdtemp(prefix="martech_dg_"))
        pos_file = self._write_csv(wave_dir / "pos.csv", pos_rows, self.POS_COLS)
        loyalty_file = self._write_csv(wave_dir / "loyalty.csv", loyalty_rows, self.LOYALTY_COLS)
        web_file = self._write_csv(wave_dir / "web.csv", web_rows, self.WEB_COLS)
        shopify_file = self._write_csv(wave_dir / "shopify.csv", shopify_rows, self.SHOPIFY_COLS)

        _stage("loading_to_snowflake")
        self._parallel_put_copy(
            pos_file, loyalty_file, web_file, shopify_file
        )

        # Bump wave counter on success
        _write_wave_state(self.wave_num + 1)

        self.summary.duration_sec = (datetime.utcnow() - start).total_seconds()
        return self.summary

    def _build_wave_population(self) -> list[CanonicalCustomer]:
        """Build the full population deterministically, then select active
        customers for this wave (new + returning).

        num_customers is treated as the ACTIVE budget per wave (how many
        customers to emit), NOT a cap on the total historical population.
        The historical pool grows unbounded so that later waves always have
        their own slot for new customers.
        """
        # Deterministic population build with fixed seed
        random.seed(self._pop_seed)
        self.faker = Faker("en_US")
        Faker.seed(self._pop_seed)

        # Scale the wave constants proportionally to requested num_customers
        scale = max(1, self.num_customers // 10_000)
        base = self.BASE_CUSTOMERS * scale
        per_wave = self.NEW_CUSTOMERS_PER_WAVE * scale

        # Build the full historical population (uncapped) — must include
        # all customers up through the current wave so wave_num has entries.
        total_historical = base + (per_wave * self.wave_num)
        all_customers = self.generate_canonical_customers(count=total_historical)

        # Assign created_wave to each customer deterministically
        for i, c in enumerate(all_customers):
            if i < base:
                c._created_wave = 0
            else:
                c._created_wave = ((i - base) // per_wave) + 1

        # Select active: all new customers from current wave + probabilistic
        # returners from the sliding window of prior waves.
        wave_rng = random.Random(self._wave_seed + 7)
        new_customers: list[CanonicalCustomer] = []
        returners: list[CanonicalCustomer] = []

        for c in all_customers:
            if c._created_wave == self.wave_num:
                new_customers.append(c)
            elif c._created_wave < self.wave_num:
                age = self.wave_num - c._created_wave
                if age > self.RETURNING_WINDOW:
                    continue
                decay = self.RETURNING_RATE * max(0.10, 1.0 - age * self.RETURNING_DECAY)
                if wave_rng.random() < decay:
                    returners.append(c)

        # Apply num_customers as budget: new customers first, then returners
        budget = self.num_customers
        active: list[CanonicalCustomer] = new_customers[:budget]
        self.summary.new_customers = len(active)
        remaining = budget - len(active)
        if remaining > 0:
            active.extend(returners[:remaining])
        self.summary.returning_customers = len(active) - self.summary.new_customers

        self.summary.customers = len(active)
        return active

    # ---- column definitions ------------------------------------------------

    POS_COLS = [
        "TXN_ID", "STORE_ID", "TS", "TENDER_TYPE", "TOTAL",
        "LOYALTY_MEMBER_NUMBER", "CUSTOMER_EMAIL", "CUSTOMER_PHONE",
        "CUSTOMER_FIRST_NAME", "CUSTOMER_LAST_NAME", "LINE_ITEMS_JSON", "SOURCE_FILE",
    ]
    LOYALTY_COLS = [
        "MEMBER_ID", "EMAIL", "PHONE", "FIRST_NAME", "LAST_NAME",
        "STREET_ADDRESS", "CITY", "STATE", "POSTAL_CODE", "COUNTRY",
        "TIER", "POINTS", "ENROLLED_AT", "SOURCE_FILE",
    ]
    WEB_COLS = [
        "EVENT_ID", "TS", "ANONYMOUS_ID", "DEVICE_ID", "UID2", "RAMPID",
        "SESSION_ID", "USER_AGENT", "IP", "EVENT_NAME", "PAGE_URL",
        "LOGGED_IN_EMAIL", "LOGGED_IN_PHONE", "LOGGED_IN_MEMBER_ID",
        "EVENT_PROPERTIES_JSON", "SOURCE_FILE",
    ]
    SHOPIFY_COLS = [
        "ORDER_ID", "ORDER_NUMBER", "CREATED_AT_SRC", "TOTAL_PRICE",
        "FINANCIAL_STATUS", "RAW_PAYLOAD", "SOURCE_FILE",
    ]
    # JSON/VARIANT columns that need PARSE_JSON in COPY
    _JSON_COLS = {"LINE_ITEMS_JSON", "RAW_PAYLOAD", "EVENT_PROPERTIES_JSON"}

    # ---- canonical data ---------------------------------------------------

    def generate_canonical_customers(self, count: int | None = None) -> list[CanonicalCustomer]:
        n = count if count is not None else self.num_customers
        customers: list[CanonicalCustomer] = []
        for i in range(n):
            canonical_first = self.faker.first_name().upper()
            nickname_first = None
            # Seed a nickname-mappable canonical for ~8% of customers (so emitters
            # can choose to emit the nickname variant). We skew canonical names to
            # the NICKNAME_COLLISIONS keys when needed so the mapping exists.
            if random.random() < self.nickname_rate:
                if canonical_first not in NICKNAME_COLLISIONS:
                    canonical_first = random.choice(list(NICKNAME_COLLISIONS.keys()))
                nickname_first = random.choice(NICKNAME_COLLISIONS[canonical_first])
                self.summary.nickname_seeded_count += 1
            canonical_last = self.faker.last_name().upper()
            married_last = None
            if random.random() < self.married_name_rate:
                # Married name is distinct enough from maiden that R01 email still
                # carries the cluster but R11/R13 (fuzzy last + email/phone) and
                # the ML scorer get features when name comparison happens.
                married_last = self.faker.last_name().upper()
                if married_last == canonical_last:
                    married_last = canonical_last + "-" + self.faker.last_name().upper()
                self.summary.married_name_count += 1
            email = f"{canonical_first.lower()}.{canonical_last.lower()}{i}@{random.choice(['gmail.com','yahoo.com','hotmail.com','hey.com','outlook.com'])}"
            phone = self._gen_phone_e164()
            customer = CanonicalCustomer(
                customer_id=f"CUST-{i:08d}",
                canonical_first_name=canonical_first,
                canonical_last_name=canonical_last,
                nickname_first_name=nickname_first,
                married_last_name=married_last,
                email=email,
                phone=phone,
                street=self.faker.street_address(),
                city=self.faker.city(),
                state=self.faker.state_abbr(),
                postal=self.faker.postcode(),
                country="US",
                has_loyalty=random.random() < self.loyalty_pct,
                has_pos=random.random() < self.pos_pct,
                has_shopify=random.random() < self.shopify_pct,
                has_web=random.random() < self.clickstream_pct,
                loyalty_member_id=None,
                enrolled_at=None,
                cookie_id=f"snw_{uuid.uuid4().hex[:12]}",
                device_id=None,
                uid2=None,
                rampid=None,
                cross_device_cookie_id=None,
                household_partner_id=None,
                household_role=None,
                borderline=False,
                email_mismatch=False,
                email_mismatch_variant=None,
                pos_alt_email=None,
            )
            if customer.has_loyalty:
                customer.loyalty_member_id = f"LM-{i:07d}"
                customer.enrolled_at = self.faker.date_time_between(start_date="-3y", end_date="-1d")
            if random.random() < 0.6:
                customer.device_id = f"UID2-{uuid.uuid4().hex[:24]}"
                customer.uid2 = customer.device_id
            if random.random() < 0.4:
                customer.rampid = f"XY{uuid.uuid4().hex[:30]}"
            if random.random() < self.cross_device_rate:
                customer.cross_device_cookie_id = f"snw_{uuid.uuid4().hex[:12]}"
            customers.append(customer)

        # Household pairing pass — shuffle a copy and walk pairs. Spouses share
        # surname + address (R14 target). Roommates share address only (negative
        # household example; address-only candidates).
        n_spouse_pairs = int(self.num_customers * self.household_spouse_rate / 2)
        n_roommate_pairs = int(self.num_customers * self.household_roommate_rate / 2)
        pool = [c for c in customers if c.has_loyalty]
        random.shuffle(pool)
        idx = 0
        for _ in range(n_spouse_pairs):
            if idx + 1 >= len(pool):
                break
            a, b = pool[idx], pool[idx + 1]
            idx += 2
            b.canonical_last_name = a.canonical_last_name
            # Re-derive b's email since lastname changed (preserve uniqueness via
            # the trailing index baked into email construction).
            b_idx = int(b.customer_id.split("-")[1])
            b.email = f"{b.canonical_first_name.lower()}.{b.canonical_last_name.lower()}{b_idx}@{b.email.split('@')[1]}"
            b.street, b.city, b.state, b.postal = a.street, a.city, a.state, a.postal
            a.household_partner_id, b.household_partner_id = b.customer_id, a.customer_id
            a.household_role = b.household_role = "spouse"
            self.summary.spouses += 2
            self.summary.households_paired += 1
        for _ in range(n_roommate_pairs):
            if idx + 1 >= len(pool):
                break
            a, b = pool[idx], pool[idx + 1]
            idx += 2
            # Roommates: address only, distinct surnames preserved
            b.street, b.city, b.state, b.postal = a.street, a.city, a.state, a.postal
            a.household_partner_id, b.household_partner_id = b.customer_id, a.customer_id
            a.household_role = b.household_role = "roommate"
            self.summary.roommates += 2
            self.summary.households_paired += 1

        # Borderline cohort: ~2% of customers tagged. Their POS + Shopify rows
        # carry layered noise (typo first + typo last + abbreviated street +
        # last-digit-flipped phone) so ML lands them in [0.55, 0.85) and LLM
        # adjudicates. Loyalty stays canonical so the engine has a ground truth.
        n_borderline = int(self.num_customers * self.borderline_rate)
        candidates = [c for c in customers if c.has_loyalty and c.has_pos and c.has_shopify]
        random.shuffle(candidates)
        for c in candidates[:n_borderline]:
            c.borderline = True
            self.summary.borderline_count += 1

        # Email-mismatch cohort: ~6% of customers tagged. Their POS rows use a
        # DIFFERENT email than loyalty (e.g., a personal email at the register
        # vs. work email on the loyalty profile). Same phone is preserved so
        # phone-anchored deterministic rules (R07/R09/R12/R13) can win the
        # dedup. Each customer is randomly assigned one of 4 variants:
        #   phone_only         -> R07 (Full Name + Phone)
        #   phone_nickname     -> R09 (Nickname First + Phone)
        #   phone_fuzzy_first  -> R12 (Fuzzy First + Phone)
        #   phone_fuzzy_last   -> R13 (First + Fuzzy Last + Phone)
        VARIANTS = ['phone_only', 'phone_nickname', 'phone_fuzzy_first', 'phone_fuzzy_last']
        n_email_mismatch = int(self.num_customers * self.email_mismatch_rate)
        candidates = [c for c in customers if c.has_loyalty and c.has_pos and not c.borderline]
        random.shuffle(candidates)
        for c in candidates[:n_email_mismatch]:
            c.email_mismatch = True
            c.email_mismatch_variant = random.choice(VARIANTS)
            # Generate an alternate email (e.g., personal vs. work) — same name
            # parts so it's plausible the same person owns both addresses.
            domain = random.choice(['gmail.com', 'yahoo.com', 'icloud.com', 'protonmail.com'])
            c.pos_alt_email = f"{c.canonical_first_name.lower()}.{c.canonical_last_name.lower()}.{random.randint(1000, 9999)}@{domain}"
            # Customers needing a nickname variant must have one available
            if c.email_mismatch_variant == 'phone_nickname' and not c.nickname_first_name:
                if c.canonical_first_name not in NICKNAME_COLLISIONS:
                    # Force a nickname-mappable canonical for this customer
                    c.canonical_first_name = random.choice(list(NICKNAME_COLLISIONS.keys()))
                c.nickname_first_name = random.choice(NICKNAME_COLLISIONS[c.canonical_first_name])
            self.summary.email_mismatch_count += 1

        # ML Cross-Source cohort: ~3% of customers. These appear in Loyalty (ground
        # truth) + POS/Shopify with a DIFFERENT email domain (same handle) and
        # different phone area code (same last-7). No loyalty_id on the alternate
        # source. Deterministic rules cannot link them, so ML must.
        #   "strong" tier: exact name, slight street abbreviation → AUTO_MERGE target
        #   "fuzzy"  tier: typo'd name, missing address → GREY_LLM target (LLM adjudicates)
        ALT_DOMAINS = ['protonmail.com', 'icloud.com', 'fastmail.com', 'zoho.com', 'tutanota.com']
        n_ml_cross = int(self.num_customers * self.ml_cross_source_rate)
        candidates = [c for c in customers
                      if c.has_loyalty and (c.has_pos or c.has_shopify)
                      and not c.borderline and not c.email_mismatch]
        random.shuffle(candidates)
        for idx, c in enumerate(candidates[:n_ml_cross]):
            c.ml_cross_source = True
            c.ml_cross_source_tier = 'strong' if idx % 3 != 0 else 'fuzzy'
            # Same email handle, different domain
            handle = c.email.split('@')[0]
            original_domain = c.email.split('@')[1]
            alt_domain = random.choice([d for d in ALT_DOMAINS if d != original_domain])
            c.ml_alt_email = f"{handle}@{alt_domain}"
            # Realistic "area code omitted" capture: same line, area code dropped.
            # Full E.164 differs (no area code) so deterministic phone rules can't
            # fire, but the trailing 7 digits legitimately match the full-number
            # records for this customer (e.g. loyalty/web).
            last7 = c.phone[-7:]
            c.ml_alt_phone = f"{last7[:3]}-{last7[3:]}"  # e.g. "295-0674"

        # Near-miss strangers: ~1.5% of customers. These are DIFFERENT people who
        # share surface features (same postal, similar first name, same last-name
        # 3-char prefix) with an existing customer. ML will generate candidate pairs
        # scoring 0.55-0.70 but they are genuinely different people → LLM should REJECT.
        SIMILAR_NAMES = {
            "MICHAEL": "MICHELE", "ROBERT": "ROBERTA", "JAMES": "JAMIE",
            "WILLIAM": "WILMA", "DAVID": "DAVIS", "RICHARD": "RACHEL",
            "JOSEPH": "JOSIE", "THOMAS": "TAMARA", "CHARLES": "CHARLOTTE",
            "DANIEL": "DANIELLE", "MATTHEW": "MATTHIAS", "ANTHONY": "ANTONIA",
            "MARK": "MARC", "STEVEN": "STEFAN", "PAUL": "PAULA",
            "ANDREW": "ANDREA", "JOSHUA": "JOCELYN", "KENNETH": "KENDRA",
            "KEVIN": "KELVIN", "BRIAN": "BRIANA", "GEORGE": "GEORGIA",
            "TIMOTHY": "TIFFANY", "RONALD": "RHONDA", "EDWARD": "EDWINA",
        }
        n_near_miss = int(self.num_customers * self.near_miss_rate)
        # Pick existing customers to be "confused with"
        target_pool = [c for c in customers if c.has_loyalty and not c.borderline
                       and not c.ml_cross_source and not c.email_mismatch]
        random.shuffle(target_pool)
        for target in target_pool[:n_near_miss]:
            # Create a near-miss record that shares postal + last name prefix
            similar_first = SIMILAR_NAMES.get(target.canonical_first_name,
                                             target.canonical_first_name[:-1] + random.choice("AEIO"))
            nm = CanonicalCustomer(
                customer_id=f"NM-{uuid.uuid4().hex[:12]}",
                canonical_first_name=similar_first,
                canonical_last_name=target.canonical_last_name,
                nickname_first_name=None,
                married_last_name=None,
                email=f"{similar_first.lower()}.{target.canonical_last_name.lower()}{random.randint(10,99)}@{random.choice(['gmail.com','outlook.com','yahoo.com'])}",
                phone=self._gen_phone_e164(),  # completely different phone
                street=self.faker.street_address(),  # different address
                city=target.city,  # same city (for postal overlap)
                state=target.state,
                postal=target.postal,  # SAME postal — triggers blocking
                country=target.country,
                has_loyalty=False,
                has_pos=True,
                has_shopify=True,
                has_web=False,
                loyalty_member_id=None,
                enrolled_at=None,
                cookie_id=f"c_{uuid.uuid4().hex[:16]}",
                device_id=None,
                uid2=None,
                rampid=None,
                cross_device_cookie_id=None,
                household_partner_id=None,
                household_role=None,
                borderline=False,
                email_mismatch=False,
                email_mismatch_variant=None,
                pos_alt_email=None,
                near_miss=True,
                near_miss_target_postal=target.postal,
                near_miss_similar_first=similar_first,
            )
            customers.append(nm)
            self.summary.near_miss_count += 1

        # Ambiguous relatives: ~1% of customers. Parent/child or siblings sharing
        # last name + address but different first name and phone. ML scores 0.60-0.75
        # due to address+surname overlap. LLM should produce UNCLEAR.
        n_relatives = int(self.num_customers * self.ambiguous_relative_rate)
        target_pool = [c for c in customers if c.has_loyalty and not c.borderline
                       and not c.ml_cross_source and not c.near_miss
                       and not c.email_mismatch and c.household_partner_id is None]
        random.shuffle(target_pool)
        for target in target_pool[:n_relatives]:
            # Relative: same last name, same address, different first/email/phone
            rel_first = self.faker.first_name().upper()
            # Ensure first name shares the same initial (makes it more ambiguous)
            while rel_first[0] != target.canonical_first_name[0]:
                rel_first = self.faker.first_name().upper()
            rel = CanonicalCustomer(
                customer_id=f"REL-{uuid.uuid4().hex[:12]}",
                canonical_first_name=rel_first,
                canonical_last_name=target.canonical_last_name,
                nickname_first_name=None,
                married_last_name=None,
                email=f"{rel_first.lower()}.{target.canonical_last_name.lower()}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}",
                phone=self._gen_phone_e164(),  # different phone
                street=target.street,  # SAME address
                city=target.city,
                state=target.state,
                postal=target.postal,
                country=target.country,
                has_loyalty=False,
                has_pos=False,
                has_shopify=True,  # only appears in Shopify
                has_web=True,
                loyalty_member_id=None,
                enrolled_at=None,
                cookie_id=f"c_{uuid.uuid4().hex[:16]}",
                device_id=f"d_{uuid.uuid4().hex[:12]}",
                uid2=None,
                rampid=None,
                cross_device_cookie_id=None,
                household_partner_id=target.customer_id,
                household_role='relative',
                borderline=False,
                email_mismatch=False,
                email_mismatch_variant=None,
                pos_alt_email=None,
                ambiguous_relative=True,
                ambiguous_relative_of=target.customer_id,
            )
            customers.append(rel)
            self.summary.ambiguous_relative_count += 1

        # Shared-line collisions: ~1% of customers. A DIFFERENT person (different
        # surname) who shares the target's phone number AND address — e.g. a
        # roommate on a shared landline. ML sees matching phone (and phone_last7)
        # plus shared address but strongly conflicting surnames; the scorer's
        # collision guard routes these into the GREY_LLM band, and the LLM should
        # REJECT them as the same individual (they are co-residents, not one person).
        n_collision = int(self.num_customers * self.collision_rate)
        target_pool = [c for c in customers if c.has_loyalty and not c.borderline
                       and not c.ml_cross_source and not c.near_miss
                       and not c.ambiguous_relative and not c.email_mismatch
                       and c.household_partner_id is None and c.phone]
        random.shuffle(target_pool)
        for target in target_pool[:n_collision]:
            twin_first = self.faker.first_name().upper()
            twin_last = self.faker.last_name().upper()
            # Force a genuine surname conflict (different initial -> JW well below 0.6)
            while twin_last[0] == target.canonical_last_name[0]:
                twin_last = self.faker.last_name().upper()
            twin = CanonicalCustomer(
                customer_id=f"COL-{uuid.uuid4().hex[:12]}",
                canonical_first_name=twin_first,
                canonical_last_name=twin_last,
                nickname_first_name=None,
                married_last_name=None,
                email=f"{twin_first.lower()}.{twin_last.lower()}{random.randint(10,99)}@{random.choice(['gmail.com','outlook.com','yahoo.com'])}",
                phone=target.phone,            # SHARED phone (landline) -> phone_last7 collision
                street=target.street,          # same address (co-resident)
                city=target.city,
                state=target.state,
                postal=target.postal,
                country=target.country,
                has_loyalty=False,
                has_pos=True,
                has_shopify=True,
                has_web=False,
                loyalty_member_id=None,
                enrolled_at=None,
                cookie_id=f"c_{uuid.uuid4().hex[:16]}",
                device_id=None,
                uid2=None,
                rampid=None,
                cross_device_cookie_id=None,
                household_partner_id=None,
                household_role=None,
                borderline=False,
                email_mismatch=False,
                email_mismatch_variant=None,
                pos_alt_email=None,
                shared_line_collision=True,
            )
            customers.append(twin)
            self.summary.collision_count += 1

        return customers

    # ---- row generators (no DB connection needed) --------------------------

    def _generate_pos_rows(self, customers: list[CanonicalCustomer]) -> list[tuple]:
        rows = []
        active_count = max(1, len(customers))
        per_customer = max(1, self.num_events // (4 * active_count))
        for c in customers:
            if not c.has_pos:
                continue
            for _ in range(per_customer):
                txn_id = f"TXN-{uuid.uuid4().hex[:12]}"
                ts = self.faker.date_time_between(start_date="-1y", end_date="now")
                scanned_loyalty = c.has_loyalty and (
                    c.email_mismatch or random.random() < 0.40
                )
                first_name = None
                last_name = None
                email = None
                phone = None
                loyalty_id = None
                if scanned_loyalty:
                    if c.email_mismatch:
                        v = c.email_mismatch_variant
                        if v == 'phone_only':
                            first_name, last_name = c.canonical_first_name, c.canonical_last_name
                        elif v == 'phone_nickname':
                            first_name = c.nickname_first_name or c.canonical_first_name
                            last_name = c.canonical_last_name
                        elif v == 'phone_fuzzy_first':
                            first_name = self._typo_name(c.canonical_first_name)
                            last_name = c.canonical_last_name
                        elif v == 'phone_fuzzy_last':
                            first_name = c.canonical_first_name
                            last_name = self._typo_name(c.canonical_last_name)
                        else:
                            first_name, last_name = c.canonical_first_name, c.canonical_last_name
                        email = c.pos_alt_email
                        phone = c.phone
                        loyalty_id = None
                    elif c.ml_cross_source:
                        # ML cross-source: different email domain, different area code,
                        # no loyalty_id. Name depends on tier.
                        email = c.ml_alt_email
                        phone = c.ml_alt_phone
                        loyalty_id = None
                        if c.ml_cross_source_tier == 'strong':
                            first_name = c.canonical_first_name
                            last_name = c.canonical_last_name
                        else:  # fuzzy
                            first_name = self._typo_name(c.canonical_first_name)
                            last_name = self._typo_name(c.canonical_last_name)
                    else:
                        use_nickname = c.nickname_first_name and random.random() < self.nickname_rate
                        first_name = c.nickname_first_name if use_nickname else c.canonical_first_name
                        last_name = c.canonical_last_name
                        if c.borderline:
                            first_name = self._typo_name(first_name)
                            last_name = self._typo_name(last_name)
                            # Suppress email/loyalty so ML can't shortcut via deterministic match
                            domain = random.choice(['gmail.com', 'yahoo.com', 'icloud.com'])
                            email = f"{c.canonical_first_name.lower()}{random.randint(100,999)}@{domain}"
                            phone = self._last_digit_phone(c.phone)
                            loyalty_id = None
                        else:
                            first_name = self._maybe_typo_name(first_name, self.pos_typo_rate)
                            last_name = self._maybe_typo_name(last_name, self.pos_typo_rate)
                            email = c.email
                            phone = c.phone
                            loyalty_id = c.loyalty_member_id
                rows.append((
                    txn_id,
                    f"S-{c.state}-{random.randint(1,99):02d}",
                    str(ts),
                    random.choice(["CARD", "APPLE_PAY", "GOOGLE_PAY", "CASH"]),
                    round(random.uniform(5, 500), 2),
                    loyalty_id, email, phone, first_name, last_name,
                    json.dumps([{"sku": f"PROD-{random.randint(1,999):03d}", "qty": 1, "price": round(random.uniform(5,200),2)}]),
                    "synthetic_pos.csv",
                ))
        return rows

    def _generate_loyalty_rows(self, customers: list[CanonicalCustomer]) -> list[tuple]:
        rows = []
        for c in customers:
            if not c.has_loyalty:
                continue
            rows.append((
                c.loyalty_member_id, c.email, c.phone,
                c.canonical_first_name, c.canonical_last_name,
                c.street, c.city, c.state, c.postal, c.country,
                random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"]),
                random.randint(0, 50000),
                str(c.enrolled_at), "synthetic_loyalty.csv",
            ))
        return rows

    def _generate_web_rows(self, customers: list[CanonicalCustomer]) -> list[tuple]:
        rows = []
        active_count = max(1, len(customers))
        per_customer = max(2, self.num_events // (4 * active_count))
        for c in customers:
            if not c.has_web:
                continue
            for _ in range(per_customer):
                event_id = f"EVT-{uuid.uuid4().hex[:12]}"
                ts = self.faker.date_time_between(start_date="-6m", end_date="now")
                cookie = c.cross_device_cookie_id if (c.cross_device_cookie_id and random.random() < 0.5) else c.cookie_id
                logged_in = random.random() < self.anon_to_known_rate
                emit_email = c.email if logged_in else None
                emit_phone = (c.phone if logged_in and random.random() < 0.3 else None)
                if c.borderline and emit_phone:
                    emit_phone = self._last_digit_phone(emit_phone)
                rows.append((
                    event_id, str(ts),
                    cookie, c.device_id, c.uid2, c.rampid,
                    f"sess-{uuid.uuid4().hex[:8]}",
                    "Mozilla/5.0 ...", self.faker.ipv4(),
                    random.choice(["page_view", "add_to_cart", "search", "purchase", "login"]),
                    f"/products/{random.randint(1,999)}",
                    emit_email, emit_phone,
                    c.loyalty_member_id if (logged_in and c.has_loyalty) else None,
                    json.dumps({"product_id": f"PROD-{random.randint(1,999):03d}"}),
                    "synthetic_clickstream.csv",
                ))
        return rows

    def _generate_shopify_rows(self, customers: list[CanonicalCustomer]) -> list[tuple]:
        rows = []
        active_count = max(1, len(customers))
        per_customer = max(1, self.num_events // (8 * active_count))
        for c in customers:
            if not c.has_shopify:
                continue
            for _ in range(per_customer):
                order_id = f"SHO-{uuid.uuid4().hex[:12]}"
                ts = self.faker.date_time_between(start_date="-1y", end_date="now")

                if c.ml_cross_source:
                    # ML cross-source: alt email/phone, no loyalty, tier-specific name
                    email = c.ml_alt_email
                    phone = c.ml_alt_phone
                    loyalty_id_val = ""
                    if c.ml_cross_source_tier == 'strong':
                        first_name = c.canonical_first_name
                        last_name = c.canonical_last_name
                        billing_street = self._abbreviate_street(c.street)
                        shipping_street = billing_street
                    else:  # fuzzy
                        first_name = self._typo_name(c.canonical_first_name)
                        last_name = self._typo_name(c.canonical_last_name)
                        billing_street = self.faker.street_address()
                        shipping_street = billing_street
                else:
                    use_nickname = c.nickname_first_name and random.random() < (self.nickname_rate * 0.75)
                    first_name = c.nickname_first_name if use_nickname else c.canonical_first_name
                    last_name = c.married_last_name or c.canonical_last_name
                    if c.borderline:
                        first_name = self._typo_name(first_name)
                        last_name = self._typo_name(last_name)
                    else:
                        first_name = self._maybe_typo_name(first_name, self.shopify_typo_rate)
                        last_name = self._maybe_typo_name(last_name, self.shopify_typo_rate)
                    billing_street = self._abbreviate_street(c.street) if c.borderline else c.street
                    shipping_street = self._abbreviate_street(c.street) if c.borderline else c.street
                    phone = self._last_digit_phone(c.phone) if c.borderline else c.phone
                    email = c.email
                    loyalty_id_val = c.loyalty_member_id or ""

                payload = {
                    "id": order_id,
                    "customer": {
                        "id": c.customer_id,
                        "email": email,
                        "phone": phone,
                        "first_name": first_name,
                        "last_name": last_name,
                    },
                    "billing_address": {
                        "address1": billing_street, "city": c.city,
                        "province": c.state, "zip": c.postal, "country": c.country,
                    },
                    "shipping_address": {
                        "address1": shipping_street, "city": c.city,
                        "province": c.state, "zip": c.postal, "country": c.country,
                    },
                    "client_details": {
                        "user_agent": "Mozilla/5.0 ...",
                        "browser_ip": self.faker.ipv4(),
                        "session_hash": uuid.uuid4().hex[:16],
                    },
                    "note_attributes": [
                        {"name": "loyalty_member_number", "value": loyalty_id_val},
                    ],
                    "landing_site": f"/?utm_source=google&gclid=Cj{uuid.uuid4().hex[:20]}",
                }
                rows.append((
                    order_id, random.randint(1000, 99999), str(ts),
                    round(random.uniform(20, 800), 2),
                    "paid", json.dumps(payload), "synthetic_shopify.csv",
                ))
        return rows

    # ---- helpers ----------------------------------------------------------

    def _gen_phone_e164(self) -> str:
        return f"+1{random.randint(2000000000, 9999999999)}"

    def _typo_name(self, name: str) -> str:
        """Single-character substitution at an interior position. Targets
        Jaro-Winkler ~0.92-0.96 vs the canonical (interior typo, prefix shared)."""
        if not name or len(name) <= 3:
            return name
        pos = random.randint(1, len(name) - 2)
        repl = random.choice("AEIOULMNRT")
        if name[pos] == repl:
            repl = "X" if repl != "X" else "Q"
        return name[:pos] + repl + name[pos + 1:]

    def _maybe_typo_name(self, name: str, rate: float) -> str:
        if random.random() < rate:
            return self._typo_name(name)
        return name

    def _abbreviate_street(self, street: str) -> str:
        if not street:
            return street
        replacements = [
            ("Street", "St"), ("Avenue", "Ave"),
            ("Boulevard", "Blvd"), ("Road", "Rd"),
            ("Drive", "Dr"), ("Lane", "Ln"),
            ("Court", "Ct"), ("Place", "Pl"),
            ("Apt ", "# "), ("Suite ", "Ste "),
        ]
        out = street
        for src, dst in replacements:
            out = out.replace(src, dst)
        return out

    def _last_digit_phone(self, phone: str) -> str:
        if not phone or not phone[-1].isdigit():
            return phone
        return phone[:-1] + str((int(phone[-1]) + 1) % 10)

    def _connect(self) -> Any:
        kwargs: dict[str, Any] = {"database": self.db_name, "schema": "BRONZE"}
        if self.connection_name:
            kwargs["connection_name"] = self.connection_name
        else:
            spcs_token_path = "/snowflake/session/token"
            if os.path.exists(spcs_token_path):
                with open(spcs_token_path) as f:
                    kwargs["token"] = f.read()
                kwargs["authenticator"] = "oauth"
                kwargs["host"] = os.environ["SNOWFLAKE_HOST"]
                kwargs["account"] = os.environ["SNOWFLAKE_ACCOUNT"]
                # COPY INTO needs an active warehouse; the OAuth session has none
                # by default. Mirror the backend app (main.py) which sets it.
                kwargs["warehouse"] = (
                    os.environ.get("SNOWFLAKE_WAREHOUSE")
                    or os.environ.get("MARTECH_WAREHOUSE")
                    or "MARTECH_WH"
                )
        return snowflake.connector.connect(**kwargs)

    # ---- file I/O ----------------------------------------------------------

    def _write_csv(self, path: Path, rows: list[tuple], cols: list[str]) -> Path:
        with open(path, "w", newline="") as f:
            w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            w.writerow(cols)
            for row in rows:
                w.writerow("" if v is None else v for v in row)
        return path

    # ---- parallel PUT + COPY -----------------------------------------------

    def _parallel_put_copy(self, pos_file: Path, loyalty_file: Path, web_file: Path, shopify_file: Path) -> None:
        stage = f"@{self.db_name}.BRONZE.SAMPLE_DATA_STAGE"
        tasks = [
            (pos_file, "POS_TRANSACTION_RAW", self.POS_COLS),
            (loyalty_file, "LOYALTY_MEMBER_RAW", self.LOYALTY_COLS),
            (web_file, "WEB_CLICKSTREAM_RAW", self.WEB_COLS),
            (shopify_file, "SHOPIFY_ORDER_RAW", self.SHOPIFY_COLS),
        ]

        def _put_copy(file: Path, table: str, cols: list[str]) -> int:
            conn = self._connect()
            cur = conn.cursor()
            try:
                cur.execute(f"PUT 'file://{file}' '{stage}/generated/' AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                col_select = ", ".join(
                    f"PARSE_JSON(${i+1})" if c in self._JSON_COLS else f"${i+1}"
                    for i, c in enumerate(cols)
                )
                cur.execute(f"""
                    COPY INTO {self.db_name}.BRONZE.{table} ({', '.join(cols)})
                    FROM (SELECT {col_select} FROM '{stage}/generated/{file.name}.gz')
                    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                                   NULL_IF = ('') EMPTY_FIELD_AS_NULL = TRUE)
                    ON_ERROR = 'CONTINUE'
                    FORCE = TRUE
                """)
                loaded = cur.rowcount or 0
                cur.execute(f"REMOVE '{stage}/generated/{file.name}.gz'")
                return loaded
            finally:
                cur.close()
                conn.close()

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(_put_copy, f, t, c): t for f, t, c in tasks}
            errors = []
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    errors.append(f"{futures[fut]}: {e}")
            if errors:
                self.summary.errors.extend(errors)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Martech synthetic data generator")
    p.add_argument("--db", default=None, help="Snowflake database (default: $MARTECH_DB or MARTECH)")
    p.add_argument("--customers", type=int, default=10_000)
    p.add_argument("--events", type=int, default=100_000)
    p.add_argument("--connection-name", default=None, help="snowflake.connector named connection")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--nickname-rate", type=float, default=0.08)
    p.add_argument("--pos-typo-rate", type=float, default=0.05)
    p.add_argument("--shopify-typo-rate", type=float, default=0.05)
    p.add_argument("--married-name-rate", type=float, default=0.02)
    p.add_argument("--household-spouse-rate", type=float, default=0.15)
    p.add_argument("--household-roommate-rate", type=float, default=0.05)
    p.add_argument("--borderline-rate", type=float, default=0.02)
    p.add_argument("--email-mismatch-rate", type=float, default=0.06)
    args = p.parse_args()
    gen = MartechDataGenerator(
        db_name=args.db,
        num_customers=args.customers,
        num_events=args.events,
        connection_name=args.connection_name,
        seed=args.seed,
        nickname_rate=args.nickname_rate,
        pos_typo_rate=args.pos_typo_rate,
        shopify_typo_rate=args.shopify_typo_rate,
        married_name_rate=args.married_name_rate,
        household_spouse_rate=args.household_spouse_rate,
        household_roommate_rate=args.household_roommate_rate,
        borderline_rate=args.borderline_rate,
        email_mismatch_rate=args.email_mismatch_rate,
    )
    summary = gen.run_all()
    print(json.dumps(summary.__dict__, default=str, indent=2))


if __name__ == "__main__":
    main()
