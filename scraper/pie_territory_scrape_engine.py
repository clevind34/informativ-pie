#!/usr/bin/env python3
"""
PIE Territory Scrape Engine — Part 1
=====================================
Automated lead generation via Browserbase scraping, targeting OEM dealer locators
by territory. Cross-references against client suppression list and outputs
Rev Ops-formatted CSV for PIE dashboard ingestion.

Pipeline: Territory Gap → OEM Target Queue → Browserbase Scrape → Suppression → CSV Export → Reconciliation

Usage:
    python pie_territory_scrape_engine.py --territory "New Jersey" --oem "Toyota,Honda"
    python pie_territory_scrape_engine.py --all-gaps --max-per-territory 50
    python pie_territory_scrape_engine.py --reconciliation  # Generate reconciliation report
"""

import json
import os
import csv
import re
import hashlib
import asyncio
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

# --- Configuration ---
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "Data Files"
OUTPUT_DIR = BASE_DIR / "Scrape_Outputs"

BROWSERBASE_API_KEY = "bb_live_OtM1ymu6MmsYWnDemBv6xPbJGSM"
BROWSERBASE_PROJECT_ID = "23dd4a81-dcb8-4d94-a5cf-a748f7787719"
BROWSERBASE_CONNECT_URL = "wss://connect.browserbase.com"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("PIE-Scraper")

# --- Territory Definitions (from US Territory Map) ---
TERRITORY_MAP = {
    "California": {
        "rep": "Ashton Gough",
        "rep_email": "agough@informativ.com",
        "rep_phone": "(919) 706-2421",
        "states": ["CA"],
    },
    "Pacific West": {
        "rep": "Carol Sanford",
        "rep_email": "csanford@informativ.com",
        "rep_phone": "(818) 359-1253",
        "states": ["AZ", "AK", "CO", "HI", "ID", "NV", "UT", "OR", "WA"],
    },
    "Central": {
        "rep": "William Fowler",
        "rep_email": "wfowler@informativ.com",
        "rep_phone": "(816) 604-7993",
        "states": ["MO", "KS", "IA", "NE", "ND", "SD", "WY", "MT"],
    },
    "Central Atlantic": {
        "rep": "Ward Carroll",
        "rep_email": "wcarroll@informativ.com",
        "rep_phone": "(919) 210-2519",
        "states": ["NC", "SC", "VA", "WV"],
    },
    "North East": {
        "rep": "David Castone",
        "rep_email": "dcastone@informativ.com",
        "rep_phone": "(484) 357-3793",
        "states": ["DE", "MD", "PA", "VT", "NH", "ME"],
    },
    "South Central": {
        "rep": "Trish Smith",
        "rep_email": "tsmith@informati.com",
        "rep_phone": "(949) 324-1627",
        "states": ["TX", "OK", "NM"],
    },
    "Great Lakes West": {
        "rep": "Matt Hanaman",
        "rep_email": "mhanaman@informativ.com",
        "rep_phone": "(815) 414-0886",
        "states": ["IL", "MN", "WI"],
    },
    "Great Lakes East": {
        "rep": "Loren Rivera",
        "rep_email": "lrivera@informativ.com",
        "rep_phone": "(440) 812-1199",
        "states": ["OH", "IN", "MI"],
    },
    "NY Metro": {
        "rep": "Tom Meredith",
        "rep_email": "tmeredith@informativ.com",
        "rep_phone": "(617) 833-9121",
        "states": ["NY", "MA", "CT", "RI"],
    },
    "South East": {
        "rep": "Tami Richter",
        "rep_email": "trichter@informativ.com",
        "rep_phone": "(614) 296-8828",
        "states": ["FL", "AL", "GA"],
    },
    "Southern Crossroads": {
        "rep": "Brock Hutchinson",
        "rep_email": "bhutchinson@informativ.com",
        "rep_phone": "(863) 602-6712",
        "states": ["AR", "TN", "KY", "MS", "LA"],
    },
    "New Jersey": {
        "rep": "Crly Alvarado",
        "rep_email": "calvarado@informativ.com",
        "rep_phone": "(800) 448-0183 x104",
        "states": ["NJ"],
    },
}

# Build reverse lookup
STATE_TO_TERRITORY = {}
for terr_name, terr_info in TERRITORY_MAP.items():
    for st in terr_info["states"]:
        STATE_TO_TERRITORY[st] = terr_name

# --- OEM Dealer Locator URLs ---
OEM_LOCATORS = {
    # Domestic
    "Ford": {"url": "https://www.ford.com/dealerships/", "brand_group": "Domestic"},
    "Lincoln": {"url": "https://www.lincoln.com/dealerships/", "brand_group": "Domestic"},
    "Chevrolet": {"url": "https://www.chevrolet.com/dealer-locator", "brand_group": "Domestic"},
    "GMC": {"url": "https://www.gmc.com/dealer-locator", "brand_group": "Domestic"},
    "Buick": {"url": "https://www.buick.com/dealer-locator", "brand_group": "Domestic"},
    "Cadillac": {"url": "https://www.cadillac.com/dealer-locator", "brand_group": "Domestic"},
    "Chrysler": {"url": "https://www.mopar.com/en-us/dealerships.html", "brand_group": "Domestic"},
    "Dodge": {"url": "https://www.mopar.com/en-us/dealerships.html", "brand_group": "Domestic"},
    "Jeep": {"url": "https://www.mopar.com/en-us/dealerships.html", "brand_group": "Domestic"},
    "Ram": {"url": "https://www.mopar.com/en-us/dealerships.html", "brand_group": "Domestic"},
    # Japanese
    "Toyota": {"url": "https://www.toyota.com/dealers/", "brand_group": "Japanese"},
    "Lexus": {"url": "https://www.lexus.com/dealers", "brand_group": "Japanese"},
    "Honda": {"url": "https://automobiles.honda.com/tools/dealership-locator", "brand_group": "Japanese"},
    "Acura": {"url": "https://www.acura.com/dealer-locator", "brand_group": "Japanese"},
    "Nissan": {"url": "https://www.nissanusa.com/dealer-locator", "brand_group": "Japanese"},
    "Infiniti": {"url": "https://www.infinitiusa.com/dealer-locator", "brand_group": "Japanese"},
    "Mazda": {"url": "https://www.mazdausa.com/find-a-dealer", "brand_group": "Japanese"},
    "Subaru": {"url": "https://www.subaru.com/find-a-retailer", "brand_group": "Japanese"},
    "Mitsubishi": {"url": "https://www.mitsubishicars.com/find-a-dealer", "brand_group": "Japanese"},
    # Korean
    "Hyundai": {"url": "https://www.hyundaiusa.com/us/en/dealer-locator", "brand_group": "Korean"},
    "Kia": {"url": "https://www.kia.com/us/en/find-a-dealer", "brand_group": "Korean"},
    "Genesis": {"url": "https://www.genesis.com/us/en/retailers", "brand_group": "Korean"},
    # European
    "Volkswagen": {"url": "https://www.vw.com/en/dealers.html", "brand_group": "European"},
    "Audi": {"url": "https://www.audiusa.com/us/web/en/dealer-search.html", "brand_group": "European"},
    "BMW": {"url": "https://www.bmwusa.com/dealer-locator.html", "brand_group": "European"},
    "Mini": {"url": "https://www.miniusa.com/dealer-locator", "brand_group": "European"},
    "Mercedes-Benz": {"url": "https://www.mbusa.com/en/dealers", "brand_group": "European"},
    "Volvo": {"url": "https://www.volvocars.com/us/dealers", "brand_group": "European"},
}

# Buyer persona target titles (from Informativ ICP)
BUYER_PERSONAS = {
    "primary": [
        "General Manager", "Dealer Principal", "Owner",
        "Chief Operating Officer", "COO", "Managing Partner",
    ],
    "secondary": [
        "Finance Director", "F&I Director", "Finance Manager",
        "F&I Manager", "Corporate Finance Director",
        "Compliance Officer", "Compliance Manager",
    ],
    "influencer": [
        "Sales Manager", "General Sales Manager", "GSM",
        "Internet Director", "Internet Sales Manager",
        "BDC Director", "BDC Manager",
        "IT Director", "IT Manager",
    ],
}

# All target titles flattened for search
ALL_TARGET_TITLES = []
for tier_titles in BUYER_PERSONAS.values():
    ALL_TARGET_TITLES.extend(tier_titles)


# --- Data Classes ---
@dataclass
class ScrapeTarget:
    """Represents a single dealer to scrape from an OEM locator."""
    territory: str
    state: str
    oem_brand: str
    locator_url: str
    assigned_rep: str
    priority: int = 0  # Higher = more urgent (based on territory deficit)


@dataclass
class ScrapedDealer:
    """Result from scraping a single dealership."""
    # Core identity
    dealership_name: str = ""
    website_url: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""
    oem_brand: str = ""
    dealer_group: str = ""

    # Contacts (buyer personas)
    contact_name: str = ""
    contact_title: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    additional_contacts: list = field(default_factory=list)

    # Inventory & Volume
    new_inventory_count: Optional[int] = None
    used_inventory_count: Optional[int] = None
    total_inventory_count: Optional[int] = None
    estimated_vehicles_sold: Optional[int] = None
    inventory_url: str = ""

    # Metadata
    territory: str = ""
    assigned_rep: str = ""
    source: str = "Browserbase_OEM_Scrape"
    scrape_timestamp: str = ""
    scrape_method: str = ""
    confidence: str = "low"

    # Suppression
    is_existing_client: bool = False
    suppression_reason: str = ""
    suppression_match: str = ""

    # Data completeness tracking
    fields_captured: list = field(default_factory=list)
    fields_missing: list = field(default_factory=list)

    def compute_completeness(self):
        """Track which fields were populated vs blank."""
        required_fields = [
            "dealership_name", "website_url", "city", "state", "phone",
            "oem_brand", "contact_name", "contact_title", "contact_email",
            "new_inventory_count", "estimated_vehicles_sold",
        ]
        self.fields_captured = []
        self.fields_missing = []
        for f in required_fields:
            val = getattr(self, f, None)
            if val and val != "" and val != 0:
                self.fields_captured.append(f)
            else:
                self.fields_missing.append(f)
        return len(self.fields_captured) / len(required_fields) * 100

    def generate_prospect_id(self):
        """Generate deterministic prospect ID from dealer name + state."""
        raw = f"{self.dealership_name}_{self.state}_{self.oem_brand}".lower()
        return hashlib.md5(raw.encode()).hexdigest()[:16]


# ===========================================================================
# SECTION 1: TERRITORY GAP ANALYSIS
# ===========================================================================

class TerritoryGapAnalyzer:
    """Analyze PIE prospect data to identify territory gaps."""

    def __init__(self):
        self.prospects = []
        self.customers = []
        self._load_data()

    def _load_data(self):
        scored_path = DATA_DIR / "pie_scored_prospects.json"
        profiles_path = DATA_DIR / "pie_unified_profiles.json"

        if scored_path.exists():
            with open(scored_path) as f:
                data = json.load(f)
                self.prospects = data.get("prospects", data) if isinstance(data, dict) else data

        if profiles_path.exists():
            with open(profiles_path) as f:
                data = json.load(f)
                self.customers = data.get("profiles", data) if isinstance(data, dict) else data

    def analyze_gaps(self):
        """Return territory gap analysis with deficit calculations."""
        terr_counts = {}
        for p in self.prospects:
            state = p.get("state", "")
            terr = STATE_TO_TERRITORY.get(state, "Unassigned")
            if terr not in terr_counts:
                terr_counts[terr] = {"net_new": 0, "cross_sell": 0, "total": 0}
            terr_counts[terr]["total"] += 1
            if p.get("prospect_type") == "net_new":
                terr_counts[terr]["net_new"] += 1
            else:
                terr_counts[terr]["cross_sell"] += 1

        # Calculate average (excluding unassigned)
        assigned_total = sum(
            d["total"] for t, d in terr_counts.items() if t != "Unassigned"
        )
        avg = assigned_total / len(TERRITORY_MAP) if TERRITORY_MAP else 0

        gaps = []
        for terr_name, terr_info in TERRITORY_MAP.items():
            count = terr_counts.get(terr_name, {"total": 0, "net_new": 0})["total"]
            deficit = max(0, avg - count)
            gaps.append({
                "territory": terr_name,
                "rep": terr_info["rep"],
                "states": terr_info["states"],
                "current_prospects": count,
                "average": round(avg),
                "deficit": round(deficit),
                "priority": round(deficit),  # Simple priority = deficit size
                "underserved": count < avg,
            })

        # Sort by deficit descending
        gaps.sort(key=lambda x: -x["deficit"])
        return gaps

    def get_customer_names_by_state(self, states):
        """Get existing customer names for suppression in given states."""
        names = set()
        for c in self.customers:
            st = c.get("state", "")
            if st in states:
                name = c.get("company_name", "") or c.get("account_name", "") or ""
                if name:
                    names.add(self._normalize_name(name))
        return names

    @staticmethod
    def _normalize_name(name):
        """Normalize dealer name for matching."""
        name = name.lower().strip()
        for suffix in ["inc", "llc", "corp", "ltd", "co", "group",
                        "auto", "automotive", "dealership", "motors",
                        "of ", "the "]:
            name = name.replace(suffix, "")
        return re.sub(r'\s+', ' ', name).strip()


# ===========================================================================
# SECTION 2: OEM TARGET QUEUE BUILDER
# ===========================================================================

class OEMTargetQueueBuilder:
    """Build prioritized scrape queue based on territory gaps and OEM coverage."""

    # Major zip codes per state for OEM locator searches
    STATE_MAJOR_ZIPS = {
        "NJ": ["07001", "07102", "08501", "07601", "08701"],
        "NY": ["10001", "11201", "10601", "12207", "14201"],
        "MA": ["02101", "01601", "01201"],
        "CT": ["06101", "06510", "06901"],
        "RI": ["02901"],
        "PA": ["19101", "15201", "18101", "17101"],
        "DE": ["19901", "19801"],
        "MD": ["21201", "20601", "21401"],
        "VT": ["05401", "05301"],
        "NH": ["03101", "03301"],
        "ME": ["04101", "04401"],
        "NC": ["27601", "28201", "27401", "28801"],
        "SC": ["29201", "29401", "29601"],
        "VA": ["23219", "22301", "23451", "24011"],
        "WV": ["25301", "26501"],
        "FL": ["33101", "32801", "33601", "32301", "34201"],
        "AL": ["35201", "36601", "35801"],
        "GA": ["30301", "31401", "30901", "30501"],
        "TX": ["75201", "77001", "78201", "73301", "76101"],
        "OK": ["73101", "74101"],
        "NM": ["87101", "88001"],
        "CA": ["90001", "94101", "92101", "95814", "93301"],
        "AZ": ["85001", "85701"],
        "CO": ["80201", "80901"],
        "NV": ["89101", "89501"],
        "OR": ["97201", "97401"],
        "WA": ["98101", "99201"],
        "UT": ["84101", "84401"],
        "ID": ["83701", "83201"],
        "AK": ["99501"],
        "HI": ["96801"],
        "MO": ["63101", "64101"],
        "KS": ["66101", "67201"],
        "IA": ["50301", "52401"],
        "NE": ["68101", "68501"],
        "ND": ["58501", "58101"],
        "SD": ["57101", "57701"],
        "WY": ["82001", "82601"],
        "MT": ["59601", "59101"],
        "IL": ["60601", "61801", "62701"],
        "MN": ["55401", "55101"],
        "WI": ["53201", "53701"],
        "OH": ["43201", "44101", "45201"],
        "IN": ["46201", "46801"],
        "MI": ["48201", "49501", "48601"],
        "AR": ["72201", "72701"],
        "TN": ["37201", "38101", "37901"],
        "KY": ["40201", "41001"],
        "MS": ["39201", "39501"],
        "LA": ["70112", "71101", "70501"],
    }

    # Priority OEMs for scraping (highest volume / best Informativ fit)
    PRIORITY_OEMS = [
        "Toyota", "Honda", "Ford", "Chevrolet", "Nissan",
        "Hyundai", "Kia", "Dodge", "GMC", "Subaru",
        "Volkswagen", "Mazda", "BMW", "Mercedes-Benz", "Audi",
    ]

    def build_queue(self, gaps, target_oems=None, max_per_territory=50):
        """Build prioritized scrape queue from territory gaps.

        Args:
            gaps: Territory gap analysis from TerritoryGapAnalyzer
            target_oems: List of OEM brands to target (None = all priority OEMs)
            max_per_territory: Max dealers to target per territory

        Returns:
            List of ScrapeTarget objects, prioritized by territory deficit
        """
        oems = target_oems or self.PRIORITY_OEMS
        queue = []

        for gap in gaps:
            if not gap["underserved"]:
                continue

            terr_name = gap["territory"]
            rep = gap["rep"]
            states = gap["states"]
            priority = gap["priority"]

            # Distribute targets across OEMs proportionally
            targets_per_oem = max(3, max_per_territory // len(oems))

            for oem in oems:
                if oem not in OEM_LOCATORS:
                    continue

                locator = OEM_LOCATORS[oem]

                for state in states:
                    zips = self.STATE_MAJOR_ZIPS.get(state, [])
                    if not zips:
                        continue

                    queue.append(ScrapeTarget(
                        territory=terr_name,
                        state=state,
                        oem_brand=oem,
                        locator_url=locator["url"],
                        assigned_rep=rep,
                        priority=priority,
                    ))

        # Sort by priority (highest deficit first)
        queue.sort(key=lambda x: -x.priority)

        logger.info(f"Built scrape queue: {len(queue)} targets across "
                    f"{len(set(t.territory for t in queue))} territories")
        return queue


# ===========================================================================
# SECTION 3: BROWSERBASE SCRAPER (Enhanced)
# ===========================================================================

class BrowserbaseDealerScraper:
    """
    Enhanced Browserbase scraper for OEM dealer locators.

    Workflow per OEM/state:
    1. Navigate to OEM dealer locator URL
    2. Enter state/zip in search
    3. Extract dealer listings (name, address, phone)
    4. For each dealer: navigate to dealer website
    5. Scrape inventory page for vehicle counts
    6. Look for staff/team page for key contacts
    7. Estimate monthly volume from inventory
    """

    INVENTORY_RATIO = 0.588  # Calibrated: vehicles_sold / inventory_count
    CONTACT_PAGE_PATHS = [
        "/about-us", "/our-team", "/staff", "/meet-our-team",
        "/about", "/contact", "/contact-us", "/leadership",
        "/our-staff", "/team", "/management",
    ]
    INVENTORY_PAGE_PATHS = [
        "/new-inventory", "/new-vehicles", "/searchnew.aspx",
        "/new", "/inventory/new", "/vehicles/new",
        "/search/new", "/new-cars", "/new-trucks",
    ]
    USED_INVENTORY_PATHS = [
        "/used-inventory", "/used-vehicles", "/searchused.aspx",
        "/used", "/inventory/used", "/vehicles/used",
        "/pre-owned", "/certified-pre-owned",
    ]

    def __init__(self, api_key=None, project_id=None, mode="real"):
        """
        Args:
            api_key: Browserbase API key
            project_id: Browserbase project ID
            mode: 'real' for live Browserbase, 'simulated' for testing
        """
        self.api_key = api_key or BROWSERBASE_API_KEY
        self.project_id = project_id or BROWSERBASE_PROJECT_ID
        self.mode = mode
        self.session_minutes_used = 0
        self.max_session_minutes = 6000  # 100 hours
        self.results = []

    async def scrape_oem_locator(self, target: ScrapeTarget, zip_codes: list):
        """Scrape an OEM dealer locator for dealers in given zip codes.

        Returns list of ScrapedDealer objects.
        """
        dealers = []

        if self.mode == "real":
            dealers = await self._scrape_real(target, zip_codes)
        else:
            dealers = self._scrape_simulated(target, zip_codes)

        return dealers

    async def _scrape_real(self, target: ScrapeTarget, zip_codes: list):
        """Real Browserbase scraping via Playwright."""
        dealers = []

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed. Falling back to simulated mode.")
            return self._scrape_simulated(target, zip_codes)

        try:
            async with async_playwright() as pw:
                # Connect to Browserbase with stealth settings
                connect_url = (
                    f"{BROWSERBASE_CONNECT_URL}?apiKey={self.api_key}"
                    f"&projectId={self.project_id}"
                    f"&enableProxy=true"
                )
                browser = await pw.chromium.connect_over_cdp(connect_url)
                context = browser.contexts[0]
                page = context.pages[0] if context.pages else await context.new_page()

                # Set realistic viewport and user agent
                await page.set_viewport_size({"width": 1440, "height": 900})

                for zip_code in zip_codes:
                    try:
                        zip_dealers = await self._scrape_locator_page(
                            page, target, zip_code
                        )
                        dealers.extend(zip_dealers)
                        self.session_minutes_used += 2  # Estimate 2 min per zip

                        if self.session_minutes_used >= self.max_session_minutes:
                            logger.warning("Approaching Browserbase session limit!")
                            break

                    except Exception as e:
                        logger.error(f"Error scraping {target.oem_brand} in {zip_code}: {e}")
                        continue

                await browser.close()

        except Exception as e:
            logger.error(f"Browserbase connection failed: {e}")
            logger.info("Falling back to simulated mode")
            return self._scrape_simulated(target, zip_codes)

        return dealers

    async def _scrape_locator_page(self, page, target: ScrapeTarget, zip_code: str):
        """Navigate OEM locator and extract dealer listings for a zip code."""
        dealers = []
        url = target.locator_url

        # Use domcontentloaded instead of networkidle — SPA sites like Toyota/Ford
        # have persistent analytics requests that never fully settle, causing 30s timeouts.
        # Retry once with increased timeout if first attempt fails.
        for attempt in range(2):
            try:
                timeout = 45000 if attempt == 0 else 60000
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_timeout(3000)  # Let SPA hydrate
                break
            except Exception as nav_err:
                if attempt == 0:
                    logger.warning(f"Navigation attempt 1 failed for {url}, retrying: {nav_err}")
                    await page.wait_for_timeout(2000)
                else:
                    logger.error(f"Navigation failed after 2 attempts for {url}: {nav_err}")
                    return dealers

        # Expanded zip selectors — covers Toyota, Honda, Ford, GM, Hyundai, etc.
        zip_selectors = [
            'input[placeholder*="zip"]', 'input[placeholder*="ZIP"]',
            'input[placeholder*="Zip"]',
            'input[placeholder*="location"]', 'input[placeholder*="Location"]',
            'input[placeholder*="city"]', 'input[placeholder*="City"]',
            'input[placeholder*="address"]', 'input[placeholder*="Address"]',
            'input[placeholder*="Enter"]',
            'input[placeholder*="Search"]', 'input[placeholder*="search"]',
            'input[name*="zip"]', 'input[name*="location"]',
            'input[name*="search"]', 'input[name*="query"]',
            'input[name*="dealer"]', 'input[name*="address"]',
            'input[type="search"]',
            'input[aria-label*="zip"]', 'input[aria-label*="Zip"]',
            'input[aria-label*="location"]', 'input[aria-label*="Location"]',
            'input[aria-label*="search"]', 'input[aria-label*="Search"]',
            'input[aria-label*="dealer"]', 'input[aria-label*="Dealer"]',
            'input[aria-label*="Find"]', 'input[aria-label*="find"]',
            'input[aria-label*="Enter"]',
            '#zip', '#zipcode', '#location-input',
            '#dealerSearch', '#dealer-search',
            '#searchInput', '#search-input',
            '[data-testid*="zip"]', '[data-testid*="search"]',
            '[data-testid*="location"]',
            # Honda-specific selectors
            'input.search-input', 'input.dealer-search',
            '#hondaDealerSearch', '.dealer-locator input[type="text"]',
            '.search-box input', '.locator-search input',
            # Broader fallbacks
            'form input[type="text"]:first-of-type',
        ]

        filled = False
        for sel in zip_selectors:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    await el.click()
                    await el.fill("")  # Clear first
                    await el.fill(zip_code)
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(4000)  # Allow results to load
                    filled = True
                    break
            except Exception:
                continue

        if not filled:
            # Last resort: try clicking any visible search button after page load
            logger.warning(f"Could not find zip input on {url}, trying URL-based search")
            # Some locators accept zip via URL parameter
            url_with_zip = f"{url}?zipcode={zip_code}&zip={zip_code}"
            try:
                await page.goto(url_with_zip, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(4000)
            except Exception:
                logger.warning(f"URL-based search also failed for {url}")
                return dealers

        # Extract dealer cards
        dealer_selectors = [
            '[class*="dealer-card"]', '[class*="dealer-result"]',
            '[class*="location-card"]', '[class*="dealer-info"]',
            '[class*="retailer-card"]', '[data-type="dealer"]',
            '.dealer', '.location-result', '.search-result',
        ]

        for sel in dealer_selectors:
            elements = await page.query_selector_all(sel)
            if elements:
                for el in elements[:20]:  # Cap at 20 per zip
                    try:
                        dealer = await self._extract_dealer_from_card(
                            el, target, zip_code
                        )
                        if dealer and dealer.dealership_name:
                            dealers.append(dealer)
                    except Exception:
                        continue
                break

        # If no structured results, try extracting from page text
        if not dealers:
            dealers = await self._extract_from_page_text(page, target, zip_code)

        return dealers

    async def _extract_dealer_from_card(self, element, target, zip_code):
        """Extract dealer info from a locator result card."""
        dealer = ScrapedDealer()
        dealer.oem_brand = target.oem_brand
        dealer.territory = target.territory
        dealer.assigned_rep = target.assigned_rep
        dealer.state = target.state
        dealer.source = f"Browserbase_OEM_{target.oem_brand}"
        dealer.scrape_timestamp = datetime.now(timezone.utc).isoformat()
        dealer.scrape_method = "oem_locator_card"

        text = await element.inner_text()
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        if lines:
            dealer.dealership_name = lines[0]

        # Extract phone
        phone_match = re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)
        if phone_match:
            dealer.phone = phone_match.group()

        # Extract address components
        addr_match = re.search(
            r'(\d+\s+[\w\s]+(?:St|Ave|Blvd|Rd|Dr|Ln|Way|Pkwy|Hwy|Ct)\.?)',
            text, re.IGNORECASE
        )
        if addr_match:
            dealer.address = addr_match.group(1)

        # Extract city, state, zip
        csz_match = re.search(
            r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+([A-Z]{2})\s+(\d{5})',
            text
        )
        if csz_match:
            dealer.city = csz_match.group(1)
            dealer.state = csz_match.group(2)
            dealer.zip_code = csz_match.group(3)

        # Try to find website link
        try:
            link = await element.query_selector('a[href*="http"]')
            if link:
                href = await link.get_attribute("href")
                if href and "ford.com" not in href and "toyota.com" not in href:
                    dealer.website_url = href
        except Exception:
            pass

        return dealer

    async def _extract_from_page_text(self, page, target, zip_code):
        """Fallback: extract dealer info from raw page text."""
        dealers = []
        text = await page.inner_text("body")

        # Look for dealer name patterns (e.g., "Bob Smith Toyota")
        brand = target.oem_brand
        pattern = rf'([\w\s]+{brand}[\w\s]*?)(?:\n|,|\()'
        matches = re.findall(pattern, text, re.IGNORECASE)

        for match in matches[:15]:
            name = match.strip()
            if len(name) > 5 and len(name) < 60:
                dealer = ScrapedDealer()
                dealer.dealership_name = name
                dealer.oem_brand = brand
                dealer.state = target.state
                dealer.territory = target.territory
                dealer.assigned_rep = target.assigned_rep
                dealer.source = f"Browserbase_OEM_{brand}"
                dealer.scrape_timestamp = datetime.now(timezone.utc).isoformat()
                dealer.scrape_method = "page_text_extraction"
                dealer.confidence = "low"
                dealers.append(dealer)

        return dealers

    async def enrich_dealer_website(self, page, dealer: ScrapedDealer):
        """Navigate to dealer website and scrape inventory + contacts."""
        if not dealer.website_url:
            return dealer

        try:
            await page.goto(dealer.website_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)

            # 1. Scrape inventory
            dealer = await self._scrape_inventory(page, dealer)

            # 2. Look for contacts
            dealer = await self._scrape_contacts(page, dealer)

            # 3. Estimate volume from inventory
            if dealer.total_inventory_count and dealer.total_inventory_count > 0:
                dealer.estimated_vehicles_sold = round(
                    dealer.total_inventory_count * self.INVENTORY_RATIO
                )

            dealer.confidence = "medium" if dealer.total_inventory_count else "low"
            if dealer.contact_name and dealer.total_inventory_count:
                dealer.confidence = "high"

        except Exception as e:
            logger.error(f"Error enriching {dealer.dealership_name}: {e}")

        return dealer

    async def _scrape_inventory(self, page, dealer: ScrapedDealer):
        """Scrape inventory counts from dealer website."""
        base_url = dealer.website_url.rstrip("/")

        # Try new inventory pages
        for path in self.INVENTORY_PAGE_PATHS:
            try:
                resp = await page.goto(
                    f"{base_url}{path}",
                    wait_until="domcontentloaded",
                    timeout=15000
                )
                if resp and resp.ok:
                    text = await page.inner_text("body")

                    # Pattern 1: "X vehicles found" / "Showing X results"
                    count_match = re.search(
                        r'(\d+)\s*(?:vehicles?|results?|cars?|trucks?)\s*(?:found|available|in stock)',
                        text, re.IGNORECASE
                    )
                    if count_match:
                        dealer.new_inventory_count = int(count_match.group(1))
                        dealer.inventory_url = f"{base_url}{path}"
                        break

                    # Pattern 2: Count DOM elements
                    cards = await page.query_selector_all(
                        '[class*="vehicle-card"], [class*="inventory-item"], '
                        '[class*="srp-list-item"], [class*="vehicle-result"]'
                    )
                    if len(cards) > 0:
                        # Check for pagination to estimate total
                        pagination_text = ""
                        try:
                            pag = await page.query_selector('[class*="pagination"], [class*="page-count"]')
                            if pag:
                                pagination_text = await pag.inner_text()
                        except Exception:
                            pass

                        total_match = re.search(r'of\s+(\d+)', pagination_text)
                        if total_match:
                            dealer.new_inventory_count = int(total_match.group(1))
                        else:
                            dealer.new_inventory_count = len(cards)
                        dealer.inventory_url = f"{base_url}{path}"
                        break

            except Exception:
                continue

        # Try used inventory
        for path in self.USED_INVENTORY_PATHS:
            try:
                resp = await page.goto(
                    f"{base_url}{path}",
                    wait_until="domcontentloaded",
                    timeout=15000
                )
                if resp and resp.ok:
                    text = await page.inner_text("body")
                    count_match = re.search(
                        r'(\d+)\s*(?:vehicles?|results?|cars?|trucks?)\s*(?:found|available|in stock)',
                        text, re.IGNORECASE
                    )
                    if count_match:
                        dealer.used_inventory_count = int(count_match.group(1))
                        break
            except Exception:
                continue

        # Calculate total
        new_ct = dealer.new_inventory_count or 0
        used_ct = dealer.used_inventory_count or 0
        if new_ct or used_ct:
            dealer.total_inventory_count = new_ct + used_ct

        return dealer

    async def _scrape_contacts(self, page, dealer: ScrapedDealer):
        """Look for key contacts on dealer team/about pages."""
        base_url = dealer.website_url.rstrip("/")

        for path in self.CONTACT_PAGE_PATHS:
            try:
                resp = await page.goto(
                    f"{base_url}{path}",
                    wait_until="domcontentloaded",
                    timeout=15000
                )
                if resp and resp.ok:
                    text = await page.inner_text("body")

                    contacts = self._extract_contacts_from_text(text)
                    if contacts:
                        # Assign primary contact (highest priority persona)
                        primary = contacts[0]
                        dealer.contact_name = primary["name"]
                        dealer.contact_title = primary["title"]
                        if primary.get("email"):
                            dealer.contact_email = primary["email"]
                        if primary.get("phone"):
                            dealer.contact_phone = primary["phone"]

                        # Store additional contacts
                        if len(contacts) > 1:
                            dealer.additional_contacts = contacts[1:]

                        break

            except Exception:
                continue

        return dealer

    def _extract_contacts_from_text(self, text):
        """Extract contacts matching buyer personas from page text."""
        contacts = []
        lines = text.split('\n')

        for i, line in enumerate(lines):
            line = line.strip()
            for tier, titles in BUYER_PERSONAS.items():
                for title in titles:
                    if title.lower() in line.lower():
                        contact = {"title": title, "tier": tier}

                        # Look for name (usually line before or same line)
                        name_line = line
                        if i > 0 and len(lines[i-1].strip()) < 40:
                            name_line = lines[i-1].strip()

                        # Extract name (look for 2-3 capitalized words)
                        name_match = re.search(
                            r'([A-Z][a-z]+\s+(?:[A-Z]\.\s+)?[A-Z][a-z]+)',
                            name_line
                        )
                        if name_match:
                            contact["name"] = name_match.group(1)
                        else:
                            # Try same line with title removed
                            cleaned = re.sub(
                                re.escape(title), '', line, flags=re.IGNORECASE
                            ).strip(' -|,')
                            name_match = re.search(
                                r'([A-Z][a-z]+\s+(?:[A-Z]\.\s+)?[A-Z][a-z]+)',
                                cleaned
                            )
                            if name_match:
                                contact["name"] = name_match.group(1)

                        # Look for email nearby
                        nearby = '\n'.join(lines[max(0,i-2):i+3])
                        email_match = re.search(
                            r'[\w.+-]+@[\w-]+\.[\w.-]+', nearby
                        )
                        if email_match:
                            contact["email"] = email_match.group()

                        # Look for phone nearby
                        phone_match = re.search(
                            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', nearby
                        )
                        if phone_match:
                            contact["phone"] = phone_match.group()

                        if contact.get("name"):
                            contacts.append(contact)

        # Sort by persona tier priority
        tier_order = {"primary": 0, "secondary": 1, "influencer": 2}
        contacts.sort(key=lambda c: tier_order.get(c.get("tier", ""), 3))

        # Deduplicate
        seen = set()
        unique = []
        for c in contacts:
            key = c.get("name", "").lower()
            if key and key not in seen:
                seen.add(key)
                unique.append(c)

        return unique

    def _scrape_simulated(self, target: ScrapeTarget, zip_codes: list):
        """Simulated scraping for testing and demo purposes."""
        import random

        dealers = []
        # Realistic dealer count per zip per OEM
        dealers_per_zip = random.randint(2, 8)

        for zip_code in zip_codes[:3]:  # Limit zips in simulation
            for i in range(dealers_per_zip):
                dealer = ScrapedDealer()
                dealer.oem_brand = target.oem_brand

                # Generate realistic dealer name
                prefixes = [
                    "Heritage", "Capital", "Metro", "Parkway", "Valley",
                    "Coastal", "Premier", "Classic", "Liberty", "Patriot",
                    "Elite", "Crown", "Star", "Diamond", "Golden",
                    "Atlantic", "Summit", "Westfield", "Northgate", "Eastside",
                ]
                prefix = random.choice(prefixes)
                dealer.dealership_name = f"{prefix} {target.oem_brand}"

                # Basic info
                dealer.state = target.state
                dealer.zip_code = zip_code
                dealer.city = self._zip_to_city(zip_code, target.state)
                dealer.territory = target.territory
                dealer.assigned_rep = target.assigned_rep
                dealer.phone = f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

                # Website
                slug = dealer.dealership_name.lower().replace(" ", "")
                dealer.website_url = f"https://www.{slug}.com"

                # Inventory (realistic ranges by brand tier)
                if target.oem_brand in ["BMW", "Mercedes-Benz", "Audi", "Lexus"]:
                    new_inv = random.randint(80, 250)
                    used_inv = random.randint(40, 150)
                elif target.oem_brand in ["Toyota", "Honda", "Ford", "Chevrolet"]:
                    new_inv = random.randint(120, 400)
                    used_inv = random.randint(60, 250)
                else:
                    new_inv = random.randint(60, 200)
                    used_inv = random.randint(30, 120)

                # Some dealers we can't get inventory for
                if random.random() < 0.15:
                    new_inv = None
                    used_inv = None

                dealer.new_inventory_count = new_inv
                dealer.used_inventory_count = used_inv
                if new_inv and used_inv:
                    dealer.total_inventory_count = new_inv + used_inv
                    dealer.estimated_vehicles_sold = round(
                        dealer.total_inventory_count * self.INVENTORY_RATIO
                    )

                # Contacts (simulate ~60% capture rate)
                if random.random() < 0.60:
                    tier = random.choice(["primary", "secondary", "influencer"])
                    titles = BUYER_PERSONAS[tier]
                    dealer.contact_title = random.choice(titles)
                    first_names = ["Mike", "Sarah", "John", "Lisa", "David",
                                   "Jennifer", "Robert", "Amanda", "Chris", "Angela"]
                    last_names = ["Johnson", "Williams", "Brown", "Jones", "Garcia",
                                  "Miller", "Davis", "Rodriguez", "Martinez", "Anderson"]
                    fname = random.choice(first_names)
                    lname = random.choice(last_names)
                    dealer.contact_name = f"{fname} {lname}"

                    if random.random() < 0.40:
                        dealer.contact_email = f"{fname[0].lower()}{lname.lower()}@{slug}.com"
                    if random.random() < 0.50:
                        dealer.contact_phone = f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"

                dealer.source = f"Browserbase_OEM_{target.oem_brand}_Simulated"
                dealer.scrape_timestamp = datetime.now(timezone.utc).isoformat()
                dealer.scrape_method = "simulated"
                dealer.confidence = "simulated"

                dealers.append(dealer)

        return dealers

    def _zip_to_city(self, zip_code, state):
        """Map major zip codes to city names."""
        zip_city = {
            "07001": "Newark", "07102": "Newark", "08501": "Trenton",
            "07601": "Hackensack", "08701": "Lakewood",
            "10001": "New York", "11201": "Brooklyn", "10601": "White Plains",
            "12207": "Albany", "14201": "Buffalo",
            "02101": "Boston", "01601": "Worcester",
            "06101": "Hartford", "06510": "New Haven",
            "19101": "Philadelphia", "15201": "Pittsburgh",
            "21201": "Baltimore", "27601": "Raleigh",
            "28201": "Charlotte", "30301": "Atlanta",
            "33101": "Miami", "32801": "Orlando", "33601": "Tampa",
            "75201": "Dallas", "77001": "Houston",
            "90001": "Los Angeles", "94101": "San Francisco",
            "85001": "Phoenix", "80201": "Denver",
            "63101": "St. Louis", "60601": "Chicago",
            "43201": "Columbus", "44101": "Cleveland",
            "46201": "Indianapolis", "48201": "Detroit",
            "37201": "Nashville", "38101": "Memphis",
        }
        return zip_city.get(zip_code, f"City-{zip_code}")


# ===========================================================================
# SECTION 4: SUPPRESSION CROSS-REFERENCE
# ===========================================================================

class SuppressionCrossReference:
    """Cross-reference scraped dealers against existing client database."""

    def __init__(self):
        self.client_names = set()
        self.client_accounts = {}  # normalized_name -> account details
        self._load_clients()

    def _load_clients(self):
        """Load client data from unified profiles and suppression registry."""
        # Load unified profiles
        profiles_path = DATA_DIR / "pie_unified_profiles.json"
        if profiles_path.exists():
            with open(profiles_path) as f:
                data = json.load(f)
                profiles = data.get("profiles", []) if isinstance(data, dict) else data

            for p in profiles:
                name = p.get("company_name", "") or p.get("account_name", "")
                if name:
                    norm = self._normalize(name)
                    self.client_names.add(norm)
                    self.client_accounts[norm] = {
                        "original_name": name,
                        "state": p.get("state", ""),
                        "mcid": p.get("mcid", ""),
                        "health_score": p.get("health_score", ""),
                    }

        # Load suppression registry
        registry_path = BASE_DIR / "suppression_registry.json"
        if registry_path.exists():
            with open(registry_path) as f:
                registry = json.load(f)
            for entry in registry.get("suppressed_accounts", []):
                name = entry.get("name", "")
                if name:
                    self.client_names.add(self._normalize(name))

        logger.info(f"Loaded {len(self.client_names)} client names for suppression")

    def check_dealer(self, dealer: ScrapedDealer) -> ScrapedDealer:
        """Check if a scraped dealer is an existing client."""
        name = dealer.dealership_name
        norm = self._normalize(name)

        # Level 1: Exact normalized match
        if norm in self.client_names:
            dealer.is_existing_client = True
            dealer.suppression_reason = "exact_name_match"
            matched = self.client_accounts.get(norm, {})
            dealer.suppression_match = matched.get("original_name", norm)
            return dealer

        # Level 2: Fuzzy match (Levenshtein distance <= 2, same state)
        for client_norm, client_info in self.client_accounts.items():
            if client_info.get("state") == dealer.state:
                if self._levenshtein(norm, client_norm) <= 2:
                    dealer.is_existing_client = True
                    dealer.suppression_reason = "fuzzy_name_state_match"
                    dealer.suppression_match = client_info.get("original_name", client_norm)
                    return dealer

        # Level 3: Substring containment (one name contains the other)
        for client_norm in self.client_names:
            if len(norm) > 5 and len(client_norm) > 5:
                if norm in client_norm or client_norm in norm:
                    dealer.is_existing_client = True
                    dealer.suppression_reason = "substring_match"
                    matched = self.client_accounts.get(client_norm, {})
                    dealer.suppression_match = matched.get("original_name", client_norm)
                    return dealer

        return dealer

    @staticmethod
    def _normalize(name):
        """Normalize dealer name for matching."""
        name = name.lower().strip()
        for suffix in ["inc", "llc", "corp", "ltd", "co", "group",
                        "auto", "automotive", "dealership", "motors",
                        "of ", "the ", ",", "."]:
            name = name.replace(suffix, "")
        return re.sub(r'\s+', ' ', name).strip()

    @staticmethod
    def _levenshtein(s1, s2):
        """Compute Levenshtein edit distance."""
        if len(s1) < len(s2):
            return SuppressionCrossReference._levenshtein(s2, s1)
        if len(s2) == 0:
            return len(s1)
        prev_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row
        return prev_row[-1]


# ===========================================================================
# SECTION 5: REV OPS CSV EXPORT
# ===========================================================================

class RevOpsExporter:
    """Export scraped dealers to PIE Upload Template CSV format."""

    CSV_HEADERS = [
        "dealership_name", "website_url", "contact_name", "contact_title",
        "contact_email", "contact_phone", "city", "state", "oem_brand",
        "dealer_group", "estimated_vehicles_sold", "current_crm",
        "current_dms", "current_credit_provider", "source", "notes",
    ]

    def export_csv(self, dealers: list, output_path: str, include_suppressed=False):
        """Export dealers to Rev Ops CSV format.

        Args:
            dealers: List of ScrapedDealer objects
            output_path: Path to write CSV
            include_suppressed: If True, include suppressed dealers with flag
        """
        filtered = []
        suppressed_count = 0

        for d in dealers:
            if d.is_existing_client:
                suppressed_count += 1
                if not include_suppressed:
                    continue
            filtered.append(d)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
            writer.writeheader()

            for d in filtered:
                # Build notes field with missing data flags
                d.compute_completeness()
                notes_parts = []
                if d.fields_missing:
                    notes_parts.append(f"REP_FILL_IN: {', '.join(d.fields_missing)}")
                if d.is_existing_client:
                    notes_parts.append(f"SUPPRESSED: {d.suppression_reason} ({d.suppression_match})")
                if d.additional_contacts:
                    contacts_str = "; ".join(
                        f"{c.get('name','')} ({c.get('title','')})"
                        for c in d.additional_contacts[:3]
                    )
                    notes_parts.append(f"Additional contacts: {contacts_str}")
                if d.confidence:
                    notes_parts.append(f"Confidence: {d.confidence}")

                row = {
                    "dealership_name": d.dealership_name,
                    "website_url": d.website_url,
                    "contact_name": d.contact_name,
                    "contact_title": d.contact_title,
                    "contact_email": d.contact_email,
                    "contact_phone": d.contact_phone,
                    "city": d.city,
                    "state": d.state,
                    "oem_brand": d.oem_brand,
                    "dealer_group": d.dealer_group,
                    "estimated_vehicles_sold": d.estimated_vehicles_sold or "",
                    "current_crm": "",  # Rep fill-in
                    "current_dms": "",  # Rep fill-in
                    "current_credit_provider": "",  # Rep fill-in
                    "source": d.source,
                    "notes": " | ".join(notes_parts),
                }
                writer.writerow(row)

        logger.info(
            f"Exported {len(filtered)} dealers to {output_path} "
            f"({suppressed_count} suppressed)"
        )
        return {"exported": len(filtered), "suppressed": suppressed_count}


# ===========================================================================
# SECTION 6: RECONCILIATION ENGINE
# ===========================================================================

class ReconciliationEngine:
    """Generate reconciliation report showing data capture rates."""

    def generate_report(self, dealers: list, gaps: list):
        """Generate comprehensive reconciliation data.

        Returns dict with all reconciliation metrics for dashboard rendering.
        """
        total = len(dealers)
        if total == 0:
            return {"error": "No dealers to reconcile"}

        # Field completeness
        field_stats = {}
        tracked_fields = [
            "dealership_name", "website_url", "city", "state", "phone",
            "oem_brand", "contact_name", "contact_title", "contact_email",
            "contact_phone", "new_inventory_count", "used_inventory_count",
            "estimated_vehicles_sold", "dealer_group",
        ]

        for field in tracked_fields:
            populated = sum(
                1 for d in dealers
                if getattr(d, field, None) and getattr(d, field) != "" and getattr(d, field) != 0
            )
            field_stats[field] = {
                "populated": populated,
                "missing": total - populated,
                "capture_rate": round(populated / total * 100, 1),
            }

        # Territory breakdown
        terr_stats = {}
        for d in dealers:
            terr = d.territory or "Unknown"
            if terr not in terr_stats:
                terr_stats[terr] = {
                    "total_scraped": 0,
                    "suppressed": 0,
                    "net_new": 0,
                    "with_contact": 0,
                    "with_inventory": 0,
                    "with_email": 0,
                    "avg_completeness": [],
                }
            ts = terr_stats[terr]
            ts["total_scraped"] += 1
            if d.is_existing_client:
                ts["suppressed"] += 1
            else:
                ts["net_new"] += 1
            if d.contact_name:
                ts["with_contact"] += 1
            if d.total_inventory_count:
                ts["with_inventory"] += 1
            if d.contact_email:
                ts["with_email"] += 1
            ts["avg_completeness"].append(d.compute_completeness())

        # Calculate averages
        for terr, ts in terr_stats.items():
            if ts["avg_completeness"]:
                ts["avg_completeness"] = round(
                    sum(ts["avg_completeness"]) / len(ts["avg_completeness"]), 1
                )
            else:
                ts["avg_completeness"] = 0

        # OEM breakdown
        oem_stats = {}
        for d in dealers:
            oem = d.oem_brand or "Unknown"
            if oem not in oem_stats:
                oem_stats[oem] = {"total": 0, "suppressed": 0, "with_inventory": 0}
            oem_stats[oem]["total"] += 1
            if d.is_existing_client:
                oem_stats[oem]["suppressed"] += 1
            if d.total_inventory_count:
                oem_stats[oem]["with_inventory"] += 1

        # Suppression summary
        suppressed = [d for d in dealers if d.is_existing_client]
        suppression_reasons = {}
        for d in suppressed:
            reason = d.suppression_reason or "unknown"
            suppression_reasons[reason] = suppression_reasons.get(reason, 0) + 1

        # Gap closure calculation
        gap_closure = {}
        for gap in gaps:
            terr = gap["territory"]
            if terr in terr_stats:
                new_leads = terr_stats[terr]["net_new"]
                deficit = gap["deficit"]
                closure_pct = min(100, round(new_leads / deficit * 100, 1)) if deficit > 0 else 100
                gap_closure[terr] = {
                    "original_deficit": deficit,
                    "new_leads_added": new_leads,
                    "remaining_deficit": max(0, deficit - new_leads),
                    "closure_pct": closure_pct,
                }

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_dealers_scraped": total,
                "total_suppressed": len(suppressed),
                "total_net_new": total - len(suppressed),
                "overall_suppression_rate": round(len(suppressed) / total * 100, 1),
                "avg_data_completeness": round(
                    sum(d.compute_completeness() for d in dealers) / total, 1
                ),
            },
            "field_completeness": field_stats,
            "territory_breakdown": terr_stats,
            "oem_breakdown": oem_stats,
            "suppression_analysis": {
                "total_suppressed": len(suppressed),
                "by_reason": suppression_reasons,
                "suppressed_dealers": [
                    {"name": d.dealership_name, "state": d.state,
                     "reason": d.suppression_reason, "matched": d.suppression_match}
                    for d in suppressed
                ],
            },
            "gap_closure": gap_closure,
            "rep_fill_in_required": {
                "fields_most_missing": sorted(
                    field_stats.items(),
                    key=lambda x: -x[1]["missing"]
                )[:5],
            },
        }

        return report


# ===========================================================================
# SECTION 7: ORCHESTRATOR
# ===========================================================================

class PIEScraperOrchestrator:
    """Main orchestrator that runs the full pipeline."""

    def __init__(self, mode="simulated"):
        self.mode = mode
        self.gap_analyzer = TerritoryGapAnalyzer()
        self.queue_builder = OEMTargetQueueBuilder()
        self.scraper = BrowserbaseDealerScraper(mode=mode)
        self.suppression = SuppressionCrossReference()
        self.exporter = RevOpsExporter()
        self.reconciler = ReconciliationEngine()

    def run(self, territories=None, oems=None, max_per_territory=50):
        """Run the full pipeline synchronously.

        Args:
            territories: List of territory names to target (None = all underserved)
            oems: List of OEM brands to scrape (None = priority OEMs)
            max_per_territory: Max dealers per territory

        Returns:
            dict with pipeline results
        """
        OUTPUT_DIR.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Step 1: Gap Analysis
        logger.info("Step 1: Running territory gap analysis...")
        gaps = self.gap_analyzer.analyze_gaps()

        if territories:
            gaps = [g for g in gaps if g["territory"] in territories]

        underserved = [g for g in gaps if g["underserved"]]
        logger.info(f"  Found {len(underserved)} underserved territories")

        # Step 2: Build Target Queue
        logger.info("Step 2: Building OEM target queue...")
        queue = self.queue_builder.build_queue(
            underserved, target_oems=oems, max_per_territory=max_per_territory
        )

        # Step 3: Scrape
        logger.info(f"Step 3: Scraping {len(queue)} targets ({self.mode} mode)...")
        all_dealers = []
        for target in queue:
            zips = self.queue_builder.STATE_MAJOR_ZIPS.get(target.state, [])
            if not zips:
                continue

            if self.mode == "real":
                dealers = asyncio.run(self.scraper.scrape_oem_locator(target, zips))
            else:
                dealers = self.scraper._scrape_simulated(target, zips)

            all_dealers.extend(dealers)

        logger.info(f"  Scraped {len(all_dealers)} dealer records")

        # Step 4: Deduplicate
        logger.info("Step 4: Deduplicating...")
        seen = set()
        unique_dealers = []
        for d in all_dealers:
            key = f"{d.dealership_name}_{d.state}".lower()
            if key not in seen:
                seen.add(key)
                unique_dealers.append(d)
        logger.info(f"  {len(unique_dealers)} unique dealers (removed {len(all_dealers) - len(unique_dealers)} dupes)")

        # Step 5: Suppression Cross-Reference
        logger.info("Step 5: Running suppression cross-reference...")
        for d in unique_dealers:
            self.suppression.check_dealer(d)
        suppressed = sum(1 for d in unique_dealers if d.is_existing_client)
        logger.info(f"  {suppressed} existing clients suppressed")

        # Step 6: Export to Rev Ops CSV
        csv_path = OUTPUT_DIR / f"PIE_Scrape_RevOps_{timestamp}.csv"
        csv_path_all = OUTPUT_DIR / f"PIE_Scrape_ALL_{timestamp}.csv"
        logger.info("Step 6: Exporting to Rev Ops CSV...")
        self.exporter.export_csv(unique_dealers, str(csv_path), include_suppressed=False)
        self.exporter.export_csv(unique_dealers, str(csv_path_all), include_suppressed=True)

        # Step 7: Reconciliation
        logger.info("Step 7: Generating reconciliation report...")
        recon = self.reconciler.generate_report(unique_dealers, gaps)

        recon_path = OUTPUT_DIR / f"PIE_Reconciliation_{timestamp}.json"
        with open(recon_path, 'w') as f:
            json.dump(recon, f, indent=2, default=str)

        logger.info("Pipeline complete!")

        return {
            "timestamp": timestamp,
            "mode": self.mode,
            "gaps": gaps,
            "targets_queued": len(queue),
            "dealers_scraped": len(all_dealers),
            "unique_dealers": len(unique_dealers),
            "suppressed": suppressed,
            "net_new_leads": len(unique_dealers) - suppressed,
            "csv_path": str(csv_path),
            "csv_all_path": str(csv_path_all),
            "reconciliation_path": str(recon_path),
            "reconciliation": recon,
            "dealers": unique_dealers,
        }


# ===========================================================================
# CLI Interface
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PIE Territory Scrape Engine")
    parser.add_argument("--territory", type=str, help="Target specific territory")
    parser.add_argument("--oem", type=str, help="Target specific OEMs (comma-separated)")
    parser.add_argument("--all-gaps", action="store_true", help="Target all underserved territories")
    parser.add_argument("--max-per-territory", type=int, default=50)
    parser.add_argument("--mode", choices=["real", "simulated"], default="simulated")
    parser.add_argument("--reconciliation", action="store_true", help="Generate reconciliation report only")

    args = parser.parse_args()

    territories = [args.territory] if args.territory else None
    oems = args.oem.split(",") if args.oem else None

    orchestrator = PIEScraperOrchestrator(mode=args.mode)
    results = orchestrator.run(
        territories=territories,
        oems=oems,
        max_per_territory=args.max_per_territory,
    )

    print(f"\n{'='*60}")
    print(f"PIPELINE RESULTS")
    print(f"{'='*60}")
    print(f"Mode:              {results['mode']}")
    print(f"Targets queued:    {results['targets_queued']}")
    print(f"Dealers scraped:   {results['dealers_scraped']}")
    print(f"Unique dealers:    {results['unique_dealers']}")
    print(f"Suppressed:        {results['suppressed']}")
    print(f"Net-new leads:     {results['net_new_leads']}")
    print(f"Rev Ops CSV:       {results['csv_path']}")
    print(f"Reconciliation:    {results['reconciliation_path']}")
