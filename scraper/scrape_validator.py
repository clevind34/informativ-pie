"""
PIE Scrape Data Validator
========================
Filters scraped dealer results to remove junk DOM text, navigation artifacts,
and low-confidence records before they enter the PIE prospect pipeline.

Runs automatically after every scrape (called by run_scrape.py) and can also
be invoked standalone for manual cleaning.

Usage:
    # As module (in run_scrape.py pipeline):
    from scrape_validator import validate_scrape_results
    clean_path, report = validate_scrape_results("output/PIE_Scrape_RevOps_*.csv")

    # Standalone:
    python scrape_validator.py --input output/PIE_Scrape_RevOps_*.csv
"""

import csv
import re
import logging
import json
from pathlib import Path
from datetime import datetime
from collections import Counter

logger = logging.getLogger("PIE.Validator")

# ─── Junk patterns: text fragments that indicate DOM nav/page chrome, not dealer data ───
JUNK_PATTERNS = [
    # Generic page navigation
    r'(?i)^skip\s*to\s*content',
    r'(?i)^build\s*now',
    r'(?i)^learn\s*about',
    r'(?i)^connected\s*services',
    r'(?i)^vehicle\s*specs',
    r'(?i)^maintain\s*your',
    r'(?i)^accessories\s',
    r'(?i)^personalize\s*your',
    r'(?i)^service\s*\n\s*account',
    r'(?i)^compare\s.*vehicles',
    r'(?i)^showroom\s*live',
    r'(?i)^genuine\s.*service',
    r'(?i)^explore\s.*drive',
    r'(?i)^protect\s*your\s*vehicle',
    r'(?i)^in\s*value\s*\|',
    r'(?i)^program\s*\|',
    r'(?i)owner.*portal',
    r'(?i)^stay\s*connected',
    r'(?i)finance\s*app',
    r'(?i)^benz\s*store',
    r'(?i)^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s',
    # Year-prefixed junk (e.g., "2026\nToyota Crown Signia")
    r'^20\d{2}\s*\n',
    r'^20\d{2}\s+[A-Z]',
    # Copyright/legal
    r'(?i)©\s*20\d{2}',
    r'(?i)^20\d{2}\s+.*of\s*(north\s*)?america',
    # Map/geo artifacts
    r'(?i)^INEGI\s*\|',
    r'(?i)^terms\s*\|',
    r'(?i)^map\s*data',
    # Hyundai page nav
    r'(?i)^contact\s+hyundai',
    r'(?i)^search\s+hyundai',
    r'(?i)^why\s+hyundai',
    r'(?i)^hyundai\s+showroom',
    r'(?i)^shop\s+hyundai',
    # BMW page chrome
    r'(?i)^the\s+BMW\s+name',
    r'(?i)^BMW\s+logo',
    r'(?i)^BMW\s+uses\s+cookies',
    r'(?i)^BMW\s+of\s+(north\s+)?america',
    # Nissan page nav
    r'(?i)^owned\s*\|',
    r'(?i)^search\s+nissan',
    # Mercedes page chrome
    r'(?i)^stay\s+connected\s+to',
    r'(?i)^benz\s+store',
    r'(?i)^mercedes.*finance',
    # VW page nav
    r'(?i)^genuine\s+volkswagen',
    r'(?i)^explore\s+volkswagen',
    r'(?i)^in\s+value',
    # Generic brand page patterns (not dealer names)
    r'(?i)^search\s+\w+\s+vehicles',
    r'(?i)^find\s+a\s+dealer',
    r'(?i)^locate\s+a\s+dealer',
    r'(?i)^dealer\s+locator',
    r'(?i)^(contact|about)\s+\w+$',
    r'(?i)cookie',
    r'(?i)^sign\s+(in|up)',
    r'(?i)^my\s+(account|garage)',
    # Ford page nav that slipped through
    r'(?i)^skip\s+to',
    r'(?i)^ford\s+home',
    # More Hyundai junk
    r'(?i)^login\s+to',
    r'(?i)hyundai\s+in\s+america',
    r'(?i)^hyundai\s+dealerships\s+near',
    # More Nissan junk
    r'(?i)^guides\s*\|',
    r'(?i)maintenance\s+schedules',
    r'(?i)nissanconnect',
    r'(?i)dealer\s+website\s*\|',
    # More Mercedes junk
    r'(?i)^discover\s+mercedes',
    r'(?i)^mercedes.*vans',
    r'(?i)^AMG\s*\|',
    r'(?i)financial\s+services',
    # More VW junk
    r'(?i)^disclaimer\s+by',
    r'(?i)^zip\s+code\s*\|',
    r'(?i)^for\s+\d+\s+months',
    r'(?i)view\s+privacy\s+policy',
    r'(?i)volkswagen\s+of\s+america',
    # Generic OEM page patterns
    r'(?i)^(shop|browse|view|see)\s+all',
    r'(?i)^(new|used|certified)\s+vehicles?$',
    r'(?i)^pre-?owned',
    r'(?i)^special\s+offers?',
    r'(?i)^(test\s+drive|schedule)',
    r'(?i)^(get\s+a\s+quote|request)',
    # Empty or too short
    r'^.{0,2}$',
]

JUNK_REGEXES = [re.compile(p) for p in JUNK_PATTERNS]

# ─── Name extraction patterns for OEMs that embed real names in noisy text ───
NAME_EXTRACTORS = {
    'Toyota': re.compile(r'State or Dealer Name\s*\|\s*\d+\s*\|\s*(.+)', re.IGNORECASE),
    'BMW': re.compile(r'(?:\w{2}\s+\d{5}\s*\|)?\s*(?:Certified\s+Center\s*\|)?\s*\d+\s*\|\s*(.+?)(?:\s*\|.*)?$'),
    'Nissan': re.compile(r'^[A-Z]\s*\|\s*(.+?)(?:\s*\|.*)?$'),
    'Hyundai': re.compile(r'from\s+\d{5}\s*\|\s*(.+)', re.IGNORECASE),
}

# ─── Minimum field requirements for a record to be considered "clean" ───
REQUIRED_FIELDS = ['dealership_name']
MIN_NAME_LENGTH = 4
MAX_NAME_LENGTH = 120


def _clean_name(raw_name: str) -> str:
    """Normalize a dealer name — collapse whitespace, strip nav prefixes."""
    name = raw_name.replace('\n', ' | ').strip()
    # Remove leading pipe or number prefixes
    name = re.sub(r'^[\|\s\d]+\|\s*', '', name)
    # Remove trailing pipes
    name = re.sub(r'\s*\|?\s*$', '', name)
    return name.strip()


def _extract_dealer_name(raw_name: str, oem: str) -> str | None:
    """Try to extract a real dealer name from noisy OEM locator text."""
    # First try OEM-specific extractors
    extractor = NAME_EXTRACTORS.get(oem)
    if extractor:
        match = extractor.search(raw_name.replace('\n', ' | '))
        if match:
            return match.group(1).strip()

    # Generic cleaning
    cleaned = _clean_name(raw_name)

    # Check against junk patterns
    for regex in JUNK_REGEXES:
        if regex.search(raw_name) or regex.search(cleaned):
            return None

    if len(cleaned) < MIN_NAME_LENGTH or len(cleaned) > MAX_NAME_LENGTH:
        return None

    return cleaned


def _score_confidence(row: dict) -> int:
    """Score record confidence 0-100 based on field completeness."""
    score = 0
    name = row.get('dealership_name', '').strip()
    if name and len(name) >= MIN_NAME_LENGTH:
        score += 30

    url = row.get('website_url', '').strip()
    if url.startswith('http'):
        score += 25
        # Prefer dealer-specific URLs over Google Maps links
        if 'google.com/maps' not in url:
            score += 5

    city = row.get('city', '').strip()
    state = row.get('state', '').strip()
    if city:
        score += 15
    if state and len(state) == 2:
        score += 5

    oem = row.get('oem_brand', '').strip()
    if oem:
        score += 5

    phone = row.get('contact_phone', '').strip()
    if phone and len(phone) >= 7:
        score += 10

    email = row.get('contact_email', '').strip()
    if email and '@' in email:
        score += 5

    return min(score, 100)


def validate_scrape_results(input_path: str, min_confidence: int = 30) -> tuple:
    """
    Validate and clean a scrape results CSV.

    Args:
        input_path: Path to the raw scrape CSV (RevOps or ALL format)
        min_confidence: Minimum confidence score (0-100) to keep a record

    Returns:
        (clean_path, report_dict) — path to cleaned CSV and validation report
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Scrape file not found: {input_path}")

    # Read raw data
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        raw_rows = list(reader)

    logger.info(f"Validating {len(raw_rows)} scraped records from {input_file.name}")

    clean_rows = []
    rejected = []
    cleaned_names = 0

    for row in raw_rows:
        raw_name = row.get('dealership_name', '')
        oem = row.get('oem_brand', '')

        # Try to extract/clean the dealer name
        extracted_name = _extract_dealer_name(raw_name, oem)

        if extracted_name is None:
            rejected.append({
                'raw_name': raw_name[:80].replace('\n', '\\n'),
                'oem': oem,
                'reason': 'junk_pattern_or_empty'
            })
            continue

        # Update the row with cleaned name
        if extracted_name != raw_name:
            row['dealership_name'] = extracted_name
            cleaned_names += 1

        # Clean city field (sometimes has address fragments)
        city = row.get('city', '').strip()
        if '\n' in city:
            # Take last line which is usually the actual city
            parts = [p.strip() for p in city.split('\n') if p.strip()]
            if parts:
                row['city'] = parts[-1]

        # Score confidence
        confidence = _score_confidence(row)
        if confidence < min_confidence:
            rejected.append({
                'raw_name': raw_name[:80].replace('\n', '\\n'),
                'oem': oem,
                'reason': f'low_confidence_{confidence}'
            })
            continue

        # Add confidence score as a field
        row['confidence_score'] = str(confidence)
        clean_rows.append(row)

    # Deduplicate by (dealer_name, state, oem_brand)
    seen = set()
    deduped = []
    dupes = 0
    for row in clean_rows:
        key = (
            row['dealership_name'].lower().strip(),
            row.get('state', '').strip().upper(),
            row.get('oem_brand', '').strip().lower()
        )
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        deduped.append(row)

    # Write clean CSV
    clean_filename = input_file.stem.replace('_RevOps_', '_Clean_').replace('_ALL_', '_Clean_') + '.csv'
    clean_path = input_file.parent / clean_filename
    out_fieldnames = (fieldnames or []) + ['confidence_score']

    with open(clean_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(deduped)

    # Build report
    oem_counts = Counter(r.get('oem_brand', 'Unknown') for r in deduped)
    reject_reasons = Counter(r['reason'] for r in rejected)
    confidence_dist = Counter()
    for r in deduped:
        score = int(r.get('confidence_score', 0))
        if score >= 80:
            confidence_dist['high_80+'] += 1
        elif score >= 50:
            confidence_dist['medium_50-79'] += 1
        else:
            confidence_dist['low_30-49'] += 1

    report = {
        'timestamp': datetime.now().isoformat(),
        'input_file': str(input_file.name),
        'output_file': str(clean_path.name),
        'raw_count': len(raw_rows),
        'clean_count': len(deduped),
        'rejected_count': len(rejected),
        'duplicates_removed': dupes,
        'names_cleaned': cleaned_names,
        'acceptance_rate': round(len(deduped) / max(len(raw_rows), 1) * 100, 1),
        'by_oem': dict(oem_counts.most_common()),
        'reject_reasons': dict(reject_reasons.most_common()),
        'confidence_distribution': dict(confidence_dist),
        'min_confidence_threshold': min_confidence,
    }

    # Write report JSON
    report_path = input_file.parent / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(
        f"Validation complete: {report['raw_count']} raw → {report['clean_count']} clean "
        f"({report['acceptance_rate']}% acceptance) | {report['rejected_count']} rejected | "
        f"{report['duplicates_removed']} dupes"
    )

    return str(clean_path), report


# ─── Standalone CLI ───
if __name__ == '__main__':
    import argparse
    import glob

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    parser = argparse.ArgumentParser(description='Validate PIE scrape results')
    parser.add_argument('--input', required=True, help='Path to raw scrape CSV (supports glob)')
    parser.add_argument('--min-confidence', type=int, default=30, help='Minimum confidence score (0-100)')
    args = parser.parse_args()

    files = glob.glob(args.input)
    if not files:
        print(f"No files matching: {args.input}")
        exit(1)

    for f in files:
        clean_path, report = validate_scrape_results(f, args.min_confidence)
        print(f"\n{'='*60}")
        print(f"Input:      {report['input_file']}")
        print(f"Output:     {report['output_file']}")
        print(f"Raw:        {report['raw_count']}")
        print(f"Clean:      {report['clean_count']}")
        print(f"Rejected:   {report['rejected_count']}")
        print(f"Duplicates: {report['duplicates_removed']}")
        print(f"Acceptance: {report['acceptance_rate']}%")
        print(f"\nBy OEM:")
        for oem, cnt in report['by_oem'].items():
            print(f"  {oem}: {cnt}")
        print(f"\nConfidence distribution:")
        for level, cnt in report['confidence_distribution'].items():
            print(f"  {level}: {cnt}")
        print(f"{'='*60}")
