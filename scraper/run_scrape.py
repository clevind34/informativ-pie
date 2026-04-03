#!/usr/bin/env python3
"""
PIE Scraper — GitHub Action Runner
====================================
Lightweight wrapper around pie_territory_scrape_engine.py optimized for
GitHub Actions execution with Browserbase budget management.

Budget: 100 Browserbase hours/month (6,000 minutes)
Strategy: 4 weekly runs × ~1,500 min each = 6,000 min/month

Usage:
    python run_scrape.py --all-gaps --mode real --max-minutes 1500
    python run_scrape.py --territory "Southern Crossroads" --mode real --force
"""

import argparse
import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add parent dir for imports if needed
sys.path.insert(0, str(Path(__file__).parent))

from pie_territory_scrape_engine import (
    PIEScraperOrchestrator,
    TERRITORY_MAP,
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("PIE-Runner")

OUTPUT_DIR = Path(__file__).parent / "output"


def apply_env_overrides():
    """Override hardcoded keys with GitHub secrets if available."""
    import pie_territory_scrape_engine as engine

    api_key = os.environ.get("BROWSERBASE_API_KEY")
    project_id = os.environ.get("BROWSERBASE_PROJECT_ID")

    if api_key:
        engine.BROWSERBASE_API_KEY = api_key
        logger.info("Using BROWSERBASE_API_KEY from environment")
    if project_id:
        engine.BROWSERBASE_PROJECT_ID = project_id
        logger.info("Using BROWSERBASE_PROJECT_ID from environment")


def determine_weekly_territories():
    """Auto-select territories based on week of month.

    Week 1 & 3 (odd): High-deficit territories (top 3 by deficit)
    Week 2 & 4 (even): Remaining underserved territories

    This spreads the 100 Browserbase hours across 4 Saturday runs.
    """
    day = datetime.now().day
    week_of_month = (day - 1) // 7 + 1  # 1-based week number
    is_odd_week = week_of_month % 2 == 1

    # Run gap analysis to get current deficits
    orchestrator = PIEScraperOrchestrator(mode="simulated")
    gaps = orchestrator.gap_analyzer.analyze_gaps()
    underserved = sorted(
        [g for g in gaps if g["underserved"]],
        key=lambda x: -x["deficit"],
    )

    if not underserved:
        logger.info("No underserved territories found")
        return [], underserved

    # Split: top 3 deficit for odd weeks, rest for even weeks
    high_deficit = [g["territory"] for g in underserved[:3]]
    medium_deficit = [g["territory"] for g in underserved[3:]]

    if is_odd_week:
        selected = high_deficit
        label = f"Week {week_of_month} (odd) — high-deficit"
    else:
        selected = medium_deficit if medium_deficit else high_deficit
        label = f"Week {week_of_month} (even) — medium-deficit"

    logger.info(f"Auto-selected territories for {label}: {selected}")
    return selected, underserved


def write_summary(results, territories_targeted, all_gaps):
    """Write run summary for GitHub Actions step summary."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    summary_path = OUTPUT_DIR / "run_summary.txt"

    lines = [
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Mode:** {results.get('mode', 'unknown')}",
        f"**Territories targeted:** {', '.join(territories_targeted) if territories_targeted else 'all underserved'}",
        f"**Targets queued:** {results.get('targets_queued', 0)}",
        f"**Dealers scraped:** {results.get('dealers_scraped', 0)}",
        f"**Unique dealers:** {results.get('unique_dealers', 0)}",
        f"**Suppressed (existing clients):** {results.get('suppressed', 0)}",
        f"**Net-new leads:** {results.get('net_new_leads', 0)}",
        "",
        "**Territory Gap Analysis:**",
    ]

    for g in all_gaps:
        flag = " ← TARGETED" if g["territory"] in (territories_targeted or []) else ""
        flag = flag or (" ← UNDERSERVED" if g["underserved"] else "")
        lines.append(
            f"  {g['territory']:25s} | {g['current_prospects']:4d} prospects "
            f"| deficit: {g['deficit']:3d}{flag}"
        )

    summary = "\n".join(lines)
    summary_path.write_text(summary)
    logger.info(f"Summary written to {summary_path}")
    return summary


def main():
    parser = argparse.ArgumentParser(description="PIE Scraper — GitHub Action Runner")
    parser.add_argument("--territory", type=str, help="Target specific territory")
    parser.add_argument("--all-gaps", action="store_true", help="Auto-select underserved territories")
    parser.add_argument("--mode", choices=["real", "simulated"], default="real")
    parser.add_argument("--max-per-territory", type=int, default=50)
    parser.add_argument("--max-minutes", type=int, default=1500,
                        help="Max Browserbase minutes for this run (budget cap)")
    parser.add_argument("--force", action="store_true",
                        help="Force scrape even if territory is above average")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)

    # Apply environment variable overrides for secrets
    apply_env_overrides()

    # Determine which territories to target
    territories_targeted = []
    all_gaps = []

    if args.territory:
        territories_targeted = [args.territory]
        # Still need gap data for summary
        temp_orch = PIEScraperOrchestrator(mode="simulated")
        all_gaps = temp_orch.gap_analyzer.analyze_gaps()
    elif args.all_gaps:
        territories_targeted, all_gaps = determine_weekly_territories()
        if not territories_targeted:
            logger.info("All territories are at or above average. Nothing to scrape.")
            write_summary({"mode": args.mode, "net_new_leads": 0}, [], all_gaps)
            return
    else:
        parser.error("Specify --territory or --all-gaps")

    # Create orchestrator with budget cap
    orchestrator = PIEScraperOrchestrator(mode=args.mode)
    orchestrator.scraper.max_session_minutes = args.max_minutes

    # Force mode: override underserved check
    if args.force and args.territory:
        original_analyze = orchestrator.gap_analyzer.analyze_gaps

        def forced_analyze():
            gaps = original_analyze()
            for g in gaps:
                if g["territory"] == args.territory:
                    g["underserved"] = True
                    g["deficit"] = max(g["deficit"], 50)
                    g["priority"] = max(g["priority"], 50)
            return gaps

        orchestrator.gap_analyzer.analyze_gaps = forced_analyze
        logger.info(f"Force mode: {args.territory} will be scraped regardless of gap status")

    # Run the pipeline
    logger.info(f"Starting PIE scrape — mode={args.mode}, budget={args.max_minutes}min, "
                f"territories={territories_targeted}")

    results = orchestrator.run(
        territories=territories_targeted if territories_targeted else None,
        max_per_territory=args.max_per_territory,
    )

    # Copy output CSVs to the output directory for artifact upload
    for key in ["csv_path", "csv_all_path"]:
        src = results.get(key)
        if src and Path(src).exists():
            import shutil
            dest = OUTPUT_DIR / Path(src).name
            shutil.copy2(src, dest)
            logger.info(f"Copied {Path(src).name} → output/")

    # Copy reconciliation
    recon = results.get("reconciliation_path")
    if recon and Path(recon).exists():
        import shutil
        shutil.copy2(recon, OUTPUT_DIR / Path(recon).name)

    # Write summary
    summary = write_summary(results, territories_targeted, all_gaps)

    # Print results
    print(f"\n{'=' * 60}")
    print(f"PIE SCRAPE COMPLETE")
    print(f"{'=' * 60}")
    print(summary)
    print(f"\nBrowserbase minutes used: ~{orchestrator.scraper.session_minutes_used}")
    print(f"Budget remaining: ~{args.max_minutes - orchestrator.scraper.session_minutes_used} min")


if __name__ == "__main__":
    main()
