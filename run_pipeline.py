"""
run_pipeline.py — Full automation entry point.

Runs: scrape → auto-flag → PhantomBuster → Clay enrich

Controlled by PIPELINE_ENABLED env var:
  PIPELINE_ENABLED=true  → runs normally
  PIPELINE_ENABLED=false → exits immediately (easy on/off from Railway Variables)
"""

import os
import sys
from datetime import datetime, timezone

if os.environ.get("PIPELINE_ENABLED", "true").lower() != "true":
    print(f"[{datetime.now(timezone.utc).isoformat()}] Pipeline disabled (PIPELINE_ENABLED != true). Exiting.")
    sys.exit(0)

print(f"[{datetime.now(timezone.utc).isoformat()}] Starting pipeline run...")

import scraper
import enrich_profiles

scraper.main()

print("\nEnriching new profiles via Clay...")
enrich_profiles.run()

print(f"\n[{datetime.now(timezone.utc).isoformat()}] Pipeline complete.")
