"""
enrich_profiles.py — Push unenriched profiles to Clay for enrichment.
Clay enriches title/company/location and pushes back to Supabase automatically.

Usage:
    venv/bin/python enrich_profiles.py
"""

import requests
from database import supabase

CLAY_WEBHOOK_URL = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-5e41dd5c-388c-4a13-b6a5-3b1539df5928"
CLAY_POSTER_WEBHOOK_URL = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-68b81a08-7232-4021-9ba7-553e3a97b552"


def push_to_clay(profiles: list[dict]) -> int:
    pushed = 0
    for p in profiles:
        payload = {
            "linkedin_url": p["linkedin_url"],
            "full_name": p.get("full_name") or "",
            "headline": p.get("title") or "",
        }
        resp = requests.post(CLAY_WEBHOOK_URL, json=payload)
        if resp.ok:
            pushed += 1
        else:
            print(f"  Failed to push {p['linkedin_url']}: {resp.text}")
    return pushed


def run():
    # Only push profiles that haven't been enriched yet
    result = supabase.table("profiles").select("id, linkedin_url, full_name, title").eq("enriched", False).execute()
    profiles = result.data

    if not profiles:
        print("No unenriched profiles to push.")
        return

    print(f"Pushing {len(profiles)} unenriched profiles to Clay...")
    pushed = push_to_clay(profiles)
    print(f"Done — pushed {pushed} profiles. Clay will enrich and write back to Supabase.")


def push_poster_to_clay(post_url: str, linkedin_url: str) -> bool:
    resp = requests.post(CLAY_POSTER_WEBHOOK_URL, json={
        "post_url": post_url,
        "linkedin_url": linkedin_url,
    })
    if not resp.ok:
        print(f"  Failed to push poster {linkedin_url}: {resp.text}")
    return resp.ok


if __name__ == "__main__":
    run()
