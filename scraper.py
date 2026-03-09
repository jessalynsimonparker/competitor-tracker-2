import json
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from config import (
    BRIGHTDATA_API_KEY,
    BRIGHTDATA_DATASET_ID,
    COMPETITOR_URLS,
    ROLLING_WINDOW_DAYS,
)
from database import init_db, upsert_post

TRIGGER_URL = "https://api.brightdata.com/datasets/v3/scrape"
SNAPSHOT_URL = "https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"

HEADERS = {
    "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
    "Content-Type": "application/json",
}

POLL_INTERVAL = 10  # seconds between status checks
MAX_WAIT = 300       # seconds before giving up on a snapshot


def company_name_from_url(url: str) -> str:
    parts = urlparse(url).path.strip("/").split("/")
    # e.g. /company/example-competitor-1 → "example-competitor-1"
    if len(parts) >= 2 and parts[0] == "company":
        return parts[1].replace("-", " ").title()
    return url


def trigger_collection(linkedin_url: str) -> str:
    resp = requests.post(
        TRIGGER_URL,
        headers=HEADERS,
        params={
            "dataset_id": BRIGHTDATA_DATASET_ID,
            "notify": "false",
            "include_errors": "true",
            "type": "discover_new",
            "discover_by": "company_url",
            "limit_per_input": "5",
        },
        json={"input": [{"url": linkedin_url}]},
    )
    resp.raise_for_status()
    data = resp.json()
    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        raise ValueError(f"No snapshot_id in response: {data}")
    return snapshot_id


def wait_for_snapshot(snapshot_id: str) -> list[dict]:
    status_url = SNAPSHOT_URL.format(snapshot_id=snapshot_id)
    elapsed = 0
    while elapsed < MAX_WAIT:
        resp = requests.get(status_url, headers=HEADERS)
        resp.raise_for_status()
        try:
            data = resp.json()
            status = data.get("status")
        except Exception:
            # NDJSON response means data is ready
            status = "ready"

        if status == "ready":
            dl = requests.get(status_url + "?format=json", headers=HEADERS)
            dl.raise_for_status()
            all_records = dl.json()
            return [r for r in all_records if isinstance(r, dict) and "error" not in r]

        if status == "failed":
            raise RuntimeError(f"Snapshot {snapshot_id} failed: {data}")
        print(f"  Snapshot {snapshot_id} status: {status} — waiting {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    raise TimeoutError(f"Snapshot {snapshot_id} did not complete within {MAX_WAIT}s")


def is_within_window(posted_date_str: str) -> bool:
    if not posted_date_str:
        return True  # include if unknown
    cutoff = datetime.now(timezone.utc) - timedelta(days=ROLLING_WINDOW_DAYS)
    try:
        # Try ISO format first, then date-only
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
            try:
                posted = datetime.strptime(posted_date_str[:19], fmt[:len(fmt)])
                if posted.tzinfo is None:
                    posted = posted.replace(tzinfo=timezone.utc)
                return posted >= cutoff
            except ValueError:
                continue
    except Exception:
        pass
    return True


def fetch_og_image(post_url: str) -> str:
    try:
        resp = requests.get(post_url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }, timeout=8)
        match = re.search(r'<meta property="og:image" content="([^"]+)"', resp.text)
        if match:
            url = match.group(1)
            # skip generic LinkedIn icons
            if "static.licdn.com" not in url:
                return url
    except Exception:
        pass
    return ""


def normalize_post(raw: dict, company_name: str) -> dict:
    images = raw.get("images") or []
    image_url = (images[0] if isinstance(images, list) and images else None) or raw.get("video_thumbnail") or ""
    if not image_url:
        image_url = fetch_og_image(raw.get("url") or "")
    return {
        "company_name": company_name,
        "post_url": raw.get("url") or "",
        "post_text": raw.get("post_text") or "",
        "posted_date": raw.get("date_posted") or "",
        "likes": int(raw.get("num_likes") or 0),
        "comments": int(raw.get("num_comments") or 0),
        "image_url": image_url,
    }


def scrape_competitor(linkedin_url: str):
    company_name = company_name_from_url(linkedin_url)
    print(f"Scraping {company_name} ({linkedin_url})...")

    snapshot_id = trigger_collection(linkedin_url)
    print(f"  Snapshot triggered: {snapshot_id}")

    raw_posts = wait_for_snapshot(snapshot_id)
    raw_posts = raw_posts[:5]
    print(f"  Retrieved {len(raw_posts)} posts")

    saved = 0
    for raw in raw_posts:
        post = normalize_post(raw, company_name)
        if not post["post_url"]:
            continue
        if not is_within_window(post["posted_date"]):
            continue
        upsert_post(post)
        saved += 1

    print(f"  Saved {saved} posts within {ROLLING_WINDOW_DAYS}-day window")


def main():
    init_db()
    for url in COMPETITOR_URLS:
        try:
            scrape_competitor(url)
        except Exception as e:
            print(f"Error scraping {url}: {e}")


if __name__ == "__main__":
    main()
