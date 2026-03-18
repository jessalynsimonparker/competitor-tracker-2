"""
phantom_runner.py — Launch PhantomBuster for all queued (flagged) posts,
poll for completion, and save likers to profiles + engagement tables.

Uses the individual "LinkedIn Post Likers Export" phantom (PHANTOMBUSTER_LIKERS_AGENT_ID).

Usage:
    venv/bin/python phantom_runner.py
"""

import json
import time
import requests
from config import PHANTOMBUSTER_API_KEY, PHANTOMBUSTER_LIKERS_AGENT_ID
from database import get_queued_posts, set_phantom_status, upsert_profile, upsert_engagement

AGENT_ID = PHANTOMBUSTER_LIKERS_AGENT_ID
HEADERS = {"X-Phantombuster-Key": PHANTOMBUSTER_API_KEY}
LAUNCH_URL = "https://api.phantombuster.com/api/v2/agents/launch"
FETCH_URL = "https://api.phantombuster.com/api/v2/agents/fetch"
CONTAINER_URL = "https://api.phantombuster.com/api/v2/containers/fetch"
POLL_INTERVAL = 15
MAX_WAIT = 600  # 10 minutes


def launch_phantom(post_url: str) -> tuple[str, str]:
    # Fetch saved argument (contains session cookie) + orgS3Folder
    agent_resp = requests.get(FETCH_URL, headers=HEADERS, params={"id": AGENT_ID})
    agent_resp.raise_for_status()
    agent_data = agent_resp.json()
    org_s3_folder = agent_data.get("orgS3Folder", "")
    s3_folder = agent_data.get("s3Folder", "")
    print(f"  orgS3Folder: {org_s3_folder}, s3Folder: {s3_folder}")
    saved_arg = agent_data.get("argument", "{}")
    if isinstance(saved_arg, str):
        saved_arg = json.loads(saved_arg)

    # Use postUrl (not spreadsheetUrl) for single post URL input
    clean_url = post_url.rstrip("/") + "/"
    saved_arg["postUrl"] = clean_url
    saved_arg.pop("spreadsheetUrl", None)  # remove conflicting field
    print(f"  Targeting URL: {clean_url}")

    # Launch with saveArgument=true — atomically saves + launches with the new URL
    resp = requests.post(LAUNCH_URL, headers=HEADERS, json={
        "id": AGENT_ID,
        "argument": saved_arg,
        "saveArgument": True,
    })
    if not resp.ok:
        print(f"  Launch error: {resp.text}")
    resp.raise_for_status()
    container_id = resp.json().get("containerId")
    print(f"  Container ID: {container_id}")
    return container_id, org_s3_folder, s3_folder


def wait_for_completion(container_id: str, org_s3_folder: str, s3_folder: str, post_url: str = "") -> list[dict]:
    elapsed = 0
    while elapsed < MAX_WAIT:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
        resp = requests.get(CONTAINER_URL, headers=HEADERS, params={"id": container_id})
        resp.raise_for_status()
        container_data = resp.json()
        status = container_data.get("status")
        print(f"  Container status: {status}")
        if status == "finished":
            print(f"  Container keys: {list(container_data.keys())}")
            if org_s3_folder and s3_folder:
                s3_url = f"https://phantombuster.s3.amazonaws.com/{org_s3_folder}/{s3_folder}/result.json"
                print(f"  Fetching results from S3: {s3_url}")
                # Wait briefly for S3 write to propagate
                time.sleep(10)
                r = requests.get(s3_url)
                if r.ok:
                    data = r.json()
                    # Filter to only results matching this post URL
                    clean_url = post_url.rstrip("/") + "/"
                    matched = [d for d in data if isinstance(d, dict) and d.get("postUrl", "").rstrip("/") + "/" == clean_url and "error" not in d]
                    print(f"  Got {len(data)} result(s) from S3, {len(matched)} match this post")
                    if data:
                        print(f"  First result: {data[0]}")
                    return matched
                else:
                    print(f"  S3 fetch failed ({r.status_code}), falling back to result-object API")
            # Fallback: containers/fetch-result-object
            print("  Waiting 30s for results to be written...")
            time.sleep(30)
            r = requests.get(
                "https://api.phantombuster.com/api/v2/containers/fetch-result-object",
                headers=HEADERS,
                params={"id": container_id},
            )
            r.raise_for_status()
            data = r.json()
            raw = data.get("resultObject")
            print(f"  Raw preview: {str(raw)[:200]}")
            if isinstance(raw, list):
                return raw
            if isinstance(raw, str):
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, list) else []
            return []
        if status == "error":
            raise RuntimeError("PhantomBuster container returned error status")
    raise TimeoutError("PhantomBuster did not complete within timeout")


def save_engagers(post_id: int, engagers: list[dict]):
    saved = 0
    skipped = 0
    for person in engagers:
        linkedin_url = (person.get("profileUrl") or person.get("profileLink") or "").rstrip("/") + "/"
        if not linkedin_url or linkedin_url == "/":
            skipped += 1
            continue

        profile = {
            "linkedin_url": linkedin_url,
            "full_name": person.get("name") or person.get("fullName") or f"{person.get('firstName', '')} {person.get('lastName', '')}".strip(),
            "title": person.get("occupation"),
            "company": None,
            "email": person.get("email"),
        }
        profile_id = upsert_profile(profile)
        upsert_engagement(post_id, profile_id, "like")
        saved += 1
    return saved, skipped


def run():
    queued = get_queued_posts()
    if not queued:
        print("No queued posts. Flag a post in the dashboard first.")
        return

    print(f"Found {len(queued)} queued post(s).")

    for i, post in enumerate(queued):
        if i > 0:
            print("  Waiting 3 minutes before next run to avoid LinkedIn rate limiting...")
            time.sleep(180)

        post_id = post["id"]
        post_url = post["post_url"]
        print(f"\nProcessing post {post_id}: {post_url}")

        try:
            set_phantom_status(post_id, "running")
            print("  Launching PhantomBuster...")
            container_id, org_s3_folder, s3_folder = launch_phantom(post_url)
            print("  Waiting for results...")
            engagers = wait_for_completion(container_id, org_s3_folder, s3_folder, post_url)
            print(f"  Got {len(engagers)} likers from PhantomBuster")
            saved, skipped = save_engagers(post_id, engagers)
            print(f"  Saved {saved} profiles ({skipped} skipped — no LinkedIn URL)")
            set_phantom_status(post_id, "done")
        except Exception as e:
            print(f"  Error: {e}")
            set_phantom_status(post_id, "error")


if __name__ == "__main__":
    run()
    import enrich_profiles
    enrich_profiles.run()
