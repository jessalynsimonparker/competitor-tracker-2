from datetime import datetime, timezone
from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_post(post: dict):
    now = datetime.now(timezone.utc).isoformat()

    existing = supabase.table("posts").select("id, likes").eq("post_url", post["post_url"]).execute()

    if existing.data:
        row = existing.data[0]
        new_likes = post.get("likes", 0)
        old_likes = row["likes"] or 0
        likes_increased = new_likes > old_likes

        supabase.table("posts").update({
            "likes": new_likes,
            "comments": post.get("comments", 0),
            "prev_likes": old_likes,
            "likes_increased": likes_increased,
            "image_url": post.get("image_url", ""),
            "last_updated_at": now,
        }).eq("id", row["id"]).execute()

        supabase.table("engagement_history").insert({
            "post_id": row["id"],
            "likes": new_likes,
            "comments": post.get("comments", 0),
        }).execute()
    else:
        supabase.table("posts").insert({
            "company_name": post.get("company_name", ""),
            "post_url": post["post_url"],
            "post_text": post.get("post_text", ""),
            "posted_date": post.get("posted_date", ""),
            "likes": post.get("likes", 0),
            "comments": post.get("comments", 0),
            "prev_likes": 0,
            "likes_increased": False,
            "flagged": False,
            "image_url": post.get("image_url", ""),
            "first_seen_at": now,
            "last_updated_at": now,
        }).execute()


def get_profiles() -> list[dict]:
    # Get all profiles with their engagement count and which posts they engaged with
    result = supabase.table("profiles").select(
        "*, engagement(post_id, engagement_type, posts(company_name, post_url, post_text))"
    ).order("last_updated_at", desc=True).execute()
    profiles = []
    for p in result.data:
        engagements = p.pop("engagement", []) or []
        p["engagement_count"] = len(engagements)
        p["engaged_posts"] = [
            {
                "post_id": e["post_id"],
                "company_name": (e.get("posts") or {}).get("company_name", ""),
                "post_url": (e.get("posts") or {}).get("post_url", ""),
                "post_text": ((e.get("posts") or {}).get("post_text") or "")[:80],
            }
            for e in engagements
        ]
        profiles.append(p)
    return profiles


def get_all_posts() -> list[dict]:
    result = supabase.table("posts").select("*").order("posted_date", desc=True).execute()
    return result.data


def flag_post(post_id: int) -> bool:
    current = supabase.table("posts").select("flagged").eq("id", post_id).execute()
    new_val = not current.data[0]["flagged"]
    update = {"flagged": new_val}
    if new_val:
        update["phantom_status"] = "queued"
    else:
        update["phantom_status"] = None
    supabase.table("posts").update(update).eq("id", post_id).execute()
    return new_val


def auto_flag_top_posts(company_name: str, top_n: int = 2):
    """Flag the top N posts by net new likes for a company. Re-queues done posts if they have new likes."""
    result = supabase.table("posts").select("id, likes, prev_likes, phantom_status").eq("company_name", company_name).execute()
    posts = result.data
    if not posts:
        return 0

    # Score by net new likes; only consider posts with actual new likes that haven't been scraped yet
    scored = [
        (p["id"], (p["likes"] or 0) - (p["prev_likes"] or 0))
        for p in posts
        if ((p["likes"] or 0) - (p["prev_likes"] or 0)) > 0
        and p.get("phantom_status") != "done"
    ]
    if not scored:
        return 0

    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[:top_n]

    flagged = 0
    for post_id, net_new in top:
        supabase.table("posts").update({
            "flagged": True,
            "phantom_status": "queued",
        }).eq("id", post_id).execute()
        print(f"  Auto-flagged post {post_id} (+{net_new} net new likes)")
        flagged += 1
    return flagged


def _extract_poster_linkedin_url(post_url: str) -> str:
    """Extract linkedin.com/in/[slug] from a LinkedIn post URL."""
    import re
    match = re.search(r'/posts/([^_?/]+)', post_url)
    if match:
        slug = match.group(1)
        return f"https://www.linkedin.com/in/{slug}/"
    return ""


def _posted_date_from_url(post_url: str) -> str:
    """Decode posted date from a LinkedIn activity ID embedded in the post URL.

    LinkedIn activity IDs are snowflake-style: top 41 bits = milliseconds since
    the unix epoch. So we can recover the post's publish date even when the
    public HTML doesn't expose it (which is the case for un-authenticated fetches).

    Returns ISO-8601 string like '2026-02-19T03:14:22+00:00', or '' if no
    activity ID is found in the URL.
    """
    import re
    m = re.search(r'activity[:-](\d+)', post_url)
    if not m:
        return ""
    activity_id = int(m.group(1))
    timestamp_ms = activity_id >> 22
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def update_post_like_count_from_engagement(post_id: int) -> int:
    """Set posts.likes for a post equal to its engagement-row count. Returns the new count.

    For manual posts we can't read likes from public HTML, so PhantomBuster's
    scraped likers are the only source of truth. Call this after each phantom
    run finishes a post.
    """
    result = (
        supabase.table("engagement")
        .select("id", count="exact")
        .eq("post_id", post_id)
        .eq("engagement_type", "like")
        .execute()
    )
    count = result.count or 0
    supabase.table("posts").update({"likes": count}).eq("id", post_id).execute()
    return count


def add_manual_post(post_url: str, pain_point: str, poster_company: str = "", poster_title: str = "") -> int:
    from scraper import fetch_og_metadata
    from enrich_profiles import push_poster_to_clay
    meta = fetch_og_metadata(post_url)
    poster_linkedin_url = _extract_poster_linkedin_url(post_url)
    posted_date = _posted_date_from_url(post_url)
    now = datetime.now(timezone.utc).isoformat()
    poster_fields = {
        "poster_name": meta.get("poster_name") or "",
        "poster_company": poster_company,
        "poster_title": poster_title,
        "poster_linkedin_url": poster_linkedin_url,
    }
    existing = supabase.table("posts").select("id").eq("post_url", post_url).execute()
    if existing.data:
        post_id = existing.data[0]["id"]
        supabase.table("posts").update({
            "flagged": True,
            "phantom_status": "queued",
            "pain_point": pain_point,
            "source": "manual",
            "post_text": meta["text"] or "",
            "image_url": meta["image"] or "",
            "likes": meta["likes"] or 0,
            "posted_date": posted_date,
            "last_updated_at": now,
            **poster_fields,
        }).eq("id", post_id).execute()
    else:
        supabase.table("posts").insert({
            "post_url": post_url,
            "company_name": "Manual",
            "post_text": meta["text"] or "",
            "image_url": meta["image"] or "",
            "pain_point": pain_point,
            "source": "manual",
            "flagged": True,
            "phantom_status": "queued",
            "likes": meta["likes"] or 0,
            "comments": 0,
            "prev_likes": 0,
            "likes_increased": False,
            "posted_date": posted_date,
            "first_seen_at": now,
            "last_updated_at": now,
            **poster_fields,
        }).execute()

    # Push poster to Clay for enrichment if we have a LinkedIn URL
    if poster_linkedin_url:
        push_poster_to_clay(post_url, poster_linkedin_url)
        print(f"  Pushed poster {poster_linkedin_url} to Clay for enrichment")

    result = supabase.table("posts").select("id").eq("post_url", post_url).execute()
    return result.data[0]["id"]


def get_queued_posts() -> list[dict]:
    result = supabase.table("posts").select("id, post_url, company_name").eq("phantom_status", "queued").execute()
    return result.data


def set_phantom_status(post_id: int, status: str):
    supabase.table("posts").update({"phantom_status": status}).eq("id", post_id).execute()


def upsert_profile(profile: dict) -> int:
    existing = supabase.table("profiles").select("id").eq("linkedin_url", profile["linkedin_url"]).execute()
    now = datetime.now(timezone.utc).isoformat()
    if existing.data:
        profile_id = existing.data[0]["id"]
        supabase.table("profiles").update({
            "full_name": profile.get("full_name"),
            "title": profile.get("title"),
            "company": profile.get("company"),
            "email": profile.get("email"),
            "last_updated_at": now,
        }).eq("id", profile_id).execute()
    else:
        result = supabase.table("profiles").insert({
            "linkedin_url": profile["linkedin_url"],
            "full_name": profile.get("full_name"),
            "title": profile.get("title"),
            "company": profile.get("company"),
            "email": profile.get("email"),
            "first_seen_at": now,
            "last_updated_at": now,
        }).execute()
        profile_id = result.data[0]["id"]
    return profile_id


def upsert_engagement(post_id: int, profile_id: int, engagement_type: str):
    supabase.table("engagement").upsert({
        "post_id": post_id,
        "profile_id": profile_id,
        "engagement_type": engagement_type,
    }, on_conflict="post_id,profile_id,engagement_type").execute()
