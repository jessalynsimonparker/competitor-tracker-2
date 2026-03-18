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
