"""
backfill_manual_posts.py — One-time backfill for manual posts.

Two fixes applied to existing rows where source='manual':
  1. Set likes = engagement-row count (PhantomBuster already scraped likers,
     but we never wrote the count back to posts.likes).
  2. Set posted_date by decoding the activity ID from the URL.

Idempotent — safe to re-run.
"""
from database import supabase, _posted_date_from_url, update_post_like_count_from_engagement


def main():
    rows = supabase.table("posts").select("id, post_url, likes, posted_date, source").eq("source", "manual").execute().data
    print(f"Found {len(rows)} manual posts.")

    for r in rows:
        post_id = r["id"]
        before_likes = r.get("likes")
        before_date = r.get("posted_date")

        new_likes = update_post_like_count_from_engagement(post_id)

        new_date = _posted_date_from_url(r["post_url"])
        if new_date and not before_date:
            supabase.table("posts").update({"posted_date": new_date}).eq("id", post_id).execute()

        print(
            f"  post {post_id}: likes {before_likes} → {new_likes}"
            f" | posted_date {before_date or '∅'} → {new_date or '∅'}"
        )


if __name__ == "__main__":
    main()
