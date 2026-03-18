from scraper import wait_for_snapshot, normalize_post
from database import upsert_post, auto_flag_top_posts

posts = wait_for_snapshot("sd_mmwe0kzv12j7nak")
company = "Poka Inc "
for raw in posts:
    post = normalize_post(raw, company)
    if post["post_url"]:
        upsert_post(post)
        print("Saved:", post["post_url"])
flagged = auto_flag_top_posts(company, top_n=2)
print(f"Auto-flagged {flagged} posts")
