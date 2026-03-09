import sqlite3
from datetime import datetime, timezone
from config import DB_PATH


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                post_url TEXT UNIQUE NOT NULL,
                post_text TEXT,
                posted_date TEXT,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                prev_likes INTEGER DEFAULT 0,
                likes_increased INTEGER DEFAULT 0,
                flagged_for_phantombuster INTEGER DEFAULT 0,
                image_url TEXT,
                first_scraped_at TEXT,
                last_scraped_at TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE posts ADD COLUMN image_url TEXT")
        except Exception:
            pass  # column already exists
        conn.commit()


def upsert_post(post: dict):
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute(
            "SELECT id, likes FROM posts WHERE post_url = ?", (post["post_url"],)
        ).fetchone()

        if existing:
            new_likes = post.get("likes", 0)
            old_likes = existing["likes"]
            likes_increased = 1 if new_likes > old_likes else 0
            prev_likes = old_likes if likes_increased else existing["likes"]

            conn.execute("""
                UPDATE posts SET
                    likes = ?,
                    comments = ?,
                    prev_likes = ?,
                    likes_increased = ?,
                    image_url = ?,
                    last_scraped_at = ?
                WHERE post_url = ?
            """, (
                new_likes,
                post.get("comments", 0),
                prev_likes,
                likes_increased,
                post.get("image_url", ""),
                now,
                post["post_url"],
            ))
        else:
            conn.execute("""
                INSERT INTO posts (
                    company_name, post_url, post_text, posted_date,
                    likes, comments, prev_likes, likes_increased,
                    flagged_for_phantombuster, image_url, first_scraped_at, last_scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?)
            """, (
                post.get("company_name", ""),
                post["post_url"],
                post.get("post_text", ""),
                post.get("posted_date", ""),
                post.get("likes", 0),
                post.get("comments", 0),
                post.get("image_url", ""),
                now,
                now,
            ))
        conn.commit()


def get_all_posts() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM posts ORDER BY posted_date DESC"
        ).fetchall()
        return [dict(row) for row in rows]


def flag_post(post_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE posts SET flagged_for_phantombuster = NOT flagged_for_phantombuster
            WHERE id = ?
        """, (post_id,))
        conn.commit()
