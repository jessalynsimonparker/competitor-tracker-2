import os
from flask import Flask, jsonify, send_from_directory
from database import get_all_posts, flag_post, get_profiles

app = Flask(__name__, static_folder="dashboard")


@app.route("/api/debug")
def api_debug():
    url = os.environ.get("SUPABASE_URL", "NOT SET")
    key = os.environ.get("SUPABASE_KEY", "NOT SET")
    return jsonify({
        "SUPABASE_URL": url,
        "SUPABASE_KEY_prefix": key[:20] if key != "NOT SET" else "NOT SET",
    })


@app.route("/")
def index():
    return send_from_directory("dashboard", "index.html")


@app.route("/api/posts")
def api_posts():
    posts = get_all_posts()
    return jsonify(posts)


@app.route("/api/profiles")
def api_profiles():
    profiles = get_profiles()
    return jsonify(profiles)


@app.route("/api/flag/<int:post_id>", methods=["POST"])
def api_flag(post_id):
    new_val = flag_post(post_id)
    return jsonify({"success": True, "flagged": new_val})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
