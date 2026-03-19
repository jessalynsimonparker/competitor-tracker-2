import os
from flask import Flask, jsonify, send_from_directory, request
from database import get_all_posts, flag_post, get_profiles, add_manual_post

app = Flask(__name__, static_folder="dashboard")


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


@app.route("/api/posts/manual", methods=["POST"])
def api_add_manual_post():
    data = request.get_json()
    url = (data.get("url") or "").strip()
    pain_point = (data.get("pain_point") or "").strip()
    poster_company = (data.get("poster_company") or "").strip()
    poster_title = (data.get("poster_title") or "").strip()
    if not url:
        return jsonify({"error": "url is required"}), 400
    post_id = add_manual_post(url, pain_point, poster_company, poster_title)
    return jsonify({"success": True, "post_id": post_id})


@app.route("/api/flag/<int:post_id>", methods=["POST"])
def api_flag(post_id):
    new_val = flag_post(post_id)
    return jsonify({"success": True, "flagged": new_val})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
