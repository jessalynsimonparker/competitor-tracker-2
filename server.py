from flask import Flask, jsonify, send_from_directory
from database import init_db, get_all_posts, flag_post

app = Flask(__name__, static_folder="dashboard")


@app.route("/")
def index():
    return send_from_directory("dashboard", "index.html")


@app.route("/api/posts")
def api_posts():
    posts = get_all_posts()
    return jsonify(posts)


@app.route("/api/flag/<int:post_id>", methods=["POST"])
def api_flag(post_id):
    flag_post(post_id)
    return jsonify({"success": True})


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000)
