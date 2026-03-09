import os
from dotenv import load_dotenv
load_dotenv()

BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY", "")
BRIGHTDATA_DATASET_ID = "gd_lyy3tktm25m4avu764"

COMPETITOR_URLS = [
    "https://www.linkedin.com/company/poka-inc-/",
    "https://www.linkedin.com/company/tulip-interfaces/",
]

DB_PATH = "posts.db"
ROLLING_WINDOW_DAYS = 30

