import os
from dotenv import load_dotenv
load_dotenv()

BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY", "")
BRIGHTDATA_DATASET_ID = "gd_lyy3tktm25m4avu764"

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
PHANTOMBUSTER_API_KEY = os.getenv("PHANTOMBUSTER_API_KEY", "")
PHANTOMBUSTER_AGENT_ID = os.getenv("PHANTOMBUSTER_AGENT_ID", "")
PHANTOMBUSTER_LIKERS_AGENT_ID = os.getenv("PHANTOMBUSTER_LIKERS_AGENT_ID", "")

COMPETITOR_URLS = [
    "https://www.linkedin.com/company/poka-inc-/",
    "https://www.linkedin.com/company/tulip-interfaces/",
]

ROLLING_WINDOW_DAYS = 30

