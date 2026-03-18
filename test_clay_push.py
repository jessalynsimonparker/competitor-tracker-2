import requests

CLAY_WEBHOOK_URL = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-5e41dd5c-388c-4a13-b6a5-3b1539df5928"

# Test profile
payload = {
    "linkedin_url": "https://www.linkedin.com/in/claudearchambault/",
    "full_name": "Claude Archambault",
    "headline": "Test",
}

resp = requests.post(CLAY_WEBHOOK_URL, json=payload)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text}")
