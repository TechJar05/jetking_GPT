


import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import requests
import logging
import json
import os

import os
from dotenv import load_dotenv

# ======== LOAD ENV VARIABLES ========
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
POWER_BI_SCOPE = os.getenv("POWER_BI_SCOPE")

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

# Initialize FastAPI
app = FastAPI()

# Allow all origins (you can restrict later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======== FUNCTION: Get Power BI Access Token ========
def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": POWER_BI_SCOPE,
    }

    response = requests.post(TOKEN_URL, data=data)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to get Power BI token")

    token_data = response.json()
    access_token = token_data.get("access_token")
    return access_token

# ======== FUNCTION: Get Power BI Dashboards ========
def get_powerbi_dashboards(access_token):
    url = "https://api.powerbi.com/v1.0/myorg/dashboards"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to fetch Power BI dashboards")

    return response.json()

# ======== ROUTE: Chat Endpoint ========
@app.post("/ask")
async def ask_powerbi(request: Request):
    try:
        data = await request.json()
        user_message = data.get("message")

        # Step 1: Authenticate Power BI
        token = get_access_token()

        # Step 2: Example - Get all dashboards
        dashboards = get_powerbi_dashboards(token)

        # Step 3: Respond to chatbot logic
        if "show dashboard" in user_message.lower():
            dashboard_names = [d["displayName"] for d in dashboards.get("value", [])]
            return {"reply": f"Here are your available Power BI dashboards: {', '.join(dashboard_names)}"}

        else:
            return {"reply": "You can ask me to 'show dashboard' to list your Power BI dashboards."}

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ======== ROOT TEST ========
@app.get("/")
def root():
    return {"message": "Power BI Chatbot API is running!"}
