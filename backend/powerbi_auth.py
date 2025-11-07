# powerbi_auth.py
import os
import time
import logging
import requests
from urllib.parse import urlencode
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("powerbi_auth")

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
# scopes space-separated in env
SCOPES = os.getenv("POWER_BI_SCOPES")

if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET and REDIRECT_URI and SCOPES):
    logger.warning("Power BI env variables missing — check .env")

AUTH_BASE = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0"
AUTHORIZE_URL = f"{AUTH_BASE}/authorize"
TOKEN_URL = f"{AUTH_BASE}/token"

router = APIRouter()

# Simple in-memory store for demo only — replace with DB or secure session storage
# Structure: {user_id: {"access_token":..., "refresh_token":..., "expires_at":...}}
TOKEN_STORE = {}

# Helper to build authorize URL
def build_authorize_url(state: str = "powerbi_state"):
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "response_mode": "query",
        "scope": SCOPES,
        "state": state,
    }
    return AUTHORIZE_URL + "?" + urlencode(params)

@router.get("/api/powerbi/login")
async def powerbi_login(response: Response):
    """
    Redirect user to Azure AD sign-in for delegated Power BI permissions.
    Frontend should hit this endpoint (opens sign-in page).
    """
    url = build_authorize_url()
    return RedirectResponse(url)

@router.get("/api/powerbi/callback")
async def powerbi_callback(request: Request):
    """
    Azure will redirect here with ?code=...&state=...
    Exchange code for tokens and store them (demo: returns simple JSON).
    """
    params = dict(request.query_params)
    error = params.get("error")
    if error:
        error_desc = params.get("error_description", "")
        raise HTTPException(status_code=400, detail=f"Auth error: {error} {error_desc}")

    code = params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="Authorization code not found in callback")

    # Exchange authorization code for tokens
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPES,  # some endpoints don't require scope here, but it's safe
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(TOKEN_URL, data=data, headers=headers)
    if resp.status_code != 200:
        logger.error("Token exchange failed: %s", resp.text)
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {resp.text}")

    token_data = resp.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in", 3600)
    id_token = token_data.get("id_token")  # optional

    # For demo: extract user identifier from id_token if present, else fallback to timestamp
    # NOTE: In production decode id_token (JWT) to get user's oid or preferred_username.
    user_key = f"user_{int(time.time())}"

    TOKEN_STORE[user_key] = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": time.time() + int(expires_in),
        "id_token": id_token,
    }

    # Return a friendly message and a small token_key that frontend can use to call other endpoints
    return JSONResponse({
        "message": "Power BI authentication successful. Save token_key for future API calls.",
        "token_key": user_key,
        "expires_in": expires_in
    })


# Helper to get a valid access token for a stored user token_key
def get_valid_token_for(token_key: str):
    record = TOKEN_STORE.get(token_key)
    if not record:
        raise HTTPException(status_code=401, detail="No token found for this token_key. Please re-authenticate.")
    if time.time() > record["expires_at"] - 60:
        # refresh
        refresh_token = record.get("refresh_token")
        if not refresh_token:
            raise HTTPException(status_code=401, detail="No refresh token available; please re-authenticate.")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": SCOPES,
        }
        r = requests.post(TOKEN_URL, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        if r.status_code != 200:
            logger.error("Refresh token failed: %s", r.text)
            # delete old record to force re-auth
            TOKEN_STORE.pop(token_key, None)
            raise HTTPException(status_code=401, detail="Refresh failed; please re-authenticate.")
        d = r.json()
        record["access_token"] = d.get("access_token")
        record["refresh_token"] = d.get("refresh_token", refresh_token)
        record["expires_at"] = time.time() + int(d.get("expires_in", 3600))
        TOKEN_STORE[token_key] = record
    return record["access_token"]

# Example: list dashboards for authenticated user
@router.get("/api/powerbi/dashboards")
async def list_dashboards(token_key: str):
    """
    Pass 'token_key' returned from callback. Example:
    GET /api/powerbi/dashboards?token_key=user_12345
    """
    access_token = get_valid_token_for(token_key)
    url = "https://api.powerbi.com/v1.0/myorg/dashboards"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if resp.status_code != 200:
        logger.error("Power BI API error: %s", resp.text)
        raise HTTPException(status_code=400, detail=f"Power BI API error: {resp.text}")
    return resp.json()
