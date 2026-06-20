import requests
import pandas as pd
import json
import os
import gspread
from google.oauth2.service_account import Credentials

# ======================
# CONFIG
# ======================
BASE_URL = "https://www.nseindia.com"
API_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"

SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "OI_Spurts_Contracts"

# ======================
# AUTH GOOGLE SHEETS
# ======================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS not set")

creds = Credentials.from_service_account_info(
    json.loads(credentials_json),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

client = gspread.authorize(creds)

# ======================
# HEADERS (NSE BLOCK PROTECTION)
# ======================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.nseindia.com/market-data/oi-spurts"
}

# ======================
# FETCH DATA
# ======================
def fetch_data():
    session = requests.Session()

    # Step 1: get cookies (IMPORTANT)
    session.get(BASE_URL, headers=HEADERS, timeout=10)

    # Step 2: call API
    res = session.get(API_URL, headers=HEADERS, timeout=15)
    res.raise_for_status()

    data = res.json()

    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    df = pd.DataFrame(data)

    print(f"Downloaded rows: {len(df)}")

    return df

# ======================
# CLEAN DATA (FIX YOUR ERROR)
# ======================
def clean_value(x):
    if isinstance(x, (dict, list)):
        return json.dumps(x)  # convert nested → string
    return x

# ======================
# UPLOAD TO GOOGLE SHEETS
# ======================
def upload(df):
    sheet = client.open_by_key(SHEET_ID)

    try:
        ws = sheet.worksheet(TAB_NAME)
        ws.clear()
    except:
        ws = sheet.add_worksheet(TAB_NAME, rows="1000", cols="30")

    # 🔥 CRITICAL CLEANING STEP
    df = df.applymap(clean_value)
    df = df.fillna("").astype(str)

    values = [df.columns.tolist()] + df.values.tolist()

    ws.update(
        "A1",
        values,
        value_input_option="RAW"
    )

    print("Uploaded to Google Sheets successfully")

# ======================
# MAIN
# ======================
def main():
    df = fetch_data()

    if df is None or df.empty:
        print("No data received")
        return

    upload(df)

    print("DONE")

if __name__ == "__main__":
    main()
