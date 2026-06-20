import requests
import pandas as pd
import json
import os
import gspread
from google.oauth2.service_account import Credentials

# =====================
# CONFIG
# =====================
BASE_URL = "https://www.nseindia.com"
API_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-contracts"

SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "OI_Spurts_Contracts"

# =====================
# GOOGLE SHEETS AUTH
# =====================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

if not credentials_json:
    raise ValueError("Missing GOOGLE_SHEETS_CREDENTIALS")

creds = Credentials.from_service_account_info(
    json.loads(credentials_json),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

client = gspread.authorize(creds)

# =====================
# NSE HEADERS
# =====================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.nseindia.com/market-data/oi-spurts"
}

# =====================
# FETCH DATA
# =====================
def fetch_data():
    session = requests.Session()

    # step 1: get cookies
    session.get(BASE_URL, headers=HEADERS, timeout=10)

    # step 2: call API
    response = session.get(API_URL, headers=HEADERS, timeout=15)
    response.raise_for_status()

    data = response.json()

    # NSE usually wraps in "data"
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    df = pd.DataFrame(data)

    print(f"Downloaded rows: {len(df)}")

    return df

# =====================
# UPLOAD TO SHEETS
# =====================
def upload(df):
    sheet = client.open_by_key(SHEET_ID)

    try:
        ws = sheet.worksheet(TAB_NAME)
        ws.clear()
    except:
        ws = sheet.add_worksheet(TAB_NAME, rows="1000", cols="30")

    df = df.fillna("")

    values = [df.columns.tolist()] + df.values.tolist()

    ws.update("A1", values, value_input_option="RAW")

    print("Uploaded to Google Sheets")

# =====================
# MAIN
# =====================
def main():
    df = fetch_data()
    upload(df)
    print("DONE")

if __name__ == "__main__":
    main()
