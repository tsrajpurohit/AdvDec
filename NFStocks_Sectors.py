import aiohttp
import asyncio
import pandas as pd
import io
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ==========================
# CONFIGURATION
# ==========================
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
WORKSHEET_NAME = "StocksSectors"

CSV_URL = "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"

# ==========================
# GOOGLE SHEETS AUTH
# ==========================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

credentials_info = json.loads(credentials_json)

credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ]
)

client = gspread.authorize(credentials)

# ==========================
# REQUEST HEADERS
# ==========================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
    "Accept": "text/csv,*/*"
}

# ==========================
# DOWNLOAD CSV
# ==========================
async def fetch_data():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            CSV_URL,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            response.raise_for_status()

            content = await response.read()

            df = pd.read_csv(
                io.StringIO(content.decode("utf-8"))
            )

            print(f"✅ Downloaded {len(df)} rows")

            return df

# ==========================
# GOOGLE SHEETS UPLOAD
# ==========================
def upload_to_google_sheets(df):
    try:
        spreadsheet = client.open_by_key(SHEET_ID)

        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            print(f"✅ Found worksheet: {WORKSHEET_NAME}")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=WORKSHEET_NAME,
                rows=str(max(len(df) + 100, 1000)),
                cols=str(max(len(df.columns) + 5, 20))
            )
            print(f"✅ Created worksheet: {WORKSHEET_NAME}")

        worksheet.clear()

        df = df.replace([float("inf"), float("-inf")], "")
        df = df.fillna("")

        data = [df.columns.tolist()] + df.values.tolist()

        worksheet.update(
            range_name="A1",
            values=data,
            value_input_option="RAW"
        )

        print("✅ Uploaded to Google Sheets")

    except Exception as e:
        print(f"❌ Google Sheets upload error: {e}")

# ==========================
# SAVE CSV
# ==========================
def save_to_csv(df):
    try:
        filename = "StocksSectors.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved {filename}")
    except Exception as e:
        print(f"❌ CSV save error: {e}")

# ==========================
# MAIN
# ==========================
async def main():
    try:
        df = await fetch_data()

        upload_to_google_sheets(df)

        save_to_csv(df)

        print("✅ Process completed successfully")

    except Exception as e:
        print(f"❌ Process failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
