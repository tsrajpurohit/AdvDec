import os
import json
import logging
import pandas as pd
import time
import random
import gspread
from google.oauth2.service_account import Credentials
from nsepython import nse_most_active, nse_get_advances_declines

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

# Google Sheets Upload
def upload_to_google_sheets(sheet_id, tab_name, dataframe):
    """Upload the provided dataframe to a Google Sheet."""
    sheet = client.open_by_key(sheet_id)

    # Try to find the worksheet or create a new one
    try:
        worksheet = sheet.worksheet(tab_name)
        worksheet.clear()  # Clear existing data
        logging.info(f"Worksheet '{tab_name}' found, cleared existing data.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows=str(len(dataframe) + 1), cols=str(len(dataframe.columns)))
        logging.info(f"Worksheet '{tab_name}' not found. Created a new one.")

    # Update worksheet with DataFrame data
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    logging.info(f"Data uploaded to '{tab_name}' successfully.")

# Fetch NSE Data
def fetch_nse_data():
    """Fetch NSE data for most active securities."""
    try:
        return nse_most_active(type="securities", sort="value")
    except Exception as e:
        logging.error(f"Error fetching NSE data: {e}")
        return None

def fetch_adv_dec_data():
    """Fetch NSE advances and declines data."""
    try:
        data = nse_get_advances_declines("index")
        # Remove 'meta' portion if it exists
        if isinstance(data, dict) and "meta" in data:
            del data["meta"]
        return data.get("data", []) if isinstance(data, dict) else []
    except Exception as e:
        logging.error(f"Error fetching advances/declines data: {e}")
        return []

# Main Function
def save_data_to_google_sheets_and_csv():
    """Fetch data from NSE API, process it, and upload to Google Sheets and CSV."""

    # Fetch Most Active Data
    most_active_data = fetch_nse_data()
    if most_active_data:
        most_active_df = pd.DataFrame(most_active_data) if isinstance(most_active_data, list) else pd.DataFrame([most_active_data])
        
        # Flatten and clean the DataFrame
        for col in most_active_df.columns:
            if most_active_df[col].dtype == 'object':
                most_active_df[col] = most_active_df[col].apply(
                    lambda x: str(x) if isinstance(x, (dict, list)) else x
                )
                most_active_df[col] = most_active_df[col].apply(
                    lambda x: x[:50000] if isinstance(x, str) and len(x) > 50000 else x
                )

        # Upload to Google Sheets
        upload_to_google_sheets(SHEET_ID, "Most Active", most_active_df)

        # Save to CSV
        csv_path = os.path.join(os.getcwd(), "Most_Active.csv")
        most_active_df.to_csv(csv_path, index=False)
        logging.info(f"Most Active data saved to CSV: {csv_path}")

    # Fetch Advances/Declines Data
    adv_dec_data = fetch_adv_dec_data()
    if adv_dec_data:
        adv_dec_df = pd.DataFrame(adv_dec_data) if adv_dec_data else pd.DataFrame()

        # Clean invalid values
        adv_dec_df = adv_dec_df.applymap(lambda x: "" if isinstance(x, (dict, list)) or pd.isnull(x) else x)

        # Upload to Google Sheets
        upload_to_google_sheets(SHEET_ID, "Adv_Dec", adv_dec_df)

        # Save to CSV
        csv_path = os.path.join(os.getcwd(), "Adv_Dec.csv")
        adv_dec_df.to_csv(csv_path, index=False)
        logging.info(f"Advances/Declines data saved to CSV: {csv_path}")

if __name__ == "__main__":
    save_data_to_google_sheets_and_csv()
