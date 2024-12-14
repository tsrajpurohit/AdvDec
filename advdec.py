import os
import json
import logging
import time
import random
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from nsepython import *

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
SHEET_ID = os.getenv('GOOGLE_SHEET_ID')  # Sheet ID from environment

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
if not SHEET_ID:
    raise ValueError("GOOGLE_SHEET_ID environment variable is not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

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

def validate_and_convert_to_dataframe(data, tab_name):
    """Ensure data is in DataFrame format, or convert it."""
    logging.info(f"Validating data for {tab_name}, type: {type(data)}")
    if isinstance(data, pd.DataFrame):
        logging.info(f"{tab_name} data is already a DataFrame.")
        return data
    elif isinstance(data, list):
        return pd.DataFrame(data)
    elif isinstance(data, dict):
        return pd.DataFrame([data])  # Convert dict to DataFrame (single-row)
    else:
        logging.warning(f"Unexpected data format for {tab_name}. Skipping.")
        return None

def flatten_dataframe(dataframe):
    """Flatten nested structures and trim large text values in DataFrame."""
    for col in dataframe.columns:
        if dataframe[col].dtype == 'object':
            dataframe[col] = dataframe[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
            dataframe[col] = dataframe[col].apply(lambda x: x[:50000] if isinstance(x, str) and len(x) > 50000 else x)
    return dataframe

def fetch_nse_data_with_retries(retries=3, delay=2):
    """Fetch NSE data with retry logic."""
    for attempt in range(retries):
        try:
            return fetch_nse_data()
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                sleep_time = random.uniform(delay, delay * 2)
                logging.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logging.error("Max retries reached. Data fetch failed.")
                return None

# Fetch data from NSE API
def fetch_nse_data():
    """Fetch NSE data for different categories."""
    try:
        # Fetch data for most active securities
        most_active_data = nse_most_active(type="securities", sort="value")
        return most_active_data
    except Exception as e:
        logging.error(f"Error fetching NSE data: {e}")
        return None

def save_data_to_csv(dataframe, file_name):
    """Save the dataframe to a CSV file in the current directory."""
    try:
        file_path = os.path.join(os.getcwd(), f"{file_name}.csv")
        dataframe.to_csv(file_path, index=False)
        logging.info(f"Data saved to CSV: {file_path}")
    except Exception as e:
        logging.error(f"Error saving {file_name} to CSV: {e}")

def save_data_to_google_sheets_and_csv():
    """Fetch data from NSE API, process, upload to Google Sheets, and save to CSV files."""
    # Fetch data from NSE
    most_active_data = fetch_nse_data()

    # Process Most Active Data
    if isinstance(most_active_data, dict):
        most_active_data = pd.DataFrame([most_active_data])
    if most_active_data is not None and not most_active_data.empty:
        most_active_df = validate_and_convert_to_dataframe(most_active_data, "Most Active")
        most_active_df = flatten_dataframe(most_active_df)
        # Upload to Google Sheets
        upload_to_google_sheets(SHEET_ID, "Most Active", most_active_df)
        # Save to CSV
        save_data_to_csv(most_active_df, "Most_Active")

    # Fetch data
    data = nse_get_advances_declines("index")

    # Remove 'meta' portion if it exists
    if isinstance(data, dict) and "meta" in data:
        del data["meta"]

    # Convert data to DataFrame
    if isinstance(data, dict):
        data = data.get("data", [])
    if data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        raise ValueError("Data is not in a suitable format for DataFrame conversion")

    # Clean invalid values
    df = df.applymap(lambda x: "" if isinstance(x, (dict, list)) or pd.isnull(x) else x)

    # Save to CSV
    csv_path = "advances_declines.csv"
    df.to_csv(csv_path, index=False)
    logging.info(f"Data successfully saved to {csv_path}")

    # Upload to Google Sheets
    upload_to_google_sheets(SHEET_ID, "Adv_Dec", df)

if __name__ == "__main__":
    save_data_to_google_sheets_and_csv()
