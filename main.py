import requests
import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials

# --- 1. CONFIGURATION ---
ER_DOMAIN = "ack.pamdas.org"
ER_TOKEN = "v8K9n2P5mR7jW1qX4zB6tS3hL9vY0uN8eC2xA5mQ"
SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"
SERVICE_ACCOUNT_FILE = "service_account.json"

# --- 2. SPECIES REFERENCE DICTIONARY ---
SPECIES_REFERENCE = {
    "cheetah": "Carnivore", "lion": "Carnivore", "leopard": "Carnivore", 
    "spotted hyena": "Carnivore", "striped hyena": "Carnivore", "jackal": "Carnivore", 
    "caracal": "Carnivore", "african wild dog": "Carnivore", "serval": "Carnivore",
    "hare": "Preferred cheetah prey", "hyrax": "Preferred cheetah prey", "dikdik": "Preferred cheetah prey", 
    "guinea fowl": "Preferred cheetah prey", "yellow-necked spurfowl": "Preferred cheetah prey", 
    "grant's gazelle": "Preferred cheetah prey", "gerenuk": "Preferred cheetah prey", 
    "klipspringer": "Preferred cheetah prey", "lesser kudu": "Preferred cheetah prey", 
    "impala": "Preferred cheetah prey", "steenbuck": "Preferred cheetah prey", 
    "bushbuck": "Preferred cheetah prey", "thomson's gazelle": "Preferred cheetah prey", 
    "duiker": "Preferred cheetah prey", "springhare": "Preferred cheetah prey",
    "vervet monkey": "Sometimes cheetah prey", "goats": "Sometimes cheetah prey", 
    "sheep": "Sometimes cheetah prey", "greater kudu": "Sometimes cheetah prey", 
    "ostrich": "Sometimes cheetah prey", "warthog": "Sometimes cheetah prey",
    "eland": "Seldom or never cheetah prey", "grevy zebra": "Seldom or never cheetah prey", 
    "common zebra": "Seldom or never cheetah prey", "baboon": "Seldom or never cheetah prey", 
    "cattle": "Seldom or never cheetah prey", "camel": "Seldom or never cheetah prey", 
    "buffalo": "Seldom or never cheetah prey", "hippo": "Seldom or never cheetah prey", 
    "giraffe": "Seldom or never cheetah prey", "elephant": "Seldom or never cheetah prey", 
    "bushpig": "Seldom or never cheetah prey", "donkey": "Seldom or never cheetah prey",
    "bat-eared fox": "Unclassified", "african civet": "Unclassified", "genet": "Unclassified", 
    "domestic dog": "Unclassified", "honey badger": "Unclassified", "kori bustard": "Unclassified", "vulture": "Unclassified"
}

REPORT_TYPE_MAP = {
    "patrol_domesticanimal": "Patrol - Domestic Animal Sighting",
    "patrol_info_ack": "Patrol Info",
    "patrolwildanimal_sight": "Patrol - Wild Animal Type",
    "transect_domestic_sight": "Transect - Domestic Animal Type",
    "transect_wildanimal_sight": "Transect - Wild Animal Type",
    "transectinfo_ack": "Transect Info"
}

# --- 3. LOGIC FUNCTIONS ---
def fetch_er_data():
    headers = {"Authorization": f"Bearer {ER_TOKEN}"}
    url = f"https://{ER_DOMAIN}/api/v1.0/activity/events/?page_size=300"
    resp = requests.get(url, headers=headers)
    return resp.json().get('data', []) if resp.status_code == 200 else []

def normalize_species_name(name):
    if not isinstance(name, str) or name.lower() == 'nan': return ""
    # Standardize to lowercase and remove unidentified tags
    name = re.sub(r"\s*\(unidentified\)", "", name, flags=re.IGNORECASE).lower().strip()
    # Ensure "dik dik" matches the dictionary "dikdik"
    return name.replace("dik dik", "dikdik")

def clean_and_process(raw_data):
    rows = []
    for event in raw_data:
        details = event.get('event_details', {})
        internal_val = event.get('event_type', '')
        cat_name = "transect" if "transect" in internal_val.lower() else "patrol"
        loc_val = details.get('routineack_block') or details.get('transectack_block') or details.get('transects') or ""

        rows.append({
            'Report_Id': event.get('serial_number'),
            'Report_Type': REPORT_TYPE_MAP.get(internal_val, event.get('event_type_label')),
            'Reported_By': event.get('reported_by', {}).get('username', 'Unknown'),
            'Reported_At_Raw': event.get('updated_at') or event.get('time'),
            'Latitude': event.get('location', {}).get('latitude') if event.get('location') else None,
            'Longitude': event.get('location', {}).get('longitude') if event.get('location') else None,
            'Domestic_Animal_Species': details.get('patrolack_speciesdomestic'),
            'Wild_Animal_Species': details.get('patrolackwild_specieswild'),
            'Number': details.get('patrolack_nb') or details.get('patrolackwild_nb'),
            'Blocks': loc_val if cat_name == "patrol" else "",
            'Transects': loc_val if cat_name == "transect" else ""
        })
    
    df = pd.DataFrame(rows)
    if df.empty: return df

    # Time Handling
    df["Reported_At"] = pd.to_datetime(df["Reported_At_Raw"]) + pd.Timedelta(hours=3)
    df["Date"] = df["Reported_At"].dt.strftime("%d/%m/%Y")
    
    # SPECIES CATEGORIZATION
    df["Species"] = df["Domestic_Animal_Species"].combine_first(df["Wild_Animal_Species"]).apply(normalize_species_name)
    
    # Apply the dictionary to create the Trophic Level column
    df["Species_Category"] = df["Species"].map(SPECIES_REFERENCE).fillna("Unclassified")
    
    return df.fillna("")

# --- 4. UPLOAD ---
def push_to_sheets(df, tab_name):
    if df.empty: return
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    
    try:
        worksheet = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=tab_name, rows="100", cols="20")
    
    worksheet.clear()
    worksheet.update('A1', [df.columns.values.tolist()] + df.values.tolist())

# --- RUN ---
data = fetch_er_data()
if data:
    final_df = clean_and_process(data)
    push_to_sheets(final_df[final_df["Report_Type"].str.contains("Patrol", na=False)], "RP")
    push_to_sheets(final_df[final_df["Report_Type"].str.contains("Transect", na=False)], "WT")
    print("🚀 RP and WT Tabs updated with Species Categories!")