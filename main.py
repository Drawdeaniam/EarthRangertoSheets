import os
import json
import requests
import pandas as pd
import re
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- 1. CONFIGURATION ---
ER_DOMAIN = "ack.pamdas.org"
ER_TOKEN = os.getenv("ER_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

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

# --- 2. HELPERS ---
def normalize_species_name(name):
    if not isinstance(name, str) or name.lower() == 'nan': return ""
    name = re.sub(r"\s*\(unidentified\)", "", name, flags=re.IGNORECASE).lower().strip()
    return name.replace("dik dik", "dikdik").replace("zebra grevy's", "grevy's zebra")

def fetch_er_data():
    headers = {"Authorization": f"Bearer {ER_TOKEN}"}
    url = f"https://{ER_DOMAIN}/api/v1.0/activity/events/?page_size=300"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        json_res = resp.json()
        return json_res.get('data', {}).get('results', []) or json_res.get('data', [])
    print(f"❌ API Error: {resp.status_code}")
    return []

# --- 3. PROCESSING ---
def clean_and_process(data):
    rows = []
    for event in data:
        details = event.get('event_details', {})
        internal_val = event.get('event_type')
        
        # Use simple mapping for types
        mapped_type = REPORT_TYPE_MAP.get(internal_val, event.get('event_type_label', internal_val))
        
        # Species
        dom_spec = details.get('patrolack_speciesdomestic') or details.get('routineack_speciesdomestic')
        wild_spec = details.get('patrolackwild_specieswild') or details.get('routineack_specieswild')
        norm_species = normalize_species_name(str(dom_spec if dom_spec else wild_spec))

        rows.append({
            'Report_Id': f"ER{event.get('serial_number')}",
            'Report_Type': mapped_type,
            'Reported_By': event.get('reported_by', {}).get('name', 'Unknown').replace(" ACK", ""),
            'Raw_Time': event.get('time'),
            'Latitude': event.get('location', {}).get('latitude'),
            'Longitude': event.get('location', {}).get('longitude'),
            'Species': norm_species,
            'Trophic': SPECIES_REFERENCE.get(norm_species, ""),
            'Number': details.get('patrolack_nb') or details.get('patrolackwild_nb'),
            'Ground_Cover': str(details.get('patrolack_groundcover', '')),
            'Habitat': str(details.get('patrolack_habitat', '')),
            'Blocks': details.get('routineack_block', ''),
            'Transects': details.get('transectack_block') or details.get('transects', '')
        })
    
    df = pd.DataFrame(rows)
    if df.empty: return df

    df["Reported_At"] = pd.to_datetime(df["Raw_Time"]) + pd.Timedelta(hours=3)
    df["Date"] = df["Reported_At"].dt.strftime("%d/%m/%Y")
    df["Time"] = df["Reported_At"].dt.strftime("%I:%M %p")
    
    return df.fillna("")

# --- 4. EXPORT ---
def push_to_sheets(df_dict):
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    
    for tab_name, df in df_dict.items():
        try:
            worksheet = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=tab_name, rows="100", cols="20")
        
        worksheet.clear()
        if not df.empty:
            worksheet.update('A1', [df.columns.values.tolist()] + df.values.tolist())
            print(f"✅ Updated {tab_name} with {len(df)} rows.")
        else:
            worksheet.update('A1', [['No data found for this category']])
            print(f"⚠️ {tab_name} cleared (no data found).")

# --- EXECUTION ---
if __name__ == "__main__":
    raw_data = fetch_er_data()
    now_eat = (datetime.utcnow() + pd.Timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    
    if raw_data:
        full_df = clean_and_process(raw_data)
        
        # Filter for the two tabs
        rp_data = full_df[full_df["Report_Type"].str.contains("Patrol", case=False)]
        wt_data = full_df[full_df["Report_Type"].str.contains("Transect", case=False)]
        
        push_to_sheets({"Sheet6": rp_data, "Sheet7": wt_data})
        
        # Add a Sync_Log tab
        log_df = pd.DataFrame([{"Last_Sync_EAT": now_eat, "RP_Rows": len(rp_data), "WT_Rows": len(wt_data)}])
        push_to_sheets({"Sync_Log": log_df})
    else:
        print("❌ No data received from EarthRanger.")

