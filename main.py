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

# We use triple quotes here to ensure all line breaks are perfectly preserved
SERVICE_ACCOUNT_JSON = {
  "type": "service_account",
  "project_id": "earthranger-integration",
  "private_key_id": "2eaa9d8a90ef0faac75614290e0322e861c48e6a",
  "private_key": """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC/BTxBBEnjDwHo
rDUkACdSRetvZrPhmNyLhEurPD4bYpRQZYBcPv7fbk1TFAsQ10fbZPl5y99KUcxN
7CDb/2gZq/utl0SSSuArOSsRweLa7TJ+UNK+/tsqDMqWunDaI+u59L0dB/7VvDrD
ADRn3hts+zsadsm7MN+flaMYvQgwE0kOM7ME+JdpF8s4eh/FZ9zs6qH6gG3np0WS
rdwWTJLjpRcORXyI/wTK9+DGDz2jWwZ68wPwDOVeuPW2cM4S4KK0qyGhOTJ7xaGT
UpDU4nMytV7tm0nyrVV2Gdjm2SXIz+LJ5oF9eHknABFBebKasEoI4AMMaSZPGIdS
pAHPG205AgMBAAECggEAC5Uf2gS5DYqyP8SlX5P0zmvNQqQpwcL46IAdQyT0J8lF
RfoJm6OkOlxgAEAbCv/gFwuwht6XoP7ZvQXVg5F2jfacMaKWS//nR30AFrkOGN6W
F7ZYjlqPY9AtE+ZP2SJMH6qZMaKeT0vKYlrnp+lTT2XiZ7s5MIpeR45qunbH1tD8
M9Yv9c22yyPCyvpdR7gwQ0fIs9kQzxxq48BR8jwGGkrOgXU/WFOr6PSAt3buXmeH
M4ZvorHmfFUWX58FPOTG/7twMqF2ipdI9eQ/1u2wbnbbop97ho/nxqOJK+UQIK24
paRlPavPowGaNO5mEUDV42h73SCVLwLsboiz73cRuwKBgQD5Tcikg4or0+w1da3a
uDQ4Hjv8CSUJ7MclPh+2jj34m1tt84vte+crfzfk16Q5gX8Ha9it8WU+qFmzIzXf
+cK/euw6skzV4daCrKZgfugbHicA8hFq3rpZRTZWyqlVmKpJ9hcE89MxLRopBkxC
pGoyvbZcglyEUmrRwQw2pgoWVwKBgQDEJrG9dAq7igLXwYrVSSXb1gMz0o6YcJoO
pX99Hp9g+wqjINxb6s8lDB4wetYjYyK5Z7+nBIlRjcKrp40eke1q2oCvrT77Wos4
owQ/rZxUe/oQBeyer5MEs9ZeJVzJ5vPPee3bpBDYWHmMHfogMtCRA08P1vNTy3mj
kM+roES+7wKBgDIeMALaqtFKxkatBKletKi/c0GkuPx4zEQxbACwMccjvEqrVmsE
qKF27s6jh1FENjxQsvus9rSU9YtsvazsMfl6hbj7FTU+NHiKqtvlR1YERsUK5PN+
GSpPHrBuB4K8sSczQMdvGPre3U54BKa1FOBkgR+x2VWEmBLY41KtKIP7AoGAD/n+
3CmJfkD9fklbX4f3t3I1DJGH3868HZlJSw2leaa49RSGHk5/1Mqp4tT+gB9hAqz6
pUXvUV80jfq1udm09tEZTjXUPXDgihptDCq94vu+IHP7E+nFFcr4GO7+IcvX6/xI
bW1tmdGLBOikKN86sbUNSYL+isK2A6aV46rILq0CgYBUIVI2uwE/ELiXO8O8m4uK
Sk2fgpfr13ocQqkubvsRErqoRBJyat2FtQ7aWbDjINFlCY23rzTQdtMTte2r2zz2
yo5q7098bw+VzYU6LYaRnwitmVjDT4x1wNz0lOyEii8bsVZvi0eq7GdlEQOB7+T8
j8UmkktM6PXbocwralB/8Q==
-----END PRIVATE KEY-----""",
  "client_email": "er-sync-bot@earthranger-integration.iam.gserviceaccount.com",
  "client_id": "105018899028643941740",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/er-sync-bot%40earthranger-integration.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# --- 2. SPECIES REFERENCE ---
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

# --- 3. HELPERS ---
def normalize_species_name(name):
    if not isinstance(name, str) or name.lower() == 'nan' or not name.strip(): return ""
    name = re.sub(r"\s*\(unidentified\)", "", name, flags=re.IGNORECASE).lower().strip()
    return name.replace("dik dik", "dikdik").replace("zebra grevy's", "grevy's zebra")

def fetch_er_data():
    headers = {"Authorization": f"Bearer {ER_TOKEN}"}
    url = f"https://{ER_DOMAIN}/api/v1.0/activity/events/?page_size=300"
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json().get('data', {}).get('results', []) or resp.json().get('data', [])
        print(f"❌ API Error: {resp.status_code}")
    except Exception as e:
        print(f"❌ Connection Error: {e}")
    return []

def clean_and_process(data):
    rows = []
    for event in data:
        details = event.get('event_details', {})
        internal_val = event.get('event_type', '')
        mapped_type = REPORT_TYPE_MAP.get(internal_val, event.get('event_type_label', internal_val))
        
        dom_spec = details.get('patrolack_speciesdomestic') or details.get('routineack_speciesdomestic')
        wild_spec = details.get('patrolackwild_specieswild') or details.get('routineack_specieswild')
        norm_species = normalize_species_name(str(dom_spec if dom_spec else wild_spec))

        cat_obj = event.get('event_category', {})
        cat_name = cat_obj.get('value', '').lower() if isinstance(cat_obj, dict) else ""
        if not cat_name: 
            cat_name = "transect" if "transect" in internal_val.lower() else "patrol"

        loc_val = details.get('routineack_block') or details.get('transectack_block') or details.get('transects') or ""

        rows.append({
            'Report_Id': f"ER{event.get('serial_number')}",
            'Report_Type': mapped_type,
            'Reported_By':
