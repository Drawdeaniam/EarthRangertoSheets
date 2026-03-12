import os
import re
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
ER_DOMAIN = "ack.pamdas.org"
ER_TOKEN = os.getenv("ER_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")

# Tab names for Patrol and Transect data respectively
PATROL_TAB = "Sheet6"
TRANSECT_TAB = "Sheet7"

# Trophic level lookup keyed by normalized lowercase ER species values.
# Categories sourced from the "Species to be recorded" reference sheet.
TROPHIC_MAP = {
    # --- Carnivore ---
    "cheetah":              "Carnivore",
    "lion":                 "Carnivore",
    "leopard":              "Carnivore",
    "hyenaspotted":         "Carnivore",
    "hyenasstriped":        "Carnivore",
    "jackal_unidentified":  "Carnivore",
    "jackal":               "Carnivore",
    "caracal":              "Carnivore",
    "africanwilddog":       "Carnivore",
    "serval":               "Carnivore",

    # --- Preferred cheetah prey ---
    "hare":                 "Preferred cheetah prey",
    "hyrax":                "Preferred cheetah prey",
    "dikdik":               "Preferred cheetah prey",
    "dikdikunidentified":   "Preferred cheetah prey",
    "guineafowlcrested":    "Preferred cheetah prey",
    "guineafowlvulturine":  "Preferred cheetah prey",
    "yellowneckedspurfowl": "Preferred cheetah prey",
    "grant'sgazelle":       "Preferred cheetah prey",
    "gazelle_grants":       "Preferred cheetah prey",
    "grantsgazelle":        "Preferred cheetah prey",
    "gerenuk":              "Preferred cheetah prey",
    "klipspringer":         "Preferred cheetah prey",
    "clipspringer":         "Preferred cheetah prey",
    "kudulesser":           "Preferred cheetah prey",
    "impala":               "Preferred cheetah prey",
    "steenbuck":            "Preferred cheetah prey",
    "steenbok":             "Preferred cheetah prey",
    "bushbuck":             "Preferred cheetah prey",
    "thomson'sgazelle":     "Preferred cheetah prey",
    "thomsonsgazelle":      "Preferred cheetah prey",
    "duiker":               "Preferred cheetah prey",
    "springhare":           "Preferred cheetah prey",

    # --- Sometimes cheetah prey ---
    "vervetmonkey":  "Sometimes cheetah prey",
    "goat":          "Sometimes cheetah prey",
    "shoat":         "Sometimes cheetah prey",
    "sheep":         "Sometimes cheetah prey",
    "greaterkudu":   "Sometimes cheetah prey",
    "kudureater":    "Sometimes cheetah prey",
    "ostrich":       "Sometimes cheetah prey",
    "ostrichsomali": "Sometimes cheetah prey",
    "ostrichunid":   "Sometimes cheetah prey",
    "warthog":       "Sometimes cheetah prey",

    # --- Seldom or never cheetah prey ---
    "eland":        "Seldom or never cheetah prey",
    "zebragrevy":   "Seldom or never cheetah prey",
    "grevy'szebra": "Seldom or never cheetah prey",
    "zebracommon":  "Seldom or never cheetah prey",
    "baboon":       "Seldom or never cheetah prey",
    "cattle":       "Seldom or never cheetah prey",
    "camel":        "Seldom or never cheetah prey",
    "buffalo":      "Seldom or never cheetah prey",
    "hippo":        "Seldom or never cheetah prey",
    "giraffe":      "Seldom or never cheetah prey",
    "ele":          "Seldom or never cheetah prey",
    "elephant":     "Seldom or never cheetah prey",
    "bushpig":      "Seldom or never cheetah prey",
    "donkey":       "Seldom or never cheetah prey",

    # --- Unclassified ---
    "bat-earedfox": "Unclassified",
    "batearedfox":  "Unclassified",
    "africancivet": "Unclassified",
    "genet":        "Unclassified",
    "domesticdog":  "Unclassified",
    "honeybadger":  "Unclassified",
    "bustardkori":  "Unclassified",
    "vulture":      "Unclassified",
}

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

# Internal event_type -> human-readable label
REPORT_TYPE_MAP = {
    "patrol_domesticanimal":     "Patrol - Domestic Animal Sighting",
    "patrol_info_ack":           "Patrol Info",
    "patrolwildanimal_sight":    "Patrol - Wild Animal Type",
    "transect_domestic_sight":   "Transect - Domestic Animal Type",
    "transect_wildanimal_sight": "Transect - Wild Animal Type",
    "transectinfo_ack":          "Transect Info",
}


# =============================================================================
# 2. HELPERS
# =============================================================================

def get_any(details_dict, keys_to_check):
    """Return the first non-empty value found in details_dict for the given keys."""
    for k in keys_to_check:
        val = details_dict.get(k)
        if val is not None and val != "":
            return val
    return ""


def normalize_species_name(name):
    """Lowercase, strip, remove '(unidentified)', and standardize naming variants."""
    if not isinstance(name, str) or name.lower() == "nan" or not name.strip():
        return ""
    name = re.sub(r"\s*\(unidentified\)", "", name, flags=re.IGNORECASE).lower().strip()
    name = name.replace("dik dik", "dikdik")
    name = name.replace("zebra grevy's", "grevy's zebra")
    name = name.replace("gazelle grant's", "grant's gazelle")
    return name


def get_trophic(species_raw):
    """Look up trophic level from TROPHIC_MAP using a normalized species key."""
    return TROPHIC_MAP.get(normalize_species_name(str(species_raw)), "")


def reformat_transect(name):
    """Convert 'A - Some Transect' -> 'Some Transect A'."""
    if isinstance(name, str):
        match = re.match(r"([A-Z])\s*-\s*(.*)", name)
        if match:
            return f"{match.group(2).strip()} {match.group(1)}"
    return name


def format_photo_urls(files_list):
    """Build a Google Sheets HYPERLINK formula for all attachment URLs in a cell."""
    urls = []
    for f in files_list:
        url = (
            f.get("url")
            or f.get("images", {}).get("original", "")
            or f.get("file_url", "")
        )
        if url:
            urls.append(url)

    if not urls:
        return ""

    parts = [f'HYPERLINK("{url}", "Photo {i + 1}")' for i, url in enumerate(urls)]
    if len(parts) == 1:
        return f"={parts[0]}"
    return "=" + '&" | "&'.join(parts)


# =============================================================================
# 3. PULL
# =============================================================================

def fetch_er_data():
    """Fetch all events from EarthRanger, following pagination links."""
    headers = {"Authorization": f"Bearer {ER_TOKEN}"}
    url = f"https://{ER_DOMAIN}/api/v1.0/activity/events/?page_size=300"
    all_results = []
    while url:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f"API Error {resp.status_code}: {resp.text}")
                break
            payload = resp.json()
            data_block = payload.get("data", {})
            results = data_block.get("results", []) if isinstance(data_block, dict) else data_block
            all_results.extend(results or [])
            url = data_block.get("next") if isinstance(data_block, dict) else None
        except Exception as e:
            print(f"Connection Error: {e}")
            break
    print(f"Pulled {len(all_results)} events from EarthRanger.")
    return all_results


# =============================================================================
# 4. BUILD RAW DATAFRAME
# =============================================================================

def build_raw_dataframe(data):
    """
    Parse the raw EarthRanger event list into a flat DataFrame that matches
    the EarthRanger_Master_Cleaned.csv column schema.
    """
    rows = []
    for event in data:
        details      = event.get("event_details", {}) or {}
        location     = event.get("location") or {}
        reported_by  = event.get("reported_by") or {}
        notes_list   = event.get("notes", [])
        related_subj = event.get("related_subjects", [])
        files_list   = event.get("files", [])

        internal_val = event.get("event_type", "")
        display_name = REPORT_TYPE_MAP.get(internal_val, event.get("event_type_label", internal_val))

        # Determine patrol vs transect category
        category_obj  = event.get("event_category", {})
        category_name = category_obj.get("value", "").lower() if isinstance(category_obj, dict) else ""
        if not category_name:
            category_name = "transect" if "transect" in internal_val.lower() else "patrol"

        # Assign location value to the correct column
        loc_value = get_any(details, ["routineack_block", "transectack_block", "transects"])
        if "transect" in category_name:
            block_val, transect_val = "", loc_value
        else:
            block_val, transect_val = loc_value, ""

        # Scout name with fallback chain
        participant = (
            reported_by.get("name")
            or f"{reported_by.get('first_name', '')} {reported_by.get('last_name', '')}".strip()
            or reported_by.get("username", "Unknown")
        )

        rows.append({
            "Report_Type":                         display_name,
            "Report_Type_Internal_Value":          internal_val,
            "Report_Id":                           event.get("serial_number"),
            "Title":                               event.get("title"),
            "Priority":                            event.get("priority_label"),
            "Priority_Internal_Value":             event.get("priority"),
            "Report_Status":                       event.get("state"),
            "Reported_By":                         participant,
            "Reported_At_(GMT+0:0)": (
                pd.to_datetime(event.get("time")).strftime("%Y-%m-%d %H:%M")
                if event.get("time") else "N/A"
            ),
            "Latitude":                            location.get("latitude"),
            "Longitude":                           location.get("longitude"),
            "Number_of_Notes":                     len(notes_list),
            "Notes":                               " | ".join(str(n.get("text", "")) for n in notes_list),
            "Number_of_Related_Subjects":          len(related_subj),
            "Collection_Report_IDs":               ", ".join(event.get("contains", [])),
            "Area":                                details.get("area", ""),
            "Perimeter":                           details.get("perimeter", ""),
            "Attachments":                         format_photo_urls(files_list),
            "CUSTOM_FIELDS_BEGIN_HERE":            "---",
            "Blocks":                              block_val,
            "Rain_Night_Before":                   get_any(details, ["routineack_rain", "walktransect_rain"]),
            "Last_Rain":                           get_any(details, ["routineack_lastrain", "walktransect_lastrain"]),
            "Wild_Animal_Species":                 get_any(details, ["patrolackwild_specieswild", "routineack_specieswild"]),
            "Number":                              get_any(details, ["patrolack_nb", "patrolackwild_nb", "routineack_nb"]),
            "Habitat":                             get_any(details, ["patrolack_habitat", "patrolackwild_habitat", "routineack_habitat"]),
            "Type":                                get_any(details, ["patrolack_type", "patrolackwild_type", "routineack_type"]),
            "Ground_Cover":                        get_any(details, ["patrolack_groundcover", "patrolackwild_groundcover", "routineack_groundcover"]),
            "Sighting_Distance_(m)":               get_any(details, ["patrolack_sightingdistance", "patrolackwild_sightingdistance", "routineack_sightingdistance"]),
            "Sighting_angle_(degrees_from_North)": get_any(details, ["patrolack_sightingangle", "patrolackwild_sightingangle", "routineack_sightingangle"]),
            "Spoor_Height_(cm)":                   get_any(details, ["patrolackwild_spoorheight", "routineack_spoorheight"]),
            "Spoor_Width_(cm)":                    get_any(details, ["patrolackwild_spoorwidth", "routineack_spoorwidth"]),
            "Track_Age":                           get_any(details, ["patrolackwild_trackage", "routineack_trackage"]),
            "Activity":                            get_any(details, ["patrolack_activity", "patrolackwild_activity", "routineack_activity"]),
            "Domestic_Animal_Species":             get_any(details, ["patrolack_speciesdomestic", "routineack_speciesdomestic"]),
            "Transects":                           transect_val,
        })

    return pd.DataFrame(rows)


# =============================================================================
# 5. CLEAN
# =============================================================================

def _empty_series(index):
    return pd.Series([""] * len(index), index=index)


def clean_dataframe(df):
    """
    Apply all cleaning transforms and return two DataFrames:
      (patrol_df, transect_df)
    Each matches the final column layout expected in Google Sheets.
    """

    # --- Timezone: GMT+0 -> EAT (GMT+3) ---
    df["Reported_At"] = (
        pd.to_datetime(df["Reported_At_(GMT+0:0)"], errors="coerce")
        + pd.Timedelta(hours=3)
    )
    df["Day"]   = df["Reported_At"].dt.day
    df["Month"] = df["Reported_At"].dt.month
    df["Year"]  = df["Reported_At"].dt.year
    df["Date"]  = df["Reported_At"].dt.strftime("%d/%m/%Y")
    df["Time"]  = df["Reported_At"].dt.strftime("%I:%M %p")

    # --- Filter: only keep records from 10 March 2026 onwards ---
    cutoff_date = pd.Timestamp("2026-03-10")
    df = df[df["Reported_At"] >= cutoff_date].copy()

    # --- Sort: by day -> officer name (A-Z) -> start time (earliest first) ---
    # _sort_date floors Reported_At to midnight so the primary sort is day-level,
    # then Reported_By alphabetically within each day, then full timestamp for time order.
    df["_sort_date"] = df["Reported_At"].dt.normalize()
    df = df.sort_values(
        by=["_sort_date", "Reported_By", "Reported_At"],
        ascending=[True, True, True]
    ).reset_index(drop=True)
    df.drop(columns=["_sort_date"], inplace=True)

    # --- Rename Last_Rain -> Rain ---
    if "Rain" in df.columns:
        df.drop(columns=["Rain"], inplace=True)
    if "Last_Rain" in df.columns:
        df.rename(columns={"Last_Rain": "Rain"}, inplace=True)

    # --- Remove ' ACK' suffix from scout names ---
    df["Reported_By"] = df["Reported_By"].str.replace(r"\s*ACK$", "", regex=True)

    # --- Reformat transect names: 'A - Name' -> 'Name A' ---
    if "Transects" in df.columns:
        df["Transects"] = df["Transects"].apply(reformat_transect)
    else:
        df["Transects"] = None

    # --- Clean Ground Cover ---
    if "Ground_Cover" in df.columns:
        df["Ground_Cover"] = df["Ground_Cover"].str.replace(
            r"^\((SG|BG|MHG)\)\s*", "", regex=True
        )

    # --- Clean Habitat ---
    if "Habitat" in df.columns:
        df["Habitat"] = (
            df["Habitat"]
            .astype(str)
            .str.replace(r"^\((.*?)\)$", r"\1", regex=True)
            .str.strip()
        )
        df["Habitat"] = df["Habitat"].str.replace(
            r"^(OWL|CWL|BWL|[(]OWL[)]|[(]CWL[)]|[(]BWL[)])\s*", "", regex=True
        )
        df["Habitat"] = df["Habitat"].replace("nan", "").fillna("")

    # --- Derive Start / End times per (Report_Id, Reported_By) group ---
    def process_group(group):
        valid_times = group.loc[group["Reported_At"].notna(), "Reported_At"]
        group["Is_First_Row"]      = False
        group["Is_Last_Row"]       = False
        group["Report_Time_Value"] = group["Time"]
        if not valid_times.empty:
            first_idx = valid_times.idxmin()
            last_idx  = valid_times.idxmax()
            group.loc[first_idx, "Is_First_Row"] = True
            if last_idx != first_idx:
                group.loc[last_idx, "Is_Last_Row"] = True
        return group

    df = df.groupby(["Report_Id", "Reported_By"], group_keys=False).apply(process_group)
    df["Final_StartTime"] = df.apply(lambda r: r["Report_Time_Value"] if r["Is_First_Row"] else "", axis=1)
    df["Final_EndTime"]   = df.apply(lambda r: r["Report_Time_Value"] if r["Is_Last_Row"]  else "", axis=1)

    # --- Propagate Transect name from 'Transect Info' rows to all sibling rows ---
    if "Transects" in df.columns:
        transect_info = df[
            df["Report_Type"].astype(str).str.lower() == "transect info"
        ][["Report_Id", "Reported_By", "Transects"]].copy()
        df = df.merge(transect_info, on=["Report_Id", "Reported_By"], how="left", suffixes=("", "_Extracted"))
        df["Transect_Final"] = df["Transects_Extracted"].combine_first(df["Transects"])
    else:
        df["Transect_Final"] = None

    # --- Merge domestic + wild species; compute Trophic ---
    # Replace empty strings with NaN first so combine_first correctly falls
    # through to Wild_Animal_Species on rows where no domestic species was recorded.
    if "Domestic_Animal_Species" in df.columns and "Wild_Animal_Species" in df.columns:
        domestic = df["Domestic_Animal_Species"].replace("", pd.NA)
        wild     = df["Wild_Animal_Species"].replace("", pd.NA)
        species_col = domestic.combine_first(wild)
    elif "Domestic_Animal_Species" in df.columns:
        species_col = df["Domestic_Animal_Species"].replace("", pd.NA)
    elif "Wild_Animal_Species" in df.columns:
        species_col = df["Wild_Animal_Species"].replace("", pd.NA)
    else:
        species_col = _empty_series(df.index)

    species_col = (
        species_col.astype(str)
        .str.replace(r"\s*\(unidentified\)", "", regex=True)
        .str.strip()
        .replace("nan", "")
    )
    trophic_col = species_col.apply(get_trophic)

    # Helper to safely pull a column or return empty series
    def col(name):
        return df[name] if name in df.columns else _empty_series(df.index)

    # --- Split into Patrol and Transect ---
    transect_mask = df["Report_Type"].astype(str).str.contains("transect", na=False, case=False)
    patrol_mask   = df["Report_Type"].astype(str).str.contains("patrol",   na=False, case=False)

    def build_output(mask, include_blocks=False, include_transect=False):
        out = pd.DataFrame({
            "Form Number":   "ER" + df.loc[mask, "Report_Id"].astype(str),
            "Scout Name":    df.loc[mask, "Reported_By"],
            "Day":           df.loc[mask, "Day"],
            "Month":         df.loc[mask, "Month"],
            "Year":          df.loc[mask, "Year"],
            "Date":          df.loc[mask, "Date"],
            "StartTime":     df.loc[mask, "Final_StartTime"],
            "Active Time":   df.loc[mask, "Time"],
            "EndTime":       df.loc[mask, "Final_EndTime"],
            "UTM East(X)":   "",
            "UTM North(Y)":  "",
            "Deg_Latitude":  col("Latitude").loc[mask],
            "Deg_Longitude": col("Longitude").loc[mask],
            "Species":       species_col.loc[mask],
            "Trophic":       trophic_col.loc[mask],
            "Number":        col("Number").loc[mask],
            "Distance":      col("Sighting_Distance_(m)").loc[mask],
            "Angle":         col("Sighting_angle_(degrees_from_North)").loc[mask],
            "Type":          col("Type").loc[mask],
            "Track height":  col("Spoor_Height_(cm)").loc[mask],
            "Track length":  col("Spoor_Width_(cm)").loc[mask],
            "Rain":          col("Rain").loc[mask],
            "Ground Cover":  col("Ground_Cover").loc[mask],
            "Habitat":       col("Habitat").loc[mask],
            "Photos":        col("Attachments").loc[mask],
            "Activity":      col("Activity").loc[mask],
        })
        if include_blocks:
            out.insert(6, "Blocks", col("Blocks").loc[mask])
        if include_transect:
            out.insert(6, "Transect", df.loc[mask, "Transect_Final"])
        return out

    patrol_df   = build_output(patrol_mask,   include_blocks=True)
    transect_df = build_output(transect_mask, include_transect=True)

    return patrol_df, transect_df


# =============================================================================
# 6. UPLOAD TO GOOGLE SHEETS
# =============================================================================

def upload_to_sheet(spreadsheet, tab_name, dataframe):
    """Clear a worksheet and write the full dataframe (header + data rows)."""
    if dataframe.empty:
        print(f"No data for tab '{tab_name}'. Skipping.")
        return

    upload_df      = dataframe.fillna("").astype(str)
    data_to_upload = [upload_df.columns.tolist()] + upload_df.values.tolist()

    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Tab '{tab_name}' not found -- creating it...")
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=50)

    worksheet.clear()
    worksheet.update(data_to_upload, value_input_option="USER_ENTERED")
    print(f"'{tab_name}' updated -- {len(dataframe)} rows, {len(dataframe.columns)} columns.")


def push_to_google_sheets(patrol_df, transect_df):
    """Authenticate with the service account and push both DataFrames."""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    try:
        creds        = Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=scope)
        client       = gspread.authorize(creds)
        spreadsheet  = client.open_by_key(SHEET_ID)

        upload_to_sheet(spreadsheet, PATROL_TAB, patrol_df)
        upload_to_sheet(spreadsheet, TRANSECT_TAB, transect_df)

    except Exception as e:
        print(f"Google Sheets Error: {e}")


# =============================================================================
# 7. ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("Starting EarthRanger -> Google Sheets sync...")

    raw_data = fetch_er_data()
    if not raw_data:
        print("No data fetched. Aborting.")
    else:
        raw_df = build_raw_dataframe(raw_data)
        print(f"Raw DataFrame: {len(raw_df)} rows x {len(raw_df.columns)} columns.")

        patrol_df, transect_df = clean_dataframe(raw_df)
        print(f"Cleaned -- Patrol: {len(patrol_df)} rows | Transect: {len(transect_df)} rows.")

        push_to_google_sheets(patrol_df, transect_df)
        print("Sync complete.")

