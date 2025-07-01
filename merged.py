import requests
from datetime import datetime, timedelta, timezone, time
import os
from dotenv import load_dotenv
import pytz
from typing import Dict, List, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import re
import mysql.connector
from mysql.connector import Error
import logging

load_dotenv()

# --- CONFIGURATION ---
# Calendly
CALENDLY_PAT = os.getenv("CALENDLY_PAT")
CALENDLY_HEADERS = {
    "Authorization": f"Bearer {CALENDLY_PAT}",
    "Content-Type": "application/json",
}

# Zoom
ZOOM_ACCOUNT_ID = os.getenv("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID = os.getenv("ZOOM_CLIENT_ID")
ZOOM_CLIENT_SECRET = os.getenv("ZOOM_CLIENT_SECRET")

# Google Sheets
SERVICE_ACCOUNT_FILE = "service-acc.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
MASTER_SHEET_ID = os.getenv("MASTER_SHEET_ID")

# Slack
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_USERS = {
    "vinamr": "U08U7SSR17U",
    # "nick": "U08UV8RB1K2"
}

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'your_database_name'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# User mappings for Calendly/Zoom
USER_MAPPINGS = {
    "sierra": {
        "calendly_uuid": os.getenv("SIERRA_UUID"),
        "zoom_email": "sierrac@ccdocs.com",
    },
    "mikaela": {
        "calendly_uuid": os.getenv("MIKAELA_UUID"),
        "zoom_email": "mikaela@ccdocs.com",
    },
    "mike": {
        "calendly_uuid": os.getenv("MIKE_UUID"),
        "zoom_email": "hammer@ccdocs.com",
    },
}

# EST timezone
EST = pytz.timezone("America/New_York")

# Initialize Google Sheets client
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("sheets", "v4", credentials=credentials)
sheet = service.spreadsheets()

# Global storage for all metrics
daily_metrics = {}

# --- DATABASE FUNCTIONS ---
def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_daily_metrics_table():
    """Create the daily_metrics table if it doesn't exist"""
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            metric_date DATE NOT NULL,
            representative ENUM('sierra','mikaela','mike') NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value DECIMAL(15,2) NOT NULL DEFAULT 0.00,
            metric_type ENUM('count','percentage','currency') NOT NULL DEFAULT 'count',
            source VARCHAR(50) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_metric (metric_date, representative, metric_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Daily metrics table created/verified")
        return True
        
    except Error as e:
        print(f"❌ Error creating daily metrics table: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def insert_metric(metric_date, representative, metric_name, metric_value, metric_type='count', source=None):
    """Insert or update a single metric"""
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        query = """
        INSERT INTO daily_metrics (metric_date, representative, metric_name, metric_value, metric_type, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            metric_value = VALUES(metric_value),
            metric_type = VALUES(metric_type),
            source = VALUES(source),
            updated_at = CURRENT_TIMESTAMP
        """
        
        values = (metric_date, representative, metric_name, metric_value, metric_type, source)
        cursor.execute(query, values)
        connection.commit()
        
        return True
        
    except Error as e:
        print(f"❌ Error inserting metric {metric_name} for {representative}: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def save_all_metrics_to_db():
    """Save all collected metrics to database"""
    yesterday = (datetime.now(EST) - timedelta(days=1)).date()
    
    print(f"\n💾 Saving all metrics to database for {yesterday}...")
    
    total_saved = 0
    for rep_name in ['sierra', 'mikaela', 'mike']:
        rep_metrics = daily_metrics.get(rep_name, {})
        
        for metric_name, metric_data in rep_metrics.items():
            success = insert_metric(
                yesterday,
                rep_name,
                metric_name,
                metric_data['value'],
                metric_data['type'],
                metric_data['source']
            )
            if success:
                total_saved += 1
                print(f"  ✅ {rep_name}: {metric_name} = {metric_data['value']} ({metric_data['type']})")
            else:
                print(f"  ❌ {rep_name}: Failed to save {metric_name}")
    
    print(f"\n✅ Total metrics saved: {total_saved}")

def store_metric(representative, metric_name, value, metric_type='count', source=None):
    """Store a metric in the global metrics dictionary"""
    if representative not in daily_metrics:
        daily_metrics[representative] = {}
    
    daily_metrics[representative][metric_name] = {
        'value': float(value),
        'type': metric_type,
        'source': source
    }

# --- SLACK FUNCTIONS ---
def send_slack_message(user_id, message):
    """Send a message to a specific Slack user"""
    if not SLACK_BOT_TOKEN:
        print("Warning: SLACK_BOT_TOKEN not found. Skipping Slack message.")
        return False

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "channel": user_id,
        "text": message,
        "mrkdwn": True,  # Enable Slack markdown formatting
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()

        if result.get("ok"):
            return True
        else:
            print(f"Slack API error: {result.get('error', 'Unknown error')}")
            return False

    except Exception as e:
        print(f"Error sending Slack message: {e}")
        return False


def broadcast_to_slack_users(message):
    """Send message to all configured Slack users"""
    print(f"\n📤 Sending to Slack users...")

    for username, user_id in SLACK_USERS.items():
        success = send_slack_message(user_id, message)
        if success:
            print(f"✅ Message sent to {username}")
        else:
            print(f"❌ Failed to send message to {username}")


# --- UTILITY FUNCTIONS ---
def is_empty_or_null(value):
    """Check if a value is empty, null, or whitespace only"""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def has_value(value):
    """Check if a value has content (opposite of is_empty_or_null)"""
    return not is_empty_or_null(value)


def parse_currency_value(value):
    """Parse currency value and return float"""
    if is_empty_or_null(value):
        return 0.0

    # Convert to string and clean up
    str_value = str(value).strip()

    # Remove currency symbols and commas
    cleaned_value = (
        str_value.replace("₹", "").replace("$", "").replace(",", "").strip()
    )

    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return 0.0


def parse_numeric_value(value):
    """Parse numeric value and return float"""
    if is_empty_or_null(value):
        return 0.0

    # Convert to string and clean up
    str_value = str(value).strip()

    try:
        return float(str_value)
    except (ValueError, TypeError):
        return 0.0


def parse_percentage_value(value):
    """Parse percentage value and return float (without % sign)"""
    if is_empty_or_null(value):
        return 0.0

    # Convert to string and clean up
    str_value = str(value).strip()
    
    # Remove % sign if present
    cleaned_value = str_value.replace("%", "").strip()

    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        return 0.0


def normalize_column_name(col_name):
    """Normalize column name by removing extra spaces, special characters, and converting to lowercase"""
    if not col_name:
        return ""

    # Convert to string and strip
    normalized = str(col_name).strip()

    # Remove extra spaces and special characters, keep only alphanumeric and basic punctuation
    normalized = re.sub(
        r"\s+", " ", normalized
    )  # Replace multiple spaces with single space
    normalized = normalized.lower()

    return normalized


def find_matching_column(target_column, available_columns):
    """Find the best matching column name from available columns"""
    target_normalized = normalize_column_name(target_column)

    # First try exact match after normalization
    for col in available_columns:
        if normalize_column_name(col) == target_normalized:
            return col

    # Then try partial matches
    for col in available_columns:
        col_normalized = normalize_column_name(col)
        if (
            target_normalized in col_normalized
            or col_normalized in target_normalized
        ):
            return col

    # Try without spaces
    target_no_spaces = target_normalized.replace(" ", "")
    for col in available_columns:
        col_no_spaces = normalize_column_name(col).replace(" ", "")
        if target_no_spaces == col_no_spaces:
            return col

    return None


def map_required_columns(df, required_columns):
    """Map required columns to actual column names in the DataFrame"""
    column_mapping = {}
    available_columns = list(df.columns)

    print(f"\nColumn Mapping:")
    print(f"Available columns: {available_columns}")

    for req_col in required_columns:
        matched_col = find_matching_column(req_col, available_columns)
        if matched_col:
            column_mapping[req_col] = matched_col
            print(f"✅ '{req_col}' -> '{matched_col}'")
        else:
            print(f"❌ '{req_col}' -> No match found")

    return column_mapping


# --- ZOOM FUNCTIONS ---
def get_zoom_access_token():
    """Get OAuth access token for Zoom API"""
    token_url = "https://zoom.us/oauth/token"
    data = {
        "grant_type": "account_credentials",
        "account_id": ZOOM_ACCOUNT_ID,
        "client_id": ZOOM_CLIENT_ID,
        "client_secret": ZOOM_CLIENT_SECRET,
    }

    response = requests.post(token_url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


def get_zoom_user_id_by_email(email, access_token):
    """Get Zoom user ID by email address"""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.zoom.us/v2/users/{email}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("id")
    except Exception as e:
        print(f"Error fetching Zoom user by email: {e}")
        return None


def get_zoom_meetings_for_user_today(user_id, email, access_token):
    """Get Zoom RECORDINGS for a user for yesterday in EST."""
    # Get yesterday's date in EST
    now_est = datetime.now(EST)
    yesterday_est = (now_est - timedelta(days=1)).date()

    # Convert to UTC for API call (Zoom API expects UTC dates)
    start_date_est = EST.localize(datetime.combine(yesterday_est, time(0, 0, 0)))
    end_date_est = EST.localize(datetime.combine(yesterday_est, time(23, 59, 59)))

    start_date_utc = start_date_est.astimezone(pytz.UTC)
    end_date_utc = end_date_est.astimezone(pytz.UTC)

    print(f"  Fetching Zoom recordings for {email} on {yesterday_est} (EST)")
    print(
        f"  UTC range: {start_date_utc.strftime('%Y-%m-%d')} to {end_date_utc.strftime('%Y-%m-%d')}"
    )

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.zoom.us/v2/users/{user_id}/recordings"
    params = {
        "from": start_date_utc.strftime("%Y-%m-%d"),
        "to": end_date_utc.strftime("%Y-%m-%d"),
    }

    meetings = []

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        print(f"  Found {len(data.get('meetings', []))} recorded meetings")

        for meeting in data.get("meetings", []):
            start_time_str = meeting.get("start_time")
            if start_time_str:
                try:
                    # Zoom returns UTC time
                    if start_time_str.endswith("Z"):
                        start_time_utc = datetime.fromisoformat(
                            start_time_str.replace("Z", "+00:00")
                        )
                    else:
                        start_time_utc = datetime.fromisoformat(start_time_str)
                        if start_time_utc.tzinfo is None:
                            start_time_utc = pytz.UTC.localize(start_time_utc)

                    start_time_est = start_time_utc.astimezone(EST)

                    # Check if meeting is yesterday in EST
                    if start_time_est.date() == yesterday_est:
                        meeting_info = {
                            "id": meeting.get("uuid", meeting.get("id")),
                            "topic": meeting.get("topic"),
                            "start_time": start_time_est,
                            "start_time_str": start_time_est.strftime(
                                "%I:%M %p EST"
                            ),
                            "has_recordings": len(
                                meeting.get("recording_files", [])
                            )
                            > 0,
                        }
                        meetings.append(meeting_info)
                        print(
                            f"    Added recording: {meeting.get('topic')} at {start_time_est.strftime('%I:%M %p EST')}"
                        )

                except Exception as e:
                    print(
                        f"    Error parsing recording time {start_time_str}: {e}"
                    )

    except Exception as e:
        print(f"  Error fetching Zoom recordings: {e}")

    print(f"  Total Zoom recordings found: {len(meetings)}")
    return meetings


# --- CALENDLY FUNCTIONS ---
def format_event_time(iso_time_str):
    """Format ISO time string to readable format in EST timezone."""
    dt = datetime.fromisoformat(iso_time_str.replace("Z", "+00:00"))
    dt_est = dt.astimezone(EST)
    return dt_est.strftime("%I:%M %p EST")


def get_calendly_events_for_user(user_uri, org_uri):
    """Get yesterday's Calendly events for a specific user."""
    # Get yesterday's date in EST
    now_est = datetime.now(EST)
    yesterday_est = (now_est - timedelta(days=1)).date()

    # Convert to UTC for API call
    start_time_est = EST.localize(datetime.combine(yesterday_est, time(0, 0, 0)))
    end_time_est = EST.localize(datetime.combine(yesterday_est, time(23, 59, 59)))

    start_time_utc = start_time_est.astimezone(pytz.UTC)
    end_time_utc = end_time_est.astimezone(pytz.UTC)

    url = "https://api.calendly.com/scheduled_events"
    params = {
        "user": user_uri,
        "organization": org_uri,
        "sort": "start_time:asc",
        "min_start_time": start_time_utc.isoformat(),
        "max_start_time": end_time_utc.isoformat(),
    }

    events = []

    while url:
        response = requests.get(url, headers=CALENDLY_HEADERS, params=params)
        response.raise_for_status()
        data = response.json()

        # Get all events (both active and canceled)
        all_events = data.get("collection", [])
        events.extend(all_events)

        url = data.get("pagination", {}).get("next_page")
        params = {}

    # Process events to extract relevant information
    active_events = []
    canceled_events = []
    
    for event in events:
        # Parse start time and convert to EST
        start_time_str = event.get("start_time")
        if start_time_str:
            dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            dt_est = dt.astimezone(EST)

            event_info = {
                "name": event.get("name", "Unnamed Event"),
                "start_time": dt_est,
                "start_time_str": format_event_time(event.get("start_time")),
                "end_time_str": format_event_time(event.get("end_time")),
                "status": event.get("status"),
                "uri": event.get("uri", ""),
            }
            
            if event.get("status") == "active":
                active_events.append(event_info)
            elif event.get("status") == "canceled":
                canceled_events.append(event_info)

    return active_events, canceled_events


# --- MATCHING FUNCTIONS ---
def match_events_with_meetings(
    calendly_events: List[Dict], zoom_meetings: List[Dict]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Match Calendly events with Zoom RECORDINGS based on time proximity.
    Returns (matched_events, unmatched_events)
    """
    matched_events = []
    unmatched_events = []

    print(
        f"\n  Matching {len(calendly_events)} Calendly events with {len(zoom_meetings)} Zoom recordings..."
    )

    # Create a copy of zoom meetings to track which ones are matched
    unmatched_zoom = zoom_meetings.copy()

    for event in calendly_events:
        event_start = event["start_time"]
        matched = False

        print(
            f"    Calendly event: {event['name']} at {event_start.strftime('%I:%M %p EST')}"
        )

        # Look for a Zoom recording within 30 minutes of the Calendly event start time
        for zoom_meeting in zoom_meetings:
            zoom_start = zoom_meeting["start_time"]

            # Calculate time difference in minutes
            time_diff = abs((event_start - zoom_start).total_seconds() / 60)

            print(
                f"      Checking Zoom recording: {zoom_meeting['topic']} at {zoom_start.strftime('%I:%M %p EST')} (diff: {time_diff:.1f} min)"
            )

            if time_diff <= 30:  # Within 30 minutes
                matched_event = {
                    **event,
                    "zoom_meeting": zoom_meeting,
                    "attended": True,
                    "time_difference_minutes": round(time_diff, 1),
                }
                matched_events.append(matched_event)
                matched = True

                # Remove from unmatched zoom meetings
                if zoom_meeting in unmatched_zoom:
                    unmatched_zoom.remove(zoom_meeting)

                print(
                    f"        ✅ MATCHED! Time difference: {time_diff:.1f} minutes"
                )
                break

        if not matched:
            unmatched_event = {**event, "attended": False}
            unmatched_events.append(unmatched_event)
            print(f"        ❌ No recording found")

    print(
        f"  Matching complete: {len(matched_events)} matched, {len(unmatched_events)} unmatched"
    )
    return matched_events, unmatched_events


# --- GOOGLE SHEETS FUNCTIONS ---
def parse_date_from_sheet_name(sheet_name):
    """Parse date from sheet name like 'June 27 - July 13' and return the end date"""
    try:
        # Pattern to match "Month Day - Month Day" format
        pattern = r"([A-Za-z]+)\s+(\d+)\s*-\s*([A-Za-z]+)\s+(\d+)"
        match = re.match(pattern, sheet_name.strip())

        if match:
            start_month, start_day, end_month, end_day = match.groups()

            # Get current year (assuming sheets are from current year)
            current_year = datetime.now().year

            # Parse the end date (assuming it's the more recent date)
            try:
                end_date = datetime.strptime(
                    f"{end_month} {end_day} {current_year}", "%B %d %Y"
                )
                return end_date
            except ValueError:
                # Try with abbreviated month names
                end_date = datetime.strptime(
                    f"{end_month} {end_day} {current_year}", "%b %d %Y"
                )
                return end_date

        # If no match, try other common date formats
        # Try "Month Day" format
        pattern2 = r"([A-Za-z]+)\s+(\d+)"
        match2 = re.search(pattern2, sheet_name.strip())
        if match2:
            month, day = match2.groups()
            current_year = datetime.now().year
            try:
                date = datetime.strptime(
                    f"{month} {day} {current_year}", "%B %d %Y"
                )
                return date
            except ValueError:
                date = datetime.strptime(
                    f"{month} {day} {current_year}", "%b %d %Y"
                )
                return date

    except Exception as e:
        print(f"Could not parse date from sheet name '{sheet_name}': {e}")
        return None

    return None


def get_latest_sheet(sheets_info):
    """Get the latest sheet based on date parsing from sheet names"""
    sheet_dates = []

    print("Available sheets:")
    for sheet_info in sheets_info:
        sheet_name = sheet_info["properties"]["title"]
        parsed_date = parse_date_from_sheet_name(sheet_name)
        sheet_dates.append((sheet_name, parsed_date, sheet_info))

        if parsed_date:
            print(
                f"  - {sheet_name} (parsed as: {parsed_date.strftime('%Y-%m-%d')})"
            )
        else:
            print(f"  - {sheet_name} (could not parse date)")

    # Filter out sheets where date parsing failed and sort by date
    valid_sheets = [
        (name, date, info)
        for name, date, info in sheet_dates
        if date is not None
    ]

    if not valid_sheets:
        print("No sheets with parseable dates found. Using the first sheet.")
        return sheets_info[0] if sheets_info else None

    # Sort by date (most recent first)
    valid_sheets.sort(key=lambda x: x[1], reverse=True)
    latest_sheet_name, latest_date, latest_sheet_info = valid_sheets[0]

    print(
        f"\nSelected latest sheet: '{latest_sheet_name}' (date: {latest_date.strftime('%Y-%m-%d')})"
    )
    return latest_sheet_info


def get_all_unique_users(df, demo_by_col):
    """Get all unique users from Demo By column, excluding JASON"""
    all_users = df[demo_by_col].dropna().unique()
    # Filter out JASON (case-insensitive) and empty strings
    filtered_users = [
        user
        for user in all_users
        if user.strip() and user.upper().strip() != "JASON"
    ]
    return sorted(filtered_users)


def get_master_sheet_data():
    """Get data from the master sheet for running close rate calculation"""
    if not MASTER_SHEET_ID:
        print(
            "Warning: MASTER_SHEET_ID not found. Skipping running close rate calculation."
        )
        return {}

    try:
        print(f"\nFetching data from master sheet: {MASTER_SHEET_ID}")

        # Fetch data from the master sheet (columns A, C, D)
        master_sheet_range = "A:D"  # Get columns A through D
        result = (
            sheet.values()
            .get(spreadsheetId=MASTER_SHEET_ID, range=master_sheet_range)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            print("No data found in master sheet.")
            return {}

        print(f"Found {len(values)} rows in master sheet")

        # Create a mapping of sales rep names to their data
        master_data = {}

        # Process each row (skip header if exists)
        for i, row in enumerate(values):
            if len(row) >= 4:  # Ensure we have at least 4 columns
                name = row[0].strip() if row[0] else ""  # Column A - name
                col_c_value = (
                    parse_numeric_value(row[2]) if len(row) > 2 else 0.0
                )  # Column C
                col_d_value = (
                    parse_numeric_value(row[3]) if len(row) > 3 else 0.0
                )  # Column D

                if (
                    name and name.upper() != "JASON"
                ):  # Exclude JASON and empty names
                    # Normalize name to match with our user mappings
                    normalized_name = name.lower().strip()

                    master_data[normalized_name] = {
                        "original_name": name,
                        "col_c": col_c_value,
                        "col_d": col_d_value,
                        "row_index": i + 1,  # 1-based row index
                    }

                    print(
                        f"  {name}: Column C = {col_c_value}, Column D = {col_d_value}"
                    )

        # Get the special value from row 5, column D
        row_5_col_d = 0.0
        if (
            len(values) >= 5 and len(values[4]) >= 4
        ):  # Row 5 (index 4), Column D (index 3)
            row_5_col_d = parse_numeric_value(values[4][3])
            print(f"  Row 5, Column D: {row_5_col_d}")

        master_data["_row_5_col_d"] = row_5_col_d

        return master_data

    except Exception as e:
        print(f"Error fetching master sheet data: {e}")
        return {}


def calculate_running_close_rate():
    """
    Re-compute each rep's Running Close Rate (Sit→Sale) without ever
    exceeding 100 %.
    - Relies on globals created elsewhere in the script:
        • user_appointments_conducted  – today's *conducted* sits per rep
        • new_clients_counts           – today's *non-organic* closes per rep
    """
    master_data = get_master_sheet_data()
    if not master_data:
        print("Skipping close-rate calc – no master data.")
        return {}

    # pull the two today-dicts from global scope
    today_sits = globals().get("user_appointments_conducted", {})
    today_closes = globals().get("new_clients_counts", {})

    name_map = {
        "sierra": ["sierra", "sierrac"],
        "mikaela": ["mikaela"],
        "mike": ["mike", "hammer"],
    }

    close_rates = {}

    for rep_key, aliases in name_map.items():
        # locate this rep's row in master_data (columns C & D)
        rep_row = None
        for alias in aliases:
            rep_row = next(
                (
                    d
                    for k, d in master_data.items()
                    if k != "_row_5_col_d" and alias in k
                ),
                None,
            )
            if rep_row:
                break

        if not rep_row:
            close_rates[rep_key] = 0.0
            continue

        cum_sits = rep_row["col_c"]  # historical conducted
        cum_closes = rep_row["col_d"]  # historical closes

        total_sits = cum_sits + today_sits.get(rep_key, 0)
        total_closes = cum_closes + today_closes.get(rep_key, 0)

        rate = (total_closes / total_sits * 100) if total_sits else 0.0
        close_rates[rep_key] = rate

        print(f"{rep_key.title()}: {total_closes}/{total_sits} → {rate:.1f}%")

        # Store in metrics
        store_metric(rep_key, "calculated_running_close_rate", rate, "percentage", "Master Sheet + Appointments")

    return close_rates


def create_slack_message(
    demo_by_data, all_users, sheet_name, metric_title, is_revenue=False
):
    """Create Slack message for a metric"""
    if is_revenue:
        complete_data = {
            user: demo_by_data.get(user, 0.0) for user in all_users
        }
    else:
        complete_data = {user: demo_by_data.get(user, 0) for user in all_users}

    if not complete_data:
        return f"✅ *{metric_title}*\nPeriod: {sheet_name}\n\nNo data found for this metric."

    # Sort by value (highest first), then by name
    sorted_data = sorted(complete_data.items(), key=lambda x: (-x[1], x[0]))

    total_value = sum(complete_data.values())

    message = f"""✅ *{metric_title}*
Period: {sheet_name}

*TEAM PERFORMANCE:*
"""

    for name, value in sorted_data:
        display_name = name.strip() if name.strip() else "(Not Specified)"
        if is_revenue:
            message += f"• {display_name}: ${value:,.0f}\n"
        else:
            message += f"• {display_name}: {value}\n"

    if is_revenue:
        message += f"\n*TOTAL: ${total_value:,.0f}*"
    else:
        message += f"\n*TOTAL: {total_value}*"

    return message


# --- MASTER SHEET ADDITIONAL METRICS ---
def get_yesterday_sheet_name():
    """Get the sheet name for yesterday's date"""
    yesterday = (datetime.now(EST) - timedelta(days=1)).date()
    return yesterday.strftime("%B %d").replace(" 0", " ")  # Remove leading zero from day

def find_yesterday_sheet_in_master():
    """Find yesterday's sheet in the master spreadsheet"""
    try:
        # Get all sheets information from master spreadsheet
        spreadsheet_metadata = (
            service.spreadsheets()
            .get(spreadsheetId=MASTER_SHEET_ID, fields="sheets.properties")
            .execute()
        )
        sheets = spreadsheet_metadata.get("sheets", [])
        
        yesterday_sheet_name = get_yesterday_sheet_name()
        print(f"Looking for sheet: '{yesterday_sheet_name}'")
        
        # Look for exact match first
        for sheet_info in sheets:
            sheet_name = sheet_info["properties"]["title"]
            if sheet_name == yesterday_sheet_name:
                print(f"✅ Found exact match: '{sheet_name}'")
                return sheet_name
        
        # Look for partial match (in case of different formatting)
        yesterday_parts = yesterday_sheet_name.lower().split()
        for sheet_info in sheets:
            sheet_name = sheet_info["properties"]["title"]
            sheet_parts = sheet_name.lower().split()
            
            # Check if month and day match
            if len(yesterday_parts) >= 2 and len(sheet_parts) >= 2:
                if yesterday_parts[0] in sheet_parts and yesterday_parts[1] in sheet_parts:
                    print(f"✅ Found partial match: '{sheet_name}'")
                    return sheet_name
        
        print(f"❌ Could not find sheet for {yesterday_sheet_name}")
        print("Available sheets:")
        for sheet_info in sheets:
            print(f"  - {sheet_info['properties']['title']}")
        
        return None
        
    except Exception as e:
        print(f"Error finding yesterday's sheet: {e}")
        return None

def get_master_sheet_additional_metrics():
    """Get additional metrics from yesterday's sheet in the master spreadsheet"""
    sheet_name = find_yesterday_sheet_in_master()
    if not sheet_name:
        print("Cannot get additional metrics without finding yesterday's sheet")
        return False
    
    try:
        print(f"\n📊 Fetching additional metrics from master sheet: '{sheet_name}'")
        
        # Fetch data from the sheet
        sheet_range = f"'{sheet_name}'!A1:M10"  # Get enough rows and columns
        result = (
            sheet.values()
            .get(spreadsheetId=MASTER_SHEET_ID, range=sheet_range)
            .execute()
        )
        values = result.get("values", [])
        
        if not values:
            print("No data found in the sheet")
            return False
        
        # Process each representative row (skip header)
        for row_idx in range(1, len(values)):
            if row_idx >= len(values) or len(values[row_idx]) == 0:
                continue
                
            row = values[row_idx]
            if len(row) < 2:  # Need at least sales rep name
                continue
                
            sales_rep = row[0].strip().lower() if row[0] else ""
            
            # Map sales rep names to our canonical names
            rep_name = None
            if "mikaela" in sales_rep:
                rep_name = "mikaela"
            elif "mike" in sales_rep or "hammer" in sales_rep:
                rep_name = "mike"
            elif "sierra" in sales_rep:
                rep_name = "sierra"
            elif "team total" in sales_rep.lower():
                continue  # Skip team total row
            
            if not rep_name:
                print(f"Skipping unknown rep: {sales_rep}")
                continue
            
            print(f"\nProcessing additional metrics for {rep_name} ({sales_rep}):")
            
            # Extract additional metrics from master sheet (with safe indexing)
            master_appointments_booked = parse_numeric_value(row[1] if len(row) > 1 else 0)
            master_appointments_conducted = parse_numeric_value(row[2] if len(row) > 2 else 0)
            master_new_clients_closed = parse_numeric_value(row[3] if len(row) > 3 else 0)
            master_organic_clients_closed = parse_numeric_value(row[4] if len(row) > 4 else 0)
            master_total_new_clients = parse_numeric_value(row[5] if len(row) > 5 else 0)
            master_rebuy_clients = parse_numeric_value(row[6] if len(row) > 6 else 0)
            master_show_rate = parse_percentage_value(row[7] if len(row) > 7 else 0)
            master_running_close_rate = parse_percentage_value(row[8] if len(row) > 8 else 0)
            master_new_client_revenue = parse_currency_value(row[9] if len(row) > 9 else 0)
            master_rebuy_revenue = parse_currency_value(row[10] if len(row) > 10 else 0)
            master_total_revenue = parse_currency_value(row[11] if len(row) > 11 else 0)
            master_avg_deal_size = parse_currency_value(row[12] if len(row) > 12 else 0)
            
            # Calculate appointments canceled (booked - conducted, but ensure non-negative)
            master_appointments_canceled = max(0, master_appointments_booked - master_appointments_conducted)
            
            # Store additional metrics with "master_" prefix to distinguish from calculated ones
            store_metric(rep_name, "master_appointments_booked", master_appointments_booked, "count", "Master Sheet")
            store_metric(rep_name, "master_appointments_conducted", master_appointments_conducted, "count", "Master Sheet")
            store_metric(rep_name, "master_appointments_canceled", master_appointments_canceled, "count", "Master Sheet")
            store_metric(rep_name, "master_show_rate", master_show_rate, "percentage", "Master Sheet")
            store_metric(rep_name, "master_new_clients_closed", master_new_clients_closed, "count", "Master Sheet")
            store_metric(rep_name, "master_organic_clients_closed", master_organic_clients_closed, "count", "Master Sheet")
            store_metric(rep_name, "master_total_new_clients_closed", master_total_new_clients, "count", "Master Sheet")
            store_metric(rep_name, "master_rebuy_clients", master_rebuy_clients, "count", "Master Sheet")
            store_metric(rep_name, "master_running_close_rate", master_running_close_rate, "percentage", "Master Sheet")
            store_metric(rep_name, "master_new_client_revenue", master_new_client_revenue, "currency", "Master Sheet")
            store_metric(rep_name, "master_rebuy_revenue", master_rebuy_revenue, "currency", "Master Sheet")
            store_metric(rep_name, "master_total_revenue", master_total_revenue, "currency", "Master Sheet")
            store_metric(rep_name, "master_average_deal_size", master_avg_deal_size, "currency", "Master Sheet")
            
            print(f"  Master Appointments Booked: {master_appointments_booked}")
            print(f"  Master Appointments Conducted: {master_appointments_conducted}")
            print(f"  Master Show Rate: {master_show_rate}%")
            print(f"  Master Running Close Rate: {master_running_close_rate}%")
            print(f"  Master Total Revenue: ${master_total_revenue:,.2f}")
        
        return True
        
    except Exception as e:
        print(f"Error fetching master sheet additional metrics: {e}")
        import traceback
        traceback.print_exc()
        return False


# --- MESSAGE CREATION FUNCTIONS ---
def create_running_close_rate_message(close_rates):
    """Create Slack message for Running Close Rate metric"""
    message = f"""✅ *RUNNING CLOSE RATE (CALCULATED)*
Date: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)
Source: Master Sheet + Appointments Data

*TEAM PERFORMANCE:*
"""

    # Sort by close rate (highest first)
    sorted_rates = sorted(
        close_rates.items(), key=lambda x: x[1], reverse=True
    )

    for name, rate in sorted_rates:
        display_name = name.title()
        message += f"• {display_name}: {rate:.1f}%\n"

    # Calculate average close rate
    if close_rates:
        avg_rate = sum(close_rates.values()) / len(close_rates)
        message += f"\n*AVERAGE CLOSE RATE: {avg_rate:.1f}%*"

    return message


def create_appointments_booked_message(user_results):
    """Create Slack message for Total Appointments Booked metric (including canceled)"""
    total_booked = sum(
        result["scheduled_count"] for result in user_results.values()
    )

    total_canceled = sum(
        result["canceled_count"] for result in user_results.values()
    )

    # Sort by booked count (highest first)
    sorted_users = sorted(
        user_results.items(),
        key=lambda x: x[1]["scheduled_count"],
        reverse=True,
    )

    message = f"""✅ *TOTAL APPOINTMENTS BOOKED (CALCULATED)*
Date: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)
Source: Calendly API

*TEAM PERFORMANCE:*
"""

    for name, result in sorted_users:
        count = result["scheduled_count"]
        canceled = result["canceled_count"]
        display_name = name.title()
        message += f"• {display_name}: {count} appointments\n"
        if canceled > 0:
            message += f"  ↳ Canceled: {canceled}\n"

        # Store calculated metrics
        store_metric(name, "calculated_appointments_booked", count, "count", "Calendly")
        store_metric(name, "calculated_appointments_canceled", canceled, "count", "Calendly")

    message += f"\n*TOTAL BOOKED: {total_booked}*"
    if total_canceled > 0:
        message += f"\n*TOTAL CANCELED: {total_canceled}*"

    return message


def create_appointments_conducted_message(user_results):
    """Create Slack message for Total Appointments Conducted metric"""
    total_conducted = sum(
        result["conducted_count"] for result in user_results.values()
    )

    # Sort by conducted count (highest first)
    sorted_users = sorted(
        user_results.items(),
        key=lambda x: x[1]["conducted_count"],
        reverse=True,
    )

    message = f"""✅ *TOTAL APPOINTMENTS CONDUCTED (CALCULATED)*
Date: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)
Source: Calendly + Zoom Recordings

*TEAM PERFORMANCE:*
"""

    for name, result in sorted_users:
        count = result["conducted_count"]
        display_name = name.title()
        message += f"• {display_name}: {count} appointments\n"

        # Store calculated metrics
        store_metric(name, "calculated_appointments_conducted", count, "count", "Calendly + Zoom")

    message += f"\n*TOTAL CONDUCTED: {total_conducted}*"

    return message


def create_show_rate_message(user_results):
    """Create Slack message for Show Rate metric"""
    message = f"""✅ *SHOW RATE (CALCULATED)*
Date: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)
Source: Calendly + Zoom Recordings

*TEAM PERFORMANCE:*
"""

    # Calculate show rates for each user
    user_show_rates = []
    total_booked_all = 0
    total_conducted_all = 0

    for name, result in user_results.items():
        booked = result["scheduled_count"]
        conducted = result["conducted_count"]
        show_rate = (conducted / booked * 100) if booked > 0 else 0

        user_show_rates.append((name, show_rate, booked, conducted))
        total_booked_all += booked
        total_conducted_all += conducted

        # Store calculated metrics
        store_metric(name, "calculated_show_rate", show_rate, "percentage", "Calendly + Zoom")

    # Sort by show rate (highest first)
    user_show_rates.sort(key=lambda x: x[1], reverse=True)

    for name, show_rate, booked, conducted in user_show_rates:
        display_name = name.title()
        message += (
            f"• {display_name}: {show_rate:.1f}% ({conducted}/{booked})\n"
        )

    # Calculate overall show rate
    overall_show_rate = (
        (total_conducted_all / total_booked_all * 100)
        if total_booked_all > 0
        else 0
    )
    message += f"\n*OVERALL SHOW RATE: {overall_show_rate:.1f}% ({total_conducted_all}/{total_booked_all})*"

    return message


def calculate_average_deal_size():
    """
    Average Deal Size (NEW CLIENT sales only).
    Relies on:
        • new_client_revenue_grouped – today's $ per rep
        • new_clients_counts        – today's deal count per rep
        • MASTER_SHEET_ID           – cumulative sheet
    Returns {rep: avg $, ..., 'team_avg': $}
    """
    # ---------- pull cumulative revenue + deals ----------
    rows = (
        sheet.values()
        .get(
            spreadsheetId=MASTER_SHEET_ID,
            range="A:K",  # col A = name, F = new-client count, J = revenue
        )
        .execute()
        .get("values", [])
    )

    # helper → "sierra campbell" → "sierra campbell"
    norm = lambda s: str(s or "").strip().lower()

    cum_rev_deals = {}
    for r in rows[1:]:
        name = norm(r[0]) if len(r) > 0 else ""
        deals = parse_numeric_value(r[5]) if len(r) > 5 else 0
        rev = parse_currency_value(r[9]) if len(r) > 9 else 0
        if name and name != "jason":
            cum_rev_deals[name] = (rev, deals)

    # ---------- today's dicts ----------
    today_rev = {
        norm(k): v
        for k, v in globals().get("new_client_revenue_grouped", {}).items()
    }
    today_deals = {
        norm(k): v for k, v in globals().get("new_clients_counts", {}).items()
    }

    # canonical rep keys we'll return
    canonical = {
        "sierra": ["sierra", "sierra campbell"],
        "mikaela": ["mikaela", "mikaela gordon"],
        "mike": ["mike", "mike hammer"],
    }

    averages = {}
    for rep_key, name_variants in canonical.items():
        # try each variant until we find a match in the cumulative dict
        cum_rev = cum_deals = 0
        for variant in name_variants:
            if variant in cum_rev_deals:
                cum_rev, cum_deals = cum_rev_deals[variant]
                break

        total_rev = cum_rev + sum(today_rev.get(v, 0) for v in name_variants)
        total_deals = cum_deals + sum(
            today_deals.get(v, 0) for v in name_variants
        )

        avg_size = (total_rev / total_deals) if total_deals else 0
        averages[rep_key] = avg_size

        print(
            f"{rep_key.title()}: ${total_rev:,.0f} / {total_deals} deals → ${avg_size:,.0f}"
        )

        # Store in metrics
        store_metric(rep_key, "calculated_average_deal_size", avg_size, "currency", "Master Sheet + Sales Data")

    # team-wide average
    if averages:
        averages["team_avg"] = sum(averages.values()) / len(averages)

    return averages


def create_deal_size_message(deal_size_dict):
    """
    Build a Slack-ready message summarizing Average Deal Size (new-client sales).
    """
    if not deal_size_dict:
        return "No deal-size data available."

    # Pull and remove optional 'team_avg'
    team_avg = deal_size_dict.pop("team_avg", None)

    # Sort reps by avg deal size, highest first
    sorted_reps = sorted(deal_size_dict.items(), key=lambda x: -x[1])

    msg = f"""✅ *AVERAGE DEAL SIZE (NEW CLIENTS) - CALCULATED*
Date: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)
Source: Master Sheet + Sales Data

*TEAM PERFORMANCE:*
"""
    for rep, avg in sorted_reps:
        msg += f"• {rep.title()}: ${avg:,.0f}\n"

    # Fallback: compute team average if not supplied
    if team_avg is None and deal_size_dict:
        team_avg = sum(deal_size_dict.values()) / len(deal_size_dict)

    if team_avg is not None:
        msg += f"\n*TEAM AVERAGE DEAL SIZE: ${team_avg:,.0f}*"

    return msg


def create_metric_slack_message(metric_name, metric_display_name, metric_type="count"):
    """Create a Slack message for a specific metric from master sheet"""
    yesterday = (datetime.now(EST) - timedelta(days=1)).date()
    
    message = f"""✅ *{metric_display_name.upper()}*
Date: {yesterday.strftime('%Y-%m-%d')} (Yesterday)
Source: Master Sheet

*TEAM PERFORMANCE:*
"""
    
    # Collect data for all reps
    rep_data = []
    total_value = 0
    
    for rep_name in ['sierra', 'mikaela', 'mike']:
        if rep_name in daily_metrics and metric_name in daily_metrics[rep_name]:
            value = daily_metrics[rep_name][metric_name]['value']
            rep_data.append((rep_name, value))
            if metric_type != "percentage":  # Don't sum percentages
                total_value += value
    
    # Sort by value (highest first)
    rep_data.sort(key=lambda x: x[1], reverse=True)
    
    # Add individual rep data
    for rep_name, value in rep_data:
        display_name = rep_name.title()
        if metric_type == "currency":
            message += f"• {display_name}: ${value:,.0f}\n"
        elif metric_type == "percentage":
            message += f"• {display_name}: {value:.1f}%\n"
        else:
            message += f"• {display_name}: {int(value)}\n"
    
    # Add total if applicable
    if metric_type == "currency" and total_value > 0:
        message += f"\n*TOTAL: ${total_value:,.0f}*"
    elif metric_type == "count" and total_value > 0:
        message += f"\n*TOTAL: {int(total_value)}*"
    elif metric_type == "percentage" and rep_data:
        avg_value = sum(x[1] for x in rep_data) / len(rep_data)
        message += f"\n*AVERAGE: {avg_value:.1f}%*"
    
    return message


# --- MAIN EXECUTION FUNCTIONS ---
def analyze_appointments():
    """Analyze appointments from Calendly and Zoom"""
    print("\n" + "=" * 80)
    print("PROCESSING APPOINTMENT DATA...")
    print(
        f"Processing data for: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)"
    )
    print("=" * 80)

    # Get organization URI
    org_uuid = os.getenv("ORG_UUID")
    org_uri = f"https://api.calendly.com/organizations/{org_uuid}"

    # Get Zoom access token
    zoom_token = get_zoom_access_token()

    # Results storage
    all_results = {}

    for name, mappings in USER_MAPPINGS.items():
        if not mappings["calendly_uuid"]:
            print(f"\n{name.upper()}: Missing Calendly UUID")
            continue

        print(f"\nProcessing {name.title()}...")

        # Get Calendly events
        user_uri = (
            f"https://api.calendly.com/users/{mappings['calendly_uuid']}"
        )
        try:
            active_events, canceled_events = get_calendly_events_for_user(user_uri, org_uri)
            all_calendly_events = active_events + canceled_events
            print(f"  Found {len(active_events)} active Calendly events")
            print(f"  Found {len(canceled_events)} canceled Calendly events")
            for event in active_events:
                print(f"    - {event['name']} at {event['start_time_str']}")
        except Exception as e:
            print(f"  Error fetching Calendly events for {name}: {e}")
            active_events = []
            canceled_events = []

        # Get Zoom recordings
        zoom_user_id = get_zoom_user_id_by_email(
            mappings["zoom_email"], zoom_token
        )
        if zoom_user_id:
            zoom_meetings = get_zoom_meetings_for_user_today(
                zoom_user_id, mappings["zoom_email"], zoom_token
            )
        else:
            print(
                f"  Could not find Zoom user for email: {mappings['zoom_email']}"
            )
            zoom_meetings = []

        # Match events with recordings
        matched, unmatched = match_events_with_meetings(
            active_events, zoom_meetings
        )

        # Store results
        all_results[name] = {
            "calendly_events": active_events,
            "canceled_events": canceled_events,
            "zoom_meetings": zoom_meetings,
            "matched_events": matched,
            "unmatched_events": unmatched,
            "scheduled_count": len(active_events),
            "canceled_count": len(canceled_events),
            "conducted_count": len(matched),
        }

        print(
            f"  Results: {len(matched)} conducted out of {len(active_events)} scheduled (active)"
        )

    # Send appointment metrics to Slack
    if all_results:
        # Send appointments booked
        slack_message = create_appointments_booked_message(all_results)
        broadcast_to_slack_users(slack_message)

        # Send appointments conducted
        slack_message = create_appointments_conducted_message(all_results)
        broadcast_to_slack_users(slack_message)

        # Send show rate
        slack_message = create_show_rate_message(all_results)
        broadcast_to_slack_users(slack_message)

        # Calculate and send running close rate
        master_data = get_master_sheet_data()
        if master_data:
            # Create a dictionary of individual user appointments conducted
            user_appointments_conducted = {
                name: result["conducted_count"]
                for name, result in all_results.items()
            }
            # Store globally for use in other functions
            globals()["user_appointments_conducted"] = user_appointments_conducted

        close_rates = calculate_running_close_rate()
        if close_rates:
            slack_message = create_running_close_rate_message(close_rates)
            broadcast_to_slack_users(slack_message)

        deal_size = calculate_average_deal_size()
        if deal_size:
            slack_message = create_deal_size_message(deal_size)
            broadcast_to_slack_users(slack_message)

    return all_results


def analyze_sales_data():
    """Analyze sales data from Google Sheets"""
    print("\n" + "=" * 80)
    print("PROCESSING SALES DATA...")
    print("=" * 80)

    try:
        # Get all sheets information
        spreadsheet_metadata = (
            service.spreadsheets()
            .get(spreadsheetId=SPREADSHEET_ID, fields="sheets.properties")
            .execute()
        )
        sheets = spreadsheet_metadata.get("sheets", [])

        if not sheets:
            print("No sheets found in this spreadsheet.")
            return {}

        # Get the latest sheet based on date parsing
        latest_sheet_info = get_latest_sheet(sheets)

        if not latest_sheet_info:
            print("Could not determine the latest sheet.")
            return {}

        latest_sheet_title = latest_sheet_info["properties"]["title"]

        # Fetch data from the latest sheet
        latest_sheet_range = f"'{latest_sheet_title}'!A1:Z"
        result = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=latest_sheet_range)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            print(f"No data found in sheet '{latest_sheet_title}'.")
            return {}

        print(f"\nProcessing data from: '{latest_sheet_title}'")

        # Convert to DataFrame for easier processing
        max_cols = max(len(row) for row in values) if values else 0
        padded_values = [row + [""] * (max_cols - len(row)) for row in values]

        # Create DataFrame with first row as headers
        df = pd.DataFrame(padded_values[1:], columns=padded_values[0])

        print(f"Total records: {len(df)}")

        # Check if required columns exist and map them
        required_columns = ["Demo By", "ORGANIC?", "REBUY?", "Deal Amount"]
        column_mapping = map_required_columns(df, required_columns)

        missing_columns = [
            col for col in required_columns if col not in column_mapping
        ]

        if missing_columns:
            print(f"\n❌ Could not find matches for: {missing_columns}")
            print("Please check your column names and try again.")
            return {}

        print(f"\n✅ All required columns found and mapped!")

        # Use mapped column names
        demo_by_col = column_mapping["Demo By"]
        organic_col = column_mapping["ORGANIC?"]
        rebuy_col = column_mapping["REBUY?"]
        deal_amount_col = column_mapping["Deal Amount"]

        # Get all unique users (excluding JASON)
        all_users = get_all_unique_users(df, demo_by_col)
        print(f"Unique users found (excluding JASON): {len(all_users)}")
        print(f"Users: {', '.join(all_users)}")

        # Parse Deal Amount column
        df["Deal Amount Parsed"] = df[deal_amount_col].apply(
            parse_currency_value
        )

        # 1. NEW CLIENTS CLOSED (both ORGANIC? and REBUY? are empty)
        new_clients_df = df[
            df[organic_col].apply(is_empty_or_null)
            & df[rebuy_col].apply(is_empty_or_null)
        ]

        # 2. NEW CLIENTS CLOSED (ORGANIC) (ORGANIC? has value, REBUY? is empty)
        organic_clients_df = df[
            df[organic_col].apply(has_value)
            & df[rebuy_col].apply(is_empty_or_null)
        ]

        # 3. REBUY CLIENTS (REBUY? has value)
        rebuy_clients_df = df[df[rebuy_col].apply(has_value)]

        # 4. NEW CLIENT REVENUE (REBUY? is empty)
        new_client_revenue_df = df[df[rebuy_col].apply(is_empty_or_null)]

        # 5. REBUY REVENUE (REBUY? has value)
        rebuy_revenue_df = df[df[rebuy_col].apply(has_value)]

        print(f"\nData Analysis:")
        print(
            f"Records with both ORGANIC? and REBUY? empty (New Clients): {len(new_clients_df)}"
        )
        print(
            f"Records with ORGANIC? value and REBUY? empty (Organic Clients): {len(organic_clients_df)}"
        )
        print(
            f"Records with REBUY? value (Rebuy Clients): {len(rebuy_clients_df)}"
        )
        print(
            f"Records with REBUY? empty (New Client Revenue): {len(new_client_revenue_df)}"
        )
        print(
            f"Records with REBUY? value (Rebuy Revenue): {len(rebuy_revenue_df)}"
        )

        # Process and send metrics to Slack

        # 1. New Clients Closed
        if len(new_clients_df) > 0:
            new_clients_counts = (
                new_clients_df[demo_by_col].value_counts().to_dict()
            )
        else:
            new_clients_counts = {}

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            # Map rep names to sheet names
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            # Find matching name in data
            count = 0
            for name_variant in rep_mapping.get(rep, [rep]):
                count += new_clients_counts.get(name_variant, 0)
            
            store_metric(rep, "calculated_new_clients_closed", count, "count", "Google Sheets")

        slack_message = create_slack_message(
            new_clients_counts,
            all_users,
            latest_sheet_title,
            "NEW CLIENTS CLOSED (CALCULATED)",
        )
        broadcast_to_slack_users(slack_message)

        # 2. Organic Clients Closed
        if len(organic_clients_df) > 0:
            organic_clients_counts = (
                organic_clients_df[demo_by_col].value_counts().to_dict()
            )
        else:
            organic_clients_counts = {}

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            count = 0
            for name_variant in rep_mapping.get(rep, [rep]):
                count += organic_clients_counts.get(name_variant, 0)
            
            store_metric(rep, "calculated_organic_clients_closed", count, "count", "Google Sheets")

        slack_message = create_slack_message(
            organic_clients_counts,
            all_users,
            latest_sheet_title,
            "NEW CLIENTS CLOSED (ORGANIC) - CALCULATED",
        )
        broadcast_to_slack_users(slack_message)

        # 3. Rebuy Clients
        if len(rebuy_clients_df) > 0:
            rebuy_clients_counts = (
                rebuy_clients_df[demo_by_col].value_counts().to_dict()
            )
        else:
            rebuy_clients_counts = {}

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            count = 0
            for name_variant in rep_mapping.get(rep, [rep]):
                count += rebuy_clients_counts.get(name_variant, 0)
            
            store_metric(rep, "calculated_rebuy_clients", count, "count", "Google Sheets")

        slack_message = create_slack_message(
            rebuy_clients_counts,
            all_users,
            latest_sheet_title,
            "REBUY CLIENTS (CALCULATED)",
        )
        broadcast_to_slack_users(slack_message)

        # 4. New Client Revenue
        if len(new_client_revenue_df) > 0:
            new_client_revenue_grouped = (
                new_client_revenue_df.groupby(demo_by_col)[
                    "Deal Amount Parsed"
                ]
                .sum()
                .to_dict()
            )
        else:
            new_client_revenue_grouped = {}

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            revenue = 0.0
            for name_variant in rep_mapping.get(rep, [rep]):
                revenue += new_client_revenue_grouped.get(name_variant, 0.0)
            
            store_metric(rep, "calculated_new_client_revenue", revenue, "currency", "Google Sheets")

        slack_message = create_slack_message(
            new_client_revenue_grouped,
            all_users,
            latest_sheet_title,
            "NEW CLIENT REVENUE (CALCULATED)",
            is_revenue=True,
        )
        broadcast_to_slack_users(slack_message)

        # 5. Total New Clients Closed
        total_new_clients = {
            rep: new_clients_counts.get(rep, 0)
            + organic_clients_counts.get(rep, 0)
            for rep in all_users
        }

        slack_message = create_slack_message(
            total_new_clients,
            all_users,
            latest_sheet_title,
            "TOTAL NEW CLIENTS CLOSED (CALCULATED)",
        )
        broadcast_to_slack_users(slack_message)

        # 6. Rebuy Revenue
        if len(rebuy_revenue_df) > 0:
            rebuy_revenue_grouped = (
                rebuy_revenue_df.groupby(demo_by_col)["Deal Amount Parsed"]
                .sum()
                .to_dict()
            )
        else:
            rebuy_revenue_grouped = {}

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            revenue = 0.0
            for name_variant in rep_mapping.get(rep, [rep]):
                revenue += rebuy_revenue_grouped.get(name_variant, 0.0)
            
            store_metric(rep, "calculated_rebuy_revenue", revenue, "currency", "Google Sheets")

        slack_message = create_slack_message(
            rebuy_revenue_grouped,
            all_users,
            latest_sheet_title,
            "REBUY REVENUE (CALCULATED)",
            is_revenue=True,
        )
        broadcast_to_slack_users(slack_message)

        # 7. Total Revenue
        total_revenue_dict = {}
        for user in all_users:
            new_revenue = new_client_revenue_grouped.get(user, 0.0)
            rebuy_revenue = rebuy_revenue_grouped.get(user, 0.0)
            total_revenue_dict[user] = new_revenue + rebuy_revenue

        # Store metrics for each rep
        for rep in ['sierra', 'mikaela', 'mike']:
            rep_mapping = {
                'sierra': ['sierra', 'sierra campbell', 'sierrac'],
                'mikaela': ['mikaela', 'mikaela gordon'],
                'mike': ['mike', 'mike hammer', 'hammer']
            }
            
            total_revenue = 0.0
            for name_variant in rep_mapping.get(rep, [rep]):
                total_revenue += total_revenue_dict.get(name_variant, 0.0)
            
            store_metric(rep, "calculated_total_revenue", total_revenue, "currency", "Google Sheets")

        slack_message = create_slack_message(
            total_revenue_dict,
            all_users,
            latest_sheet_title,
            "TOTAL REVENUE (CALCULATED)",
            is_revenue=True,
        )
        broadcast_to_slack_users(slack_message)

        # Store global variables for running close rate calculation
        globals()["new_clients_counts"] = new_clients_counts
        globals()["new_client_revenue_grouped"] = new_client_revenue_grouped

        # Return processed data for database storage
        return {
            'new_clients_counts': new_clients_counts,
            'organic_clients_counts': organic_clients_counts,
            'rebuy_clients_counts': rebuy_clients_counts,
            'new_client_revenue_grouped': new_client_revenue_grouped,
            'rebuy_revenue_grouped': rebuy_revenue_grouped,
            'sheet_name': latest_sheet_title
        }

    except HttpError as err:
        print(f"An error occurred: {err}")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        return {}


def send_master_sheet_metric_messages():
    """Send Slack messages for master sheet metrics"""
    master_metrics_to_send = [
        ("master_appointments_booked", "Total Appointments Booked (Master Sheet)", "count"),
        ("master_appointments_conducted", "Total Appointments Conducted (Master Sheet)", "count"),
        ("master_show_rate", "Show Rate (Master Sheet)", "percentage"),
        ("master_new_clients_closed", "New Clients Closed (Master Sheet)", "count"),
        ("master_organic_clients_closed", "New Clients Closed Organic (Master Sheet)", "count"),
        ("master_total_new_clients_closed", "Total New Clients Closed (Master Sheet)", "count"),
        ("master_rebuy_clients", "Rebuy Clients (Master Sheet)", "count"),
        ("master_running_close_rate", "Running Close Rate (Master Sheet)", "percentage"),
        ("master_new_client_revenue", "New Client Revenue (Master Sheet)", "currency"),
        ("master_rebuy_revenue", "Rebuy Revenue (Master Sheet)", "currency"),
        ("master_total_revenue", "Total Revenue (Master Sheet)", "currency"),
        ("master_average_deal_size", "Average Deal Size (Master Sheet)", "currency"),
    ]
    
    for metric_name, display_name, metric_type in master_metrics_to_send:
        message = create_metric_slack_message(metric_name, display_name, metric_type)
        broadcast_to_slack_users(message)


def main():
    """Main function to run all analyses"""
    print("🚀 Starting comprehensive sales and appointment analysis...")
    print(f"Processing data for: {(datetime.now(EST) - timedelta(days=1)).strftime('%Y-%m-%d')} (Yesterday)")

    # Check required environment variables
    required_vars = [
        CALENDLY_PAT,
        ZOOM_ACCOUNT_ID,
        ZOOM_CLIENT_ID,
        ZOOM_CLIENT_SECRET,
        SPREADSHEET_ID,
        MASTER_SHEET_ID,
        SLACK_BOT_TOKEN,
    ]

    if not all(required_vars):
        print("Please set all required environment variables:")
        print("- CALENDLY_PAT")
        print("- ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET")
        print("- User UUIDs: SIERRA_UUID, MIKAELA_UUID, MIKE_UUID")
        print("- ORG_UUID")
        print("- GOOGLE_SHEET_ID")
        print("- MASTER_SHEET_ID")
        print("- SLACK_BOT_TOKEN")
        print("- DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (for database)")
        return

    try:
        # Create database table if it doesn't exist
        create_daily_metrics_table()

        # Analyze appointments (Calendly + Zoom) - CALCULATED metrics
        appointment_results = analyze_appointments()

        # Analyze sales data (Google Sheets) - CALCULATED metrics
        sales_data = analyze_sales_data()

        # Get additional metrics from master sheet - MASTER SHEET metrics
        get_master_sheet_additional_metrics()

        # Send master sheet metrics to Slack
        send_master_sheet_metric_messages()

        # Save all metrics to database
        save_all_metrics_to_db()

        print("\n✅ All analyses completed, messages sent to Slack, and data saved to database!")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
