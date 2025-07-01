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
    """Get Zoom RECORDINGS for a user for today in EST."""
    # Get today's date in EST
    now_est = datetime.now(EST)
    today_est = now_est.date()

    # Convert to UTC for API call (Zoom API expects UTC dates)
    start_date_est = EST.localize(datetime.combine(today_est, time(0, 0, 0)))
    end_date_est = EST.localize(datetime.combine(today_est, time(23, 59, 59)))

    start_date_utc = start_date_est.astimezone(pytz.UTC)
    end_date_utc = end_date_est.astimezone(pytz.UTC)

    print(f"  Fetching Zoom recordings for {email} on {today_est} (EST)")
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

                    # Check if meeting is today in EST
                    if start_time_est.date() == today_est:
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
    """Get today's Calendly events for a specific user."""
    # Get today's date in EST
    now_est = datetime.now(EST)
    today_est = now_est.date()

    # Convert to UTC for API call
    start_time_est = EST.localize(datetime.combine(today_est, time(0, 0, 0)))
    end_time_est = EST.localize(datetime.combine(today_est, time(23, 59, 59)))

    start_time_utc = start_time_est.astimezone(pytz.UTC)
    end_time_utc = end_time_est.astimezone(pytz.UTC)

    url = "https://api.calendly.com/scheduled_events"
    params = {
        "user": user_uri,
        "organization": org_uri,
        "sort": "start_time:asc",
        "min_start_time": start_time_utc.isoformat(),
        "max_start_time": end_time_utc.isoformat(),
        "status": "active",
    }

    events = []

    while url:
        response = requests.get(url, headers=CALENDLY_HEADERS, params=params)
        response.raise_for_status()
        data = response.json()

        active_events = [
            event
            for event in data.get("collection", [])
            if event.get("status") == "active"
        ]
        events.extend(active_events)

        url = data.get("pagination", {}).get("next_page")
        params = {}

    # Process events to extract relevant information
    processed_events = []
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
            processed_events.append(event_info)

    return processed_events


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
    Re-compute each rep’s Running Close Rate (Sit→Sale) without ever
    exceeding 100 %.
    - Relies on globals created elsewhere in the script:
        • user_appointments_conducted  – today’s *conducted* sits per rep
        • new_clients_counts           – today’s *non-organic* closes per rep
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
        # locate this rep’s row in master_data (columns C & D)
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
            message += (
                f"• {display_name}: ${value:,.0f}\n"  # Changed from ₹ to $
            )
        else:
            message += f"• {display_name}: {value}\n"

    if is_revenue:
        message += f"\n*TOTAL: ${total_value:,.0f}*"  # Changed from ₹ to $
    else:
        message += f"\n*TOTAL: {total_value}*"

    return message


# --- MESSAGE CREATION FUNCTIONS ---
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


def create_running_close_rate_message(close_rates):
    """Create Slack message for Running Close Rate metric"""
    message = f"""✅ *RUNNING CLOSE RATE*
Date: {datetime.now(EST).strftime('%Y-%m-%d')}
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
    """Create Slack message for Total Appointments Booked metric"""
    total_booked = sum(
        result["scheduled_count"] for result in user_results.values()
    )

    # Sort by booked count (highest first)
    sorted_users = sorted(
        user_results.items(),
        key=lambda x: x[1]["scheduled_count"],
        reverse=True,
    )

    message = f"""✅ *TOTAL APPOINTMENTS BOOKED*
Date: {datetime.now(EST).strftime('%Y-%m-%d')}
Source: Calendly API

*TEAM PERFORMANCE:*
"""

    for name, result in sorted_users:
        count = result["scheduled_count"]
        display_name = name.title()
        message += f"• {display_name}: {count} appointments\n"

    message += f"\n*TOTAL BOOKED: {total_booked}*"

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

    message = f"""✅ *TOTAL APPOINTMENTS CONDUCTED*
Date: {datetime.now(EST).strftime('%Y-%m-%d')}
Source: Calendly + Zoom Recordings

*TEAM PERFORMANCE:*
"""

    for name, result in sorted_users:
        count = result["conducted_count"]
        display_name = name.title()
        message += f"• {display_name}: {count} appointments\n"

    message += f"\n*TOTAL CONDUCTED: {total_conducted}*"

    return message


def create_show_rate_message(user_results):
    """Create Slack message for Show Rate metric"""
    message = f"""✅ *SHOW RATE*
Date: {datetime.now(EST).strftime('%Y-%m-%d')}
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
        • new_client_revenue_grouped – today’s $ per rep
        • new_clients_counts        – today’s deal count per rep
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

    # ---------- today’s dicts ----------
    today_rev = {
        norm(k): v
        for k, v in globals().get("new_client_revenue_grouped", {}).items()
    }
    today_deals = {
        norm(k): v for k, v in globals().get("new_clients_counts", {}).items()
    }

    # canonical rep keys we’ll return
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

    # team-wide average
    if averages:
        averages["team_avg"] = sum(averages.values()) / len(averages)

    return averages


def create_deal_size_message(deal_size_dict):
    """
    Build a Slack-ready message summarizing Average Deal Size (new-client sales).

    Parameters
    ----------
    deal_size_dict : dict
        Output from `calculate_average_deal_size()`
        e.g. {'sierra': 3500.0, 'mikaela': 4100.0, 'mike': 2800.0, 'team_avg': 3466.7}

    Returns
    -------
    str
        Formatted Slack message.
    """
    if not deal_size_dict:
        return "No deal-size data available."

    # Pull and remove optional 'team_avg'
    team_avg = deal_size_dict.pop("team_avg", None)

    # Sort reps by avg deal size, highest first
    sorted_reps = sorted(deal_size_dict.items(), key=lambda x: -x[1])

    msg = f"""✅ *AVERAGE DEAL SIZE (NEW CLIENTS)*
Date: {datetime.now(EST).strftime('%Y-%m-%d')}
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


# --- MAIN EXECUTION FUNCTIONS ---
def analyze_appointments():
    """Analyze appointments from Calendly and Zoom"""
    print("\n" + "=" * 80)
    print("PROCESSING APPOINTMENT DATA...")
    print(
        f"Current time (EST): {datetime.now(EST).strftime('%Y-%m-%d %H:%M:%S %Z')}"
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
            calendly_events = get_calendly_events_for_user(user_uri, org_uri)
            print(f"  Found {len(calendly_events)} Calendly events")
            for event in calendly_events:
                print(f"    - {event['name']} at {event['start_time_str']}")
        except Exception as e:
            print(f"  Error fetching Calendly events for {name}: {e}")
            calendly_events = []

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
            calendly_events, zoom_meetings
        )

        # Store results
        all_results[name] = {
            "calendly_events": calendly_events,
            "zoom_meetings": zoom_meetings,
            "matched_events": matched,
            "unmatched_events": unmatched,
            "scheduled_count": len(calendly_events),
            "conducted_count": len(matched),
        }

        print(
            f"  Results: {len(matched)} conducted out of {len(calendly_events)} scheduled"
        )

    # Calculate total appointments conducted for running close rate
    total_conducted = sum(
        result["conducted_count"] for result in all_results.values()
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
            return

        # Get the latest sheet based on date parsing
        latest_sheet_info = get_latest_sheet(sheets)

        if not latest_sheet_info:
            print("Could not determine the latest sheet.")
            return

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
            return

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
            return

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

        slack_message = create_slack_message(
            new_clients_counts,
            all_users,
            latest_sheet_title,
            "NEW CLIENTS CLOSED",
        )
        broadcast_to_slack_users(slack_message)

        # 2. Organic Clients Closed
        if len(organic_clients_df) > 0:
            organic_clients_counts = (
                organic_clients_df[demo_by_col].value_counts().to_dict()
            )
        else:
            organic_clients_counts = {}

        slack_message = create_slack_message(
            organic_clients_counts,
            all_users,
            latest_sheet_title,
            "NEW CLIENTS CLOSED (ORGANIC)",
        )
        broadcast_to_slack_users(slack_message)

        # 3. Rebuy Clients
        if len(rebuy_clients_df) > 0:
            rebuy_clients_counts = (
                rebuy_clients_df[demo_by_col].value_counts().to_dict()
            )
        else:
            rebuy_clients_counts = {}

        slack_message = create_slack_message(
            rebuy_clients_counts,
            all_users,
            latest_sheet_title,
            "REBUY CLIENTS",
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

        slack_message = create_slack_message(
            new_client_revenue_grouped,
            all_users,
            latest_sheet_title,
            "NEW CLIENT REVENUE",
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
            "TOTAL NEW CLIENTS CLOSED",
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

        slack_message = create_slack_message(
            rebuy_revenue_grouped,
            all_users,
            latest_sheet_title,
            "REBUY REVENUE",
            is_revenue=True,
        )
        broadcast_to_slack_users(slack_message)

        # 7. Total Revenue
        total_revenue_dict = {}
        for user in all_users:
            new_revenue = new_client_revenue_grouped.get(user, 0.0)
            rebuy_revenue = rebuy_revenue_grouped.get(user, 0.0)
            total_revenue_dict[user] = new_revenue + rebuy_revenue

        slack_message = create_slack_message(
            total_revenue_dict,
            all_users,
            latest_sheet_title,
            "TOTAL REVENUE",
            is_revenue=True,
        )
        broadcast_to_slack_users(slack_message)

    except HttpError as err:
        print(f"An error occurred: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Main function to run all analyses"""
    print("🚀 Starting comprehensive sales and appointment analysis...")

    # Check required environment variables
    required_vars = [
        CALENDLY_PAT,
        ZOOM_ACCOUNT_ID,
        ZOOM_CLIENT_ID,
        ZOOM_CLIENT_SECRET,
        SPREADSHEET_ID,
        SLACK_BOT_TOKEN,
    ]

    if not all(required_vars):
        print("Please set all required environment variables:")
        print("- CALENDLY_PAT")
        print("- ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET")
        print("- User UUIDs: SIERRA_UUID, MIKAELA_UUID, MIKE_UUID")
        print("- ORG_UUID")
        print("- GOOGLE_SHEET_ID")
        print("- MASTER_SHEET_ID (optional)")
        print("- SLACK_BOT_TOKEN")
        return

    try:
        # Analyze appointments (Calendly + Zoom) - This also calculates Running Close Rate
        analyze_appointments()

        # Analyze sales data (Google Sheets)
        analyze_sales_data()

        print("\n✅ All analyses completed and messages sent to Slack!")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
