#!/usr/bin/env python3
import json
import sys
import os
import time
import hashlib
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def convert_time_to_ms(time_str):
    """Convert a time string to milliseconds"""
    if not time_str:
        return None
    try:
        # Handle MM:SS.mm format
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes, seconds = parts
            total_seconds = int(minutes)*60 + float(seconds)
            return int(total_seconds * 1000)
        return None
    except Exception as e:
        print(f"Error converting time '{time_str}': {e}")
        return None

def parse_date(date_str):
    """Parse date string in M/D format and return YYYY-MM-DD"""
    if not date_str:
        return None
    try:
        month, day = date_str.split('/')
        year = datetime.now().year  # Use current year
        return f"{year}-{int(month):02d}-{int(day):02d}"
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        return None

def parse_race_time(time_str):
    """Parse race time string (e.g. '8:15AM') to proper time format"""
    if not time_str:
        return None
    try:
        # Parse the time and return it as a formatted string
        time = datetime.strptime(time_str, '%I:%M%p')
        return time.strftime('%H:%M:%S')
    except Exception as e:
        print(f"Error parsing race time '{time_str}': {e}")
        return None

def format_race_time(time_str):
    """Format a race time string to HH:MM:SS"""
    if not time_str:
        return None
    try:
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:  # MM:SS format
                dt = datetime.strptime(f"00:{time_str}", '%H:%M:%S.%f' if '.' in time_str else '%H:%M:%S')
            else:  # HH:MM:SS format
                dt = datetime.strptime(time_str, '%H:%M:%S.%f' if '.' in time_str else '%H:%M:%S')
            return dt.strftime('%H:%M:%S')
        return None
    except Exception as e:
        print(f"Error formatting race time '{time_str}': {e}")
        return None

def get_or_create_competitor(supabase: Client, competitor_data: dict):
    """Check if a competitor exists and return its id"""
    name_long = competitor_data.get("name_long")
    designation = competitor_data.get("designation")
    
    if not name_long:
        return None
        
    # Search for competitor with both name_long and designation
    query = supabase.table("competitor").select("*").eq("name_long", name_long)
    if designation:
        query = query.eq("designation", designation)
    else:
        query = query.is_("designation", None)
    
    response = query.execute()
    
    if response.data:
        competitor_desc = f"'{name_long}'{' ' + designation if designation else ''}"
        print(f"Found existing competitor {competitor_desc} with ID: {response.data[0]['id']}")
        return response.data[0]["id"]
        
    competitor_payload = {
        "name_long": name_long,
        "name_short": competitor_data.get("name_short"),
        "designation": designation,
        "external_id": competitor_data.get("external_id")
    }
    
    response = supabase.table("competitor").insert(competitor_payload).execute()
    competitor_desc = f"'{name_long}'{' ' + designation if designation else ''}"
    print(f"Created new competitor {competitor_desc} with ID: {response.data[0]['id']}")
    return response.data[0]["id"]

def get_or_create_category(supabase: Client, category_data: dict, abbreviation: str = None):
    """Check if a category exists and return its id"""
    name = category_data.get("name")
    if not name:
        return None
        
    response = supabase.table("category").select("*").eq("name", name).execute()
    if response.data:
        print(f"Found existing category '{name}' with ID: {response.data[0]['id']}")
        return response.data[0]["id"]
        
    category_payload = {
        "name": name,
        "title": category_data.get("title"),
        "course_length": category_data.get("course_length"),
        "abbreviation": abbreviation
    }
    
    response = supabase.table("category").insert(category_payload).execute()
    print(f"Created new category '{name}' ({abbreviation}) with ID: {response.data[0]['id']}")
    return response.data[0]["id"]

def upsert_event(supabase: Client, event_data: dict):
    """Insert or update an event record"""
    name = event_data.get("name")
    start_date = event_data.get("start_date")
    
    if not name or not start_date:
        return None
        
    response = supabase.table("event").select("id").eq("name", name).eq("start_date", start_date).execute()
    
    event_payload = {
        "name": name,
        "start_date": start_date,
        "end_date": event_data.get("end_date") or start_date,
        "location": event_data.get("location"),
        "provider_id": 1  # ClockCaster provider ID
    }
    
    if response.data:
        event_id = response.data[0]["id"]
        response = supabase.table("event").update(event_payload).eq("id", event_id).execute()
        print(f"Updated event '{name}' ({start_date}) with ID: {event_id}")
    else:
        response = supabase.table("event").insert(event_payload).execute()
        print(f"Created new event '{name}' ({start_date}) with ID: {response.data[0]['id']}")
        
    return response.data[0]["id"]

def generate_race_fingerprint(event_name: str, race_data: dict, schedule_item: dict) -> str:
    """Generate a unique fingerprint for a race based on event and race details"""
    components = [
        event_name.replace(" ", "_"),
        str(race_data.get("race_day", "")),
        str(race_data.get("race_num", "")),
        schedule_item.get("cat_abrev", ""),
        schedule_item.get("race_abrev", "")
    ]
    return "_".join(filter(None, components)).lower()

def upsert_race(supabase: Client, race_data: dict, fingerprint: str):
    """Insert or update a race record"""
    race_num = race_data.get("race_num")
    if not race_num:
        return None
        
    # Add fingerprint to race data
    race_data["fingerprint"] = fingerprint
        
    # Check for existing race with same fingerprint
    response = supabase.table("race").select("id").eq("fingerprint", fingerprint).execute()
    
    # Remove start_armed field if it exists
    if "start_armed" in race_data:
        del race_data["start_armed"]
    
    if response.data:
        race_id = response.data[0]["id"]
        response = supabase.table("race").update(race_data).eq("id", race_id).execute()
        print(f"Updated race #{race_num} with ID: {race_id} [fingerprint: {fingerprint}]")
    else:
        response = supabase.table("race").insert(race_data).execute()
        print(f"Created new race #{race_num} with ID: {response.data[0]['id']} [fingerprint: {fingerprint}]")
        
    return response.data[0]["id"]

def upsert_schedule(supabase: Client, event_id: str, race_id: int, category_id: str):
    """Insert or update a schedule record"""
    response = supabase.table("schedule").select("id").eq("event_id", event_id).eq("race_id", race_id).eq("category_id", category_id).execute()
    
    if response.data:
        schedule_id = response.data[0]["id"]
        print(f"Found existing schedule item with ID: {schedule_id}")
        return schedule_id
        
    response = supabase.table("schedule").insert({
        "event_id": event_id,
        "race_id": race_id,
        "category_id": category_id
    }).execute()
    
    print(f"Created new schedule item with ID: {response.data[0]['id']}")
    return response.data[0]["id"]

def upsert_result(supabase: Client, result_data: dict, schedule_id: int):
    """Insert or update a result record"""
    lane_boat_number = result_data.get("lane_boat_number")
    if not lane_boat_number:
        return None
        
    response = supabase.table("result").select("id").eq("schedule_id", schedule_id).eq("lane_boat_number", lane_boat_number).execute()
    
    result_data["schedule_id"] = schedule_id
    
    if response.data:
        result_id = response.data[0]["id"]
        response = supabase.table("result").update(result_data).eq("id", result_id).execute()
        print(f"Updated result for lane/boat {lane_boat_number} with ID: {result_id}")
    else:
        response = supabase.table("result").insert(result_data).execute()
        print(f"Created new result for lane/boat {lane_boat_number} with ID: {response.data[0]['id']}")
        
    return response.data[0]["id"]

# =============================================================================
# API POLLING FUNCTIONS
# =============================================================================
def fetch_clockcaster_data(event_id):
    """Fetch data from ClockCaster API for the specified event ID"""
    print(f"\nFetching data from ClockCaster API for event ID: {event_id}")
    url = "https://pdx.clockcaster.com/api/eventDump"
    
    try:
        response = requests.post(
            url,
            files={"eventId": (None, str(event_id))}
        )
        
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding API response as JSON")
        return None

def calculate_data_hash(data):
    """Calculate a hash of the data to detect changes"""
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_str.encode()).hexdigest()

def save_data_to_file(data, filename="payload.json"):
    """Save data to a file"""
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filename}")
        return True
    except Exception as e:
        print(f"Error saving data to file: {e}")
        return False

def poll_and_process(event_id, interval_minutes=5, supabase=None):
    """Poll the API at regular intervals and process data if changed"""
    last_data_hash = None
    
    print(f"Starting polling for event ID {event_id} every {interval_minutes} minutes...")
    
    while True:
        try:
            data = fetch_clockcaster_data(event_id)
            if not data:
                print("Failed to fetch data, will retry in the next interval")
                time.sleep(interval_minutes * 60)
                continue
            
            current_hash = calculate_data_hash(data)
            
            # Check if data has changed
            if current_hash != last_data_hash:
                print("Data has changed, processing updates...")
                last_data_hash = current_hash
                
                # Save to file for debugging and as a backup
                save_data_to_file(data)
                
                # Process the data using existing functions
                process_data(data, supabase)
            else:
                print(f"No changes detected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Wait for the next interval
            print(f"Next check in {interval_minutes} minutes...")
            time.sleep(interval_minutes * 60)
            
        except Exception as e:
            print(f"Error during polling: {e}")
            print("Continuing to poll...")
            time.sleep(interval_minutes * 60)

def process_data(payload, supabase):
    """Process data from payload and update database"""
    # Process event info
    print("\nProcessing event information...")
    event_data = payload.get("info", {})
    event_id = upsert_event(supabase, event_data)
    if not event_id:
        print("Failed to create or update event")
        return
    
    # Process schedule and results
    print("\nProcessing schedule and results...")
    total_races = len(payload.get("schedule", []))
    processed_races = 0
    
    for schedule_item in payload.get("schedule", []):
        processed_races += 1
        print(f"\nProcessing race {processed_races}/{total_races}...")
        
        # Create or update race with fingerprint
        race_data = schedule_item.get("race", {}).copy()
        if "start_armed" in race_data:
            del race_data["start_armed"]
        
        # Generate fingerprint for this race
        fingerprint = generate_race_fingerprint(event_data["name"], race_data, schedule_item)
        race_id = upsert_race(supabase, race_data, fingerprint)
        
        if not race_id:
            print(f"Skipping race due to missing race number")
            continue
            
        # Create or update category with abbreviation
        category_data = schedule_item.get("category", {})
        category_id = get_or_create_category(supabase, category_data, schedule_item.get("cat_abrev"))
        if not category_id:
            print(f"Skipping race due to missing category information")
            continue
            
        # Create or update schedule entry
        schedule_id = upsert_schedule(supabase, event_id, race_id, category_id)
        if not schedule_id:
            print(f"Skipping race due to schedule creation failure")
            continue
            
        # Process results
        print(f"\nProcessing results for race #{race_data.get('race_num')}...")
        total_results = len(schedule_item.get("results", []))
        processed_results = 0
        
        for result in schedule_item.get("results", []):
            processed_results += 1
            print(f"Processing result {processed_results}/{total_results}...")
            
            # Create or update competitor
            competitor_id = get_or_create_competitor(supabase, result.get("competitor", {}))
            if not competitor_id:
                print("Skipping result due to missing competitor information")
                continue
                
            # Prepare and create/update result
            result_data = {
                "competitor_id": competitor_id,
                "placement": int(result["placement"]) if result.get("placement") else None,
                "lane_boat_number": result.get("lane_boat_number"),
                "start_time": format_race_time(result.get("start_time")),
                "finish_time": format_race_time(result.get("finish_time")),
                "raw_time": format_race_time(result.get("raw_time")),
                "total_time": convert_time_to_ms(result.get("total_time")),
                "adjustment": result.get("adjustment"),
                "handicap": result.get("handicap"),
                "remark": result.get("remark"),
                "notes": result.get("notes")
            }
            
            # Filter out None values
            result_data = {k: v for k, v in result_data.items() if v is not None}
            
            result_id = upsert_result(supabase, result_data, schedule_id)
            if not result_id:
                print("Failed to process result")
                continue
    
    print("\nData processing complete!")
    print(f"Processed {processed_races} races successfully.")

# =============================================================================
# MAIN IMPORT LOGIC
# =============================================================================
def main():
    print("\nClockCaster data import starting...")
    
    # Connect to Supabase
    opts = ClientOptions().replace(schema="timing")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_API_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_API_KEY environment variables must be set.")
        sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=opts)
    print("Connected to Supabase successfully")
    
    # Check command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='ClockCaster data import tool')
    parser.add_argument('--poll', action='store_true', help='Enable polling mode')
    parser.add_argument('--event-id', type=str, default="60", help='ClockCaster event ID to poll')
    parser.add_argument('--interval', type=int, default=5, help='Polling interval in minutes')
    parser.add_argument('--file', type=str, help='Process data from local file instead of API')
    args = parser.parse_args()
    
    if args.poll:
        # Polling mode
        poll_and_process(args.event_id, args.interval, supabase)
    else:
        # One-time processing mode
        if args.file:
            # Process from file
            try:
                with open(args.file, "r") as f:
                    payload = json.load(f)
                    print(f"Successfully loaded {args.file}")
                    process_data(payload, supabase)
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                sys.exit(1)
        else:
            # Fetch once from API and process
            data = fetch_clockcaster_data(args.event_id)
            if data:
                save_data_to_file(data)
                process_data(data, supabase)
            else:
                print("Failed to fetch data from API")
                sys.exit(1)

if __name__ == "__main__":
    main()