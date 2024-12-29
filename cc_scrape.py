import json
from bs4 import BeautifulSoup
import re

# Load the HTML from a file (adjust filename as needed)
filename = "cc.html"
with open(filename, "r", encoding="utf-8") as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, "html.parser")

# ---------------------------
# Extract Event Information
# ---------------------------
info = {}
print("Extracting event info...")

event_title = soup.select_one(".eventHeadingTitleWrap h1.eventHeadingTitle")
if event_title:
    info["name"] = event_title.get_text(strip=True)
    print("Event Name:", info["name"])
else:
    print("No event title found.")

event_date = soup.select_one(".eventHeadingDate")
if event_date:
    raw_date = event_date.get_text(strip=True)
    parts = raw_date.split("/")
    if len(parts) == 3:
        year = parts[2]
        month = parts[0].zfill(2)
        day = parts[1].zfill(2)
        info["start_date"] = f"{year}-{month}-{day}"
        info["end_date"] = f"{year}-{month}-{day}"
        print("Start date:", info["start_date"])
        print("End date:", info["end_date"])
    else:
        info["start_date"] = raw_date
        print("Start date (unparsed):", raw_date)
else:
    print("No event date found.")

event_venues = soup.select(".eventHeadingVenue")
location_parts = []
for v in event_venues:
    loc_text = v.get_text(strip=True)
    if loc_text:
        location_parts.append(loc_text)

if location_parts:
    info["location"] = ", ".join(location_parts)
    print("Location:", info["location"])
else:
    print("No venue/location info found.")

# ---------------------------
# Extract Schedule (Races)
# ---------------------------
schedule = []

race_headers = soup.find_all("h4")
print("Number of h4 headers found:", len(race_headers))

for i, header in enumerate(race_headers, start=1):
    print("\n------------------------")
    print(f"Processing header #{i}:")
    race_data = {}

    header_text = header.get_text(strip=True)
    print("Header text:", header_text)
    
    # Normalize the header text by ensuring there's a space before the hyphen
    # This will turn something like "[12/8]- Mens..." into "[12/8] - Mens..."
    header_text = re.sub(r"\](?=-)", "] ", header_text)  # Ensure a space before dash if it follows a bracket
    parts = header_text.split(" - ")
    if len(parts) < 2:
        print("  Could not split header on ' - ', skipping this header.")
        continue

    left_part = parts[0]
    category_part = parts[1]
    print("  Left part (time info):", left_part)
    print("  Category part:", category_part)

    # Extract category abbreviation if present
    cat_abrev = None
    cat_name = category_part
    if "(" in category_part and ")" in category_part:
        start_idx = category_part.index("(")
        end_idx = category_part.index(")")
        cat_abrev = category_part[start_idx+1:end_idx].strip()
        cat_name = category_part[:start_idx].strip()

    race_data["cat_abrev"] = cat_abrev if cat_abrev else cat_name
    print("  cat_abrev:", race_data["cat_abrev"])
    print("  cat_name:", cat_name.strip())

    # Parse race_num, time, date from left_part
    left_parts = left_part.split()
    race_data["race"] = {}
    race_num = None
    race_time = None
    race_day = None

    if left_parts:
        race_num_raw = left_parts[0]
        if race_num_raw.endswith(":"):
            race_num_raw = race_num_raw[:-1]
        race_data["race"]["race_num"] = race_num_raw

    # Attempt to parse time (e.g. "7:30 AM")
    if len(left_parts) >= 3:
        race_time = left_parts[1] + " " + left_parts[2]
        race_data["time"] = race_time
        race_data["race"]["race_time"] = race_time

    # Parse date from something like "[MM/DD]"
    for token in left_parts:
        if token.startswith("[") and token.endswith("]"):
            raw_race_date = token.strip("[]")
            dparts = raw_race_date.split("/")
            if len(dparts) == 2:
                if "start_date" in info and len(info["start_date"].split("-")) == 3:
                    eyear = info["start_date"].split("-")[0]
                else:
                    eyear = "2024"
                month = dparts[0].zfill(2)
                day = dparts[1].zfill(2)
                race_day = f"{eyear}-{month}-{day}"
                race_data["date"] = raw_race_date
                race_data["race"]["race_day"] = race_day

    print("  race_num:", race_num)
    print("  race_time:", race_time)
    print("  race_day:", race_day)

    # Try to get sub_type or "Flight" info from next h5
    sub_header = header.find_next("h5")
    if sub_header:
        sub_type_text = sub_header.get_text(strip=True)
        if sub_type_text:
            race_data["race"]["sub_type"] = sub_type_text
            print("  sub_type (Flight):", sub_type_text)
    else:
        print("  No h5 (Flight info) found after this header.")

    # Set category object
    race_data["category"] = {"name": cat_name.strip()} if cat_name else {}
    print("  category name:", race_data["category"]["name"])

    # Find the corresponding table
    table = header.find_next("table")
    if not table:
        print("  No table found after this header.")
    else:
        print("  Table found after this header.")
    results = []
    if table:
        tbody = table.find("tbody", class_="result-body")
        if not tbody:
            print("  No tbody with class 'result-body' found in the table.")
        else:
            rows = tbody.find_all("tr")
            print("  Found", len(rows), "rows in result-body.")

            for row_index, row in enumerate(rows, start=1):
                ths = row.find_all("th")
                tds = row.find_all("td")
                if len(ths) == 1 and len(tds) == 1:
                    lane_number = ths[0].get_text(strip=True)
                    competitor_data = {}
                    
                    strong_tag = tds[0].find("strong")
                    if strong_tag:
                        competitor_name_short = strong_tag.get_text(strip=True)
                        competitor_data["name_short"] = competitor_name_short
                    
                    # Use the br tag to get the name_long
                    br_tag = tds[0].find("br")
                    if br_tag and br_tag.next_sibling:
                        competitor_data["name_long"] = br_tag.next_sibling.strip()
                    
                    result_entry = {
                        "lane_boat_number": lane_number,
                        "competitor": competitor_data
                    }
                    results.append(result_entry)
                else:
                    print("    Row structure not matching expected pattern (1 th and 1 td). Skipping row.")

    if results:
        print("  Results found for this race:", len(results))
        race_data["results"] = results
    else:
        print("  No results found for this race.")

    # Only add to schedule if we have cat_abrev
    if "cat_abrev" in race_data and race_data["cat_abrev"]:
        schedule.append(race_data)
        print("  Race data appended to schedule.")
    else:
        print("  No cat_abrev or empty cat_abrev, not appending to schedule.")

print("\nFinished parsing. Building JSON output...")

output = {
    "info": info,
    "schedule": schedule
}

with open("output.json", "w", encoding="utf-8") as outfile:
    json.dump(output, outfile, indent=2)

print("\nDONE.")
