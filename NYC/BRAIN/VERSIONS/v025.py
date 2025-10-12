import logging
import csv
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError

# === Configuration ===
TRIALS_SITE = "https://www.nyc.gov/site/nypd/bureaus/administrative/trials.page"
FIFTY_A_SITE = "https://50-a.org"
PAYROLL_SITE = "https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e/explore/query/SELECT%0A%20%20%60fiscal_year%60%2C%0A%20%20%60payroll_number%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60last_name%60%2C%0A%20%20%60first_name%60%2C%0A%20%20%60mid_init%60%2C%0A%20%20%60agency_start_date%60%2C%20%60work_location_borough%60%2C%20%60title_description%60%2C%20%60leave_status_as_of_june_30%60%2C%20%60base_salary%60%2C%20%60pay_basis%60%2C%20%60regular_hours%60%2C%20%60regular_gross_paid%60%2C%20%60ot_hours%60%2C%20%60total_ot_paid%60%2C%20%60total_other_pay%60%0AWHERE%20caseless_one_of(%60agency_name%60,%20%22Police%20Department%22)%0AORDER%20BY%20%60agency_name%60%20ASC%20NULL%20LAST,%20%60fiscal_year%60%20DESC%20NULL%20FIRST/page/filter"
KEYWORDS = ["Date", "Time", "Rank", "Name", "Trial Room", "Case Type"]
THRESHOLD = 2
LOG_FILE = "copwatchdog.log"
CSV_FILE = "copwatchdog.csv"

# === Setup logging (overwrite each run) ===
logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("Script started")

# === Helper Functions ===
def score_table_by_keywords(table, keywords):
    points = 0
    headers = [th.inner_text().strip() for th in table.query_selector_all("th")]
    for kw in keywords:
        if any(kw.lower() in h.lower() for h in headers):
            points += 1
    for row in table.query_selector_all("tr"):
        for cell in row.query_selector_all("td"):
            text = cell.inner_text().strip()
            for kw in keywords:
                if kw.lower() in text.lower():
                    points += 0.5
    logging.info(f"Table scored {points} points based on keywords")
    return points

def extract_table(table):
    headers = [th.inner_text().strip() for th in table.query_selector_all("th")]
    records = []
    for row in table.query_selector_all("tr"):
        cells = row.query_selector_all("td")
        if not cells:
            continue
        record = {}
        for idx, cell in enumerate(cells):
            label = headers[idx] if idx < len(headers) else f"column_{idx}"
            record[label] = cell.inner_text().strip()
        records.append(record)
    logging.info(f"Extracted {len(records)} rows from table")
    return records

def enrich_with_50a(page, record):
    officer_name = record.get("Name")
    officer_rank = record.get("Rank")
    if not officer_name or not officer_rank:
        logging.warning("Missing Name or Rank; skipping 50-a enrichment")
        return

    logging.info(f"Searching 50-a.org for officer {officer_name} ({officer_rank})")
    try:
        page.goto(FIFTY_A_SITE, wait_until="networkidle")
        search_input = page.query_selector("#q")
        if not search_input:
            logging.warning("50-a search box not found")
            return
        search_input.fill(officer_name)
        search_input.press("Enter")
        page.wait_for_selector(".officer.active", timeout=5000)
    except TimeoutError:
        logging.warning(f"No officers found matching {officer_name}")
        return

    officers = page.query_selector_all(".officer.active")
    target_officer = None
    for o in officers:
        rank_elem = o.query_selector(".command")
        if not rank_elem:
            continue
        if officer_rank.lower() in rank_elem.inner_text().strip().lower():
            target_officer = o
            break

    if not target_officer:
        logging.warning(f"No matching officer found for {officer_name} ({officer_rank})")
        return

    target_officer.query_selector("a.name").click()
    try:
        page.wait_for_selector(".container.summary", timeout=5000)
    except TimeoutError:
        logging.warning(f"Officer page did not load summary for {officer_name}")
        return

    summary = page.query_selector(".container.summary")
    record.update({
        "Allegations": summary.query_selector(".allegations .count").inner_text() if summary.query_selector(".allegations .count") else "0",
        "Lawsuits": summary.query_selector(".complaints .count").inner_text() if summary.query_selector(".complaints .count") else "0",
        "Settlement": "0",
        "Last Earned": "0",
    })
    logging.info(f"Updated record with 50-a.org info: {record}")

def enrich_with_payroll(page, record):
    first = record.get("First", "")
    last = record.get("Last", "")
    rank = record.get("Rank", "")

    if not first or not last or not rank:
        logging.warning("Missing First, Last, or Rank; skipping payroll enrichment")
        return

    query = f"{first},{last}"
    logging.info(f"Searching payroll for {query} ({rank})")

    try:
        page.goto(PAYROLL_SITE, wait_until="networkidle")
        search_input = page.query_selector("input#search-view")
        if not search_input:
            logging.warning("Payroll search box not found")
            return
        search_input.fill(query)
        search_input.press("Enter")
        page.wait_for_selector("table", timeout=5000)
    except TimeoutError:
        logging.warning(f"No payroll data found for {query}")
        return

    rows = page.query_selector_all("table tbody tr")
    if not rows:
        logging.warning(f"No payroll rows for {query}")
        return

    payroll_data = None
    for row in rows:
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells or len(cells) < 5:
            continue
        year = cells[0]
        title_description = cells[8] if len(cells) > 8 else ""
        if year == "2024" and rank.lower() in title_description.lower():
            payroll_data = {
                "base_salary": cells[10],
                "regular_gross_paid": cells[12],
                "ot_hours": cells[13],
                "total_ot_paid": cells[14],
                "total_other_pay": cells[15],
            }
            break

    if payroll_data:
        record.update(payroll_data)
        record["Last Earned"] = payroll_data["regular_gross_paid"]
        logging.info(f"Updated record with payroll info: {record}")
    else:
        logging.warning(f"No 2024 payroll match for {query} ({rank})")

# === Main Script ===
all_records = []

with sync_playwright() as p:
    logging.info("Launching headless Chromium")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # --- Extract from NYC.gov trials page ---
    logging.info(f"Visiting site: {TRIALS_SITE}")
    page.goto(TRIALS_SITE, wait_until="networkidle")
    tables = page.query_selector_all("table")
    logging.info(f"Found {len(tables)} tables on the page")
    scored_tables = [(table, score_table_by_keywords(table, KEYWORDS)) for table in tables]
    selected_tables = [t for t, score in scored_tables if score >= THRESHOLD]
    logging.info(f"{len(selected_tables)} tables selected above threshold {THRESHOLD}")

    for table in selected_tables:
        data = extract_table(table)
        for record in data:
            # --- Split name early so enrichment functions can use it ---
            if record.get("Name"):
                parts = record["Name"].split()
                record["First"] = parts[0]
                record["Last"] = " ".join(parts[1:]) if len(parts) > 1 else ""
            logging.info(f"Record extracted: {record}")
            all_records.append(record)

    # --- Enrich records with 50-a.org ---
    for record in all_records:
        enrich_with_50a(page, record)

    # --- Enrich records with payroll ---
    page = context.new_page()
    for record in all_records:
        enrich_with_payroll(page, record)

    logging.info("All records processed, browser closing")
    browser.close()
    logging.info("Browser closed, script finished")

# === Structure and save CSV ===
csv_path = Path(CSV_FILE)
structured_records = []

for r in all_records:
    structured_records.append({
        "Date": r.get("Date",""),
        "Time": r.get("Time",""),
        "Rank": r.get("Rank",""),
        "First": r.get("First",""),
        "Last": r.get("Last",""),
        "Room": r.get("Trial Room",""),
        "Case Type": r.get("Case Type",""),
        "Allegations": r.get("Allegations","0"),
        "Lawsuits": r.get("Lawsuits","0"),
        "Settlement": r.get("Settlement","0"),
        "Last Earned": r.get("Last Earned","0"),
        "Base Salary": r.get("base_salary",""),
        "Regular Gross Paid": r.get("regular_gross_paid",""),
        "OT Hours": r.get("ot_hours",""),
        "Total OT Paid": r.get("total_ot_paid",""),
        "Total Other Pay": r.get("total_other_pay",""),
    })

# --- Write CSV ---
with csv_path.open("w", newline="", encoding="utf-8") as f:
    fieldnames = ["Date","Time","Rank","First","Last","Room","Case Type",
                  "Allegations","Lawsuits","Settlement","Last Earned",
                  "Base Salary","Regular Gross Paid","OT Hours","Total OT Paid","Total Other Pay"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(structured_records)

logging.info(f"Saved {len(structured_records)} records to CSV: {CSV_FILE}")

# --- Output ---
for r in structured_records:
    print(r)
