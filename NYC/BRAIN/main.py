import logging
import csv
import re
import os
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError

# === Configuration ===
SITES = {
    "NYPDTRIAL": "https://www.nyc.gov/site/nypd/bureaus/administrative/trials.page",
    "FIFTYA": "https://50-a.org",
    "PAYROLL": "https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e/explore/query/SELECT%0A%20%20%60fiscal_year%60%2C%0A%20%20%60payroll_number%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60last_name%60%2C%0A%20%20%60first_name%60%2C%0A%20%20%60mid_init%60%2C%0A%20%20%60agency_start_date%60%2C%0A%20%20%60work_location_borough%60%2C%0A%20%20%60title_description%60%2C%0A%20%20%60leave_status_as_of_june_30%60%2C%0A%20%20%60base_salary%60%2C%0A%20%20%60pay_basis%60%2C%0A%20%20%60regular_hours%60%2C%0A%20%20%60regular_gross_paid%60%2C%0A%20%20%60ot_hours%60%2C%0A%20%20%60total_ot_paid%60%2C%0A%20%20%60total_other_pay%60%0AWHERE%0A%20%20caseless_one_of%28%0A%20%20%20%20%60agency_name%60%2C%0A%20%20%20%20%22Police%20Department%22%2C%0A%20%20%20%20%22POLICE%20DEPARTMENT%22%0A%20%20%29%0AORDER%20BY%20%60agency_name%60%20ASC%20NULL%20LAST%2C%20%60fiscal_year%60%20DESC%20NULL%20FIRST/page/filter"
}

KEYWORDS = ["Date", "Time", "Rank", "Name", "Trial Room", "Case Type"]
THRESHOLD = 2
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
LOG_FILE = "../../LOGS/thoth.log"

# Generate monthly CSV filename in format YYMM-copwatchdog.csv
current_date = datetime.now()
month_prefix = f"{str(current_date.year)[2:]}{current_date.month:02d}"
CSV_FILE = f"{month_prefix}-copwatchdog.csv"
CSV_DIR = Path("../CSV")  # Output directory for CSV files
LOCAL_CSV_FILE = "copwatchdog.csv"  # Keep a copy in the current directory

# === Setup logging ===
logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("Them Dogs Gonna Get'm")

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
    logging.info(f"Extracted {len(records)} rows from table (headers: {headers})")
    return records

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower()) if s else ""

def _parse_mmddyyyy(s: str):
    try:
        return datetime.strptime(s, "%m/%d/%Y")
    except Exception:
        return None

def _parse_mm01yyyy(s: str):
    try:
        return datetime.strptime(s, "%m/01/%Y")
    except Exception:
        return None

def _split_candidate_name(name_text: str):
    if not name_text:
        return "", ""
    name_text = name_text.strip()
    if "," in name_text:
        last, rest = [p.strip() for p in name_text.split(",", 1)]
        first = rest.split()[0] if rest else ""
        return first, last
    parts = name_text.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

# === Payroll Name Matching Helpers ===
def _strip_suffix(name: str) -> str:
    if not name:
        return ""
    parts = name.lower().split()
    if parts and parts[-1] in SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)

def _match_first_name(source_first: str, candidate_first: str) -> bool:
    if not source_first or not candidate_first:
        logging.info(f"First-name match skipped due to missing value: source='{source_first}', candidate='{candidate_first}'")
        return False
    result = _norm(source_first.split()[0]) == _norm(candidate_first.split()[0])
    logging.info(f"First-name match: source='{source_first}' candidate='{candidate_first}' -> {result}")
    return result

def _match_last_name(source_last: str, candidate_last: str) -> bool:
    if not source_last or not candidate_last:
        logging.info(f"Last-name match skipped due to missing value: source='{source_last}', candidate='{candidate_last}'")
        return False
    source_last_clean = _strip_suffix(source_last)
    candidate_last_clean = _strip_suffix(candidate_last)
    result = (_norm(source_last_clean) in _norm(candidate_last_clean) or
              _norm(candidate_last_clean) in _norm(source_last_clean))
    logging.info(f"Last-name match: source='{source_last}' candidate='{candidate_last}' -> {result}")
    return result

# === NYPDTRIAL Extraction ===
def extract_from_nypdtrial(page, retries=3, timeout=5000):
    logging.info(f"Visiting NYPD Trials: {SITES['NYPDTRIAL']}")
    attempt = 0
    while attempt < retries:
        try:
            page.goto(SITES["NYPDTRIAL"], timeout=timeout, wait_until="networkidle")
            break
        except TimeoutError:
            attempt += 1
            logging.warning(f"NYPD Trials page load timed out (attempt {attempt}/{retries})")
            if attempt >= retries:
                logging.error(f"Failed to load NYPD Trials page after {retries} attempts")
                return []

    tables = page.query_selector_all("table")
    logging.info(f"Found {len(tables)} tables on the NYPD Trials page")

    scored_tables = [(table, score_table_by_keywords(table, KEYWORDS)) for table in tables]
    selected_tables = [t for t, score in scored_tables if score >= THRESHOLD]
    logging.info(f"Selected {len(selected_tables)} tables with threshold >= {THRESHOLD}")

    records = []
    for ti, table in enumerate(selected_tables, start=1):
        data = extract_table(table)
        logging.info(f"Table #{ti}: extracted {len(data)} rows")
        for row_idx, record in enumerate(data, start=1):
            if record.get("Name"):
                parts = record["Name"].split()
                record["First"] = parts[0]
                record["Last"] = " ".join(parts[1:]) if len(parts) > 1 else ""
                logging.info(f"Record #{row_idx} parsed Name -> First: '{record['First']}' Last: '{record['Last']}'")
            records.append(record)
    logging.info(f"Total trial records extracted: {len(records)}")
    return records

# === FIFTYA Enrichment ===
def enrich_with_50a(page, record):
    officer_name = record.get("Name")
    first = record.get("First")
    last = record.get("Last")
    if not officer_name or not first or not last:
        logging.warning("50-a: Missing Name/First/Last; skipping enrichment")
        return

    logging.info(f"50-a: Searching for '{officer_name}' (First='{first}' Last='{last}')")
    try:
        page.goto(SITES["FIFTYA"], wait_until="networkidle")
        logging.info(f"50-a: loaded {page.url}")
        search_input = page.query_selector("#q")
        if not search_input:
            logging.warning("50-a: search input '#q' not found")
            return
        search_input.fill(officer_name)
        search_input.press("Enter")
        page.wait_for_selector(".officer.active", timeout=7000)
    except TimeoutError:
        logging.warning(f"50-a: timeout or no search results for '{officer_name}'")
        return

    officers = page.query_selector_all(".officer.active")
    logging.info(f"50-a: {len(officers)} search results for '{officer_name}'")
    target_officer = None
    for officer_idx, o in enumerate(officers, start=1):
        name_el = o.query_selector("a.name")
        if not name_el:
            continue
        name_text = name_el.inner_text().strip()
        candidate_row_first, candidate_row_last = _split_candidate_name(name_text)
        logging.info(f"50-a: candidate#{officer_idx} '{name_text}' -> First='{candidate_row_first}' Last='{candidate_row_last}'")
        if _norm(candidate_row_first) == _norm(first) and _norm(candidate_row_last) == _norm(last):
            target_officer = o
            logging.info(f"50-a: exact match found on candidate#{officer_idx} '{name_text}'")
            break

    if not target_officer:
        logging.warning(f"50-a: no exact name match found for '{officer_name}'. Trying partial-last fallback.")
        for officer_idx, o in enumerate(officers, start=1):
            name_el = o.query_selector("a.name")
            if not name_el:
                continue
            name_text = name_el.inner_text().strip()
            candidate_row_first, candidate_row_last = _split_candidate_name(name_text)
            if _norm(candidate_row_first) == _norm(first) and _norm(candidate_row_last).endswith(_norm(last)):
                target_officer = o
                logging.info(f"50-a: fallback partial match found on candidate#{officer_idx} '{name_text}'")
                break

    if not target_officer:
        logging.warning(f"50-a: still no match for '{officer_name}'. Skipping 50-a enrichment.")
        return

    try:
        target_officer.query_selector("a.name").click()
        page.wait_for_selector("div.identity", timeout=7000)
        logging.info("50-a: officer profile loaded")
    except TimeoutError:
        logging.warning("50-a: officer profile did not load in time after click")
        return

    identity = page.query_selector("div.identity")
    if not identity:
        logging.warning("50-a: 'div.identity' not found on profile")
        return

    identity_text = identity.inner_text().strip()
    logging.info(f"50-a: identity snippet (first 300 chars): {identity_text[:300]!s}")

    badge = None
    badge_selectors = ["span.badge", ".badge", "span.badge-number", "div.badge"]
    for sel in badge_selectors:
        el = identity.query_selector(sel)
        if el:
            txt = el.inner_text().strip()
            m = re.search(r'(\d+)', txt)
            if m:
                badge = m.group(1)
                logging.info(f"50-a: badge extracted via selector '{sel}': {badge}")
                break
    if not badge:
        m = re.search(r'Badge\s*#?\s*([0-9]+)', identity_text, re.I)
        if m:
            badge = m.group(1)
            logging.info(f"50-a: badge extracted via regex in identity text: {badge}")
    record["badge"] = badge

    precinct_link = None
    precinct_number = None
    anchor_selectors = ["div.command a.command", "a[href*='precinct']", "a[href*='pct']", "a[href*='precincts']", "a"]
    for sel in anchor_selectors:
        el = identity.query_selector(sel)
        if el:
            href = el.get_attribute("href") if el.get_attribute else None
            if href:
                if href.startswith("http"):
                    precinct_link = href
                else:
                    precinct_link = SITES["FIFTYA"].rstrip("/") + "/" + href.lstrip("/")
                m_num = re.search(r'(\d{1,3})', href)
                if m_num:
                    precinct_number = int(m_num.group(1))
                else:
                    m_str = re.search(r'/([A-Za-z0-9\-]+)$', href)
                    if m_str:
                        precinct_number = m_str.group(1)
                break

    if not precinct_link:
        m = re.search(r'Precinct\s+(\d{1,3})', identity_text, re.I)
        if m:
            precinct_number = int(m.group(1))
            logging.info(f"50-a: precinct number extracted from text: {precinct_number}")
    record["precinct_link"] = precinct_link
    record["precinct_number"] = precinct_number

    service_start = None
    m = re.search(r"Service\s+started\s+([A-Za-z]+)\s+(\d{4})", identity_text, re.I)
    if m:
        month_str, year = m.groups()
        try:
            month = datetime.strptime(month_str[:3], "%b").month
            service_start = f"{month:02}/01/{year}"
            logging.info(f"50-a: service_start parsed: {service_start}")
        except Exception:
            service_start = None
    else:
        m2 = re.search(r"Started\s+([A-Za-z]+)\s+(\d{4})", identity_text, re.I)
        if m2:
            month_str, year = m2.groups()
            try:
                month = datetime.strptime(month_str[:3], "%b").month
                service_start = f"{month:02}/01/{year}"
                logging.info(f"50-a: service_start parsed via fallback: {service_start}")
            except Exception:
                service_start = None
    record["service_start"] = service_start

    last_earned = None
    comp_elem = identity.query_selector("span.compensation")
    if comp_elem:
        comp_text = comp_elem.inner_text().strip()
        m = re.search(r'\$[\d,]+(?:\.\d+)?', comp_text)
        if m:
            last_earned = m.group(0)
            logging.info(f"50-a: last_earned extracted from span.compensation: {last_earned}")
        else:
            last_earned = comp_text
            logging.info(f"50-a: last_earned raw extracted from span.compensation: {last_earned}")
    else:
        m = re.search(r'made\s*\$([\d,]+(?:\.\d+)?)', identity_text, re.I)
        if m:
            last_earned = f"${m.group(1)}"
            logging.info(f"50-a: last_earned extracted via regex in identity text: {last_earned}")
    record["last_earned"] = last_earned

    discipline = identity.query_selector("div.discipline")
    record["has_discipline"] = "Y" if discipline and discipline.query_selector("article.message") else "N"
    news = identity.query_selector("div.news")
    record["has_articles"] = "Y" if news and news.query_selector_all("a") else "N"
    logging.info(f"50-a: has_discipline={record['has_discipline']} has_articles={record['has_articles']}")

    summary = page.query_selector("div.container.summary")
    if summary:
        mapping = {
            "Complaints": "num_complaints",
            "Allegations": "num_allegations",
            "Substantiated": "num_substantiated",
            "Substantiated (Charges)": "num_substantiated_charges",
            "Unsubstantiated": "num_unsubstantiated",
            "Within NYPD Guidelines": "num_within_guidelines"
        }
        for div in summary.query_selector_all("div.column div"):
            label_elem = div.query_selector("span.name")
            count_elem = div.query_selector("span.count")
            if label_elem and count_elem:
                label = label_elem.inner_text().strip()
                count_text = count_elem.inner_text().strip()
                try:
                    count = int(count_text)
                except Exception:
                    continue
                if label in mapping:
                    record[mapping[label]] = count
                    logging.info(f"50-a: summary '{label}' -> {count}")

    lawsuits = page.query_selector("div.lawsuits-details")
    if lawsuits:
        text = lawsuits.inner_text()
        m = re.search(r"Named in (\d+) known lawsuits", text)
        record["num_lawsuits"] = int(m.group(1)) if m else 0
        m2 = re.search(r"\$(\d[\d,]*) total settlements", text)
        record["total_settlements"] = int(m2.group(1).replace(",", "")) if m2 else 0
        logging.info(f"50-a: lawsuits={record.get('num_lawsuits')} settlements={record.get('total_settlements')}")

    logging.info(f"50-a: enrichment complete for '{officer_name}' (badge={record.get('badge')}, pct={record.get('precinct_number')}, started={record.get('service_start')}, last_earned={record.get('last_earned')})")

# === PAYROLL Enrichment ===
def enrich_with_payroll(page, record):
    first = record.get("First", "")
    last = record.get("Last", "")

    if not first or not last:
        logging.warning("Payroll: Missing First or Last; skipping enrichment")
        return

    query = f"{first} {last}"
    logging.info(f"Payroll: Searching for '{query}'")
    try:
        page.goto(SITES["PAYROLL"], wait_until="networkidle")
        logging.info(f"Payroll: loaded {page.url}")
        search_input = page.query_selector("input#search-view")
        if not search_input:
            logging.warning("Payroll: search input 'input#search-view' not found")
            return
        search_input.fill(query)
        search_input.press("Enter")
        page.wait_for_selector("table tbody tr", timeout=7000)
    except TimeoutError:
        logging.warning(f"Payroll: timeout or no results for '{query}'")
        return

    rows = page.query_selector_all("table tbody tr")
    logging.info(f"Payroll: found {len(rows)} table rows for '{query}'")
    if not rows:
        logging.warning(f"Payroll: no rows returned for '{query}'")
        return

    current_year = datetime.now().year
    priority_year = str(current_year - 1)
    fallback_year = str(current_year - 2)
    logging.info(f"Payroll: targeting {priority_year} first, then {fallback_year}")

    service_start_dt = _parse_mm01yyyy(record.get("service_start", ""))
    if service_start_dt:
        logging.info(f"Payroll: using service_start tie-breaker = {service_start_dt.date()}")

    chosen = None  # Initialize BEFORE the loop

    for row_idx, row in enumerate(rows, start=1):
        if row_idx > 10 and not chosen:
            logging.info(f"Payroll: reached 5 rows without finding {priority_year}/{fallback_year}, stopping early")
            break

        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells or len(cells) < 17:
            logging.info(f"Payroll: skipping row #{row_idx} (insufficient cells)")
            continue

        year = cells[0]
        last_name = cells[3]
        first_name = cells[4]
        agency_start_date = cells[6]

        logging.info(
            f"Payroll: row#{row_idx} -> year={year}, first='{first_name}', "
            f"last='{last_name}', agency_start='{agency_start_date}'"
        )

        # Only consider two years prior to current
        if year not in (priority_year, fallback_year):
            continue

        # Relaxed normalized name check
        if not _match_last_name(last, last_name):
            logging.info(f"Payroll: row#{row_idx} last-name mismatch (candidate '{first_name} {last_name}')")
            continue

        # === Priority year, choose immediately and stop ===
        if year == priority_year:
            chosen = (cells, None)
            logging.info(f"Payroll: row#{row_idx} is {year}, chosen immediately, breaking loop")
            break

        # === Fallback year, tie-break by delta_days ===
        if year == fallback_year:
            delta_days = None
            if service_start_dt:
                asd = _parse_mmddyyyy(agency_start_date)
                if asd:
                    delta_days = abs((asd - service_start_dt).days)
                    logging.info(f"Payroll: row#{row_idx} matched {fallback_year}; agency_start delta_days={delta_days}")

            if not chosen:
                chosen = (cells, delta_days)
                logging.info(f"Payroll: row#{row_idx} tentatively chosen")
            else:
                _, current_delta = chosen
                if delta_days is not None and (current_delta is None or delta_days < current_delta):
                    chosen = (cells, delta_days)
                    logging.info(
                        f"Payroll: row#{row_idx} replaces previous {fallback_year} (smaller delta {delta_days} < {current_delta})"
                    )

    if chosen:
        cells, _ = chosen
        try:
            payroll_data = {
                "leave_status_as_of_june_30": cells[9],
                "base_salary": cells[10],
                "pay_basis": cells[11],
                "regular_hours": cells[12],
                "regular_gross_paid": cells[13],
                "ot_hours": cells[14],
                "total_ot_paid": cells[15],
                "total_other_pay": cells[16],
            }
            record.update(payroll_data)
            record["Last Earned"] = payroll_data["regular_gross_paid"]
            logging.info(
                f"Payroll: chosen row year={cells[0]} agency_start={cells[6]} status={cells[9]} "
                f"- payroll fields updated"
            )
        except Exception as e:
            logging.warning(f"Payroll: failed to parse chosen row for '{query}': {e}")
    else:
        logging.warning(f"Payroll: no suitable payroll match found for '{query}'")


# === Main Script ===
all_records = []

with sync_playwright() as p:
    logging.info("Launching headless Chromium")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Extract NYPDTRIAL
    all_records = extract_from_nypdtrial(page, retries=3, timeout=5000)
    logging.info(f"Main: extracted {len(all_records)} records from NYPDTRIAL")

    # Enrich with FIFTYA
    logging.info("Main: beginning 50-a enrichment pass")
    for idx, record in enumerate(all_records, start=1):
        logging.info(f"Main: 50-a enrich record #{idx} - {record.get('Name')}")
        enrich_with_50a(page, record)

    # Enrich with PAYROLL
    logging.info("Main: beginning payroll enrichment pass")
    page = context.new_page()
    for idx, record in enumerate(all_records, start=1):
        logging.info(f"Main: payroll enrich record #{idx} - First='{record.get('First')}' Last='{record.get('Last')}'")
        enrich_with_payroll(page, record)

    browser.close()
    logging.info("Browser closed, Dogs returned")

# === Save CSV ===
# Ensure the CSV directory exists
CSV_DIR.mkdir(parents=True, exist_ok=True)
csv_path = CSV_DIR / CSV_FILE
local_csv_path = Path(LOCAL_CSV_FILE)

fieldnames = [
    "Date","Time","Rank","First","Last","Room","Case Type",
    "Badge","PCT","PCT URL","Started","Last Earned",
    "Disciplined","Articles",
    "# Complaints","# Allegations","# Substantiated","# Charges",
    "# Unsubstantiated","# Guidelined",
    "# Lawsuits","Total Settlements",
    "Status","Base Salary","Pay Basis","Regular Hours","Regular Gross Paid","OT Hours","Total OT Paid","Total Other Pay"
]

# Function to write CSV with the same content
def write_csv_file(filepath, records):
    logging.info(f"Writing CSV to {filepath}")
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        written = 0
        for r in records:
            writer.writerow({
                "Date": r.get("Date",""),
                "Time": r.get("Time",""),
                "Rank": r.get("Rank",""),
                "First": r.get("First",""),
                "Last": r.get("Last",""),
                "Room": r.get("Trial Room",""),
                "Case Type": r.get("Case Type",""),
                "Badge": r.get("badge",""),
                "PCT": r.get("precinct_number",""),
                "PCT URL": r.get("precinct_link",""),
                "Started": r.get("service_start",""),
                "Last Earned": r.get("last_earned",""),
                "Disciplined": r.get("has_discipline","N"),
                "Articles": r.get("has_articles","N"),
                "# Complaints": r.get("num_complaints",0),
                "# Allegations": r.get("num_allegations",0),
                "# Substantiated": r.get("num_substantiated",0),
                "# Charges": r.get("num_substantiated_charges",0),
                "# Unsubstantiated": r.get("num_unsubstantiated",0),
                "# Guidelined": r.get("num_within_guidelines",0),
                "# Lawsuits": r.get("num_lawsuits",0),
                "Total Settlements": r.get("total_settlements",0),
                "Status": r.get("leave_status_as_of_june_30",""),
                "Base Salary": r.get("base_salary",""),
                "Pay Basis": r.get("pay_basis",""),
                "Regular Hours": r.get("regular_hours",""),
                "Regular Gross Paid": r.get("regular_gross_paid",""),
                "OT Hours": r.get("ot_hours",""),
                "Total OT Paid": r.get("total_ot_paid",""),
                "Total Other Pay": r.get("total_other_pay",""),
            })
            written += 1
        return written

# Write to both locations
monthly_written = write_csv_file(csv_path, all_records)
local_written = write_csv_file(local_csv_path, all_records)

logging.info(f"CSV write complete. Monthly file ({csv_path}): {monthly_written} rows, Local file ({local_csv_path}): {local_written} rows")
