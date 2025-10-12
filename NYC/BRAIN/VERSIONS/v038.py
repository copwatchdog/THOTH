import logging
import csv
import re
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError

# === Configuration ===
SITES = {
    "NYPDTRIAL": "https://www.nyc.gov/site/nypd/bureaus/administrative/trials.page",
    "FIFTYA": "https://50-a.org",
    "PAYROLL": "https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e/explore/query/SELECT%0A%20%20%60fiscal_year%60%2C%0A%20%20%60payroll_number%60%2C%0A%20%20%60agency_name%60%2C%20%20%60last_name%60%2C%20%20%60first_name%60%2C%20%20%60mid_init%60%2C%20%20%60agency_start_date%60%2C%20%60work_location_borough%60%2C%20%60title_description%60%2C%20%60leave_status_as_of_june_30%60%2C%20%20%60base_salary%60%2C%20%20%60pay_basis%60%2C%20%20%60regular_hours%60%2C%20%20%60regular_gross_paid%60%2C%20%20%60ot_hours%60%2C%20%20%60total_ot_paid%60%2C%20%20%60total_other_pay%60%0AWHERE%20caseless_one_of(%60agency_name%60,%20%22Police%20Department%22)%0AORDER%20BY%20%60agency_name%60%20ASC%20NULL%20LAST,%20%60fiscal_year%60%20DESC%20NULL%20FIRST/page/filter"
}

KEYWORDS = ["Date", "Time", "Rank", "Name", "Trial Room", "Case Type"]
THRESHOLD = 2
LOG_FILE = "copwatchdog.log"
CSV_FILE = "copwatchdog.csv"

# === Setup logging ===
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

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower()) if s else ""

def _normalize_rank(r: str) -> str:
    if not r:
        return ""
    r = r.lower().strip().replace(".", "")
    mapping = {
        "det": "detective",
        "detective": "detective",
        "po": "police officer",
        "officer": "police officer",
        "police officer": "police officer",
        "sgt": "sergeant",
        "sergeant": "sergeant",
        "lt": "lieutenant",
        "lieutenant": "lieutenant",
        "capt": "captain",
        "captain": "captain",
    }
    return mapping.get(r, r)

def _extract_rank_text(command_text: str) -> str:
    # e.g., "Police Officer at Quartermaster Section since April 2024"
    if not command_text:
        return ""
    before_at = command_text.split(" at ", 1)[0]
    return before_at.strip().lower()

def _parse_display_name(name_text: str):
    # returns (first, last)
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
    return parts[0], parts[-1]

def _name_match_score(candidate_name: str, rec_first: str, rec_last: str) -> int:
    cf, cl = _parse_display_name(candidate_name)
    score = 0
    if _norm(cl) and _norm(cl) == _norm(rec_last):
        score += 4
    if _norm(cf) and _norm(cf) == _norm(rec_first):
        score += 3
    elif cf and rec_first and cf[0].lower() == rec_first[0].lower():
        score += 1
    return score

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

# === NYPDTRIAL Extraction ===
def extract_from_nypdtrial(page):
    logging.info(f"Visiting site: {SITES['NYPDTRIAL']}")
    page.goto(SITES["NYPDTRIAL"], wait_until="networkidle")
    tables = page.query_selector_all("table")
    logging.info(f"Found {len(tables)} tables on the page")
    scored_tables = [(table, score_table_by_keywords(table, KEYWORDS)) for table in tables]
    selected_tables = [t for t, score in scored_tables if score >= THRESHOLD]
    logging.info(f"{len(selected_tables)} tables selected above threshold {THRESHOLD}")

    records = []
    for table in selected_tables:
        data = extract_table(table)
        for record in data:
            if record.get("Name"):
                parts = record["Name"].split()
                record["First"] = parts[0]
                record["Last"] = " ".join(parts[1:]) if len(parts) > 1 else ""
            records.append(record)
    return records

# === FIFTYA Enrichment (Full three-section + robust officer selection) ===
def enrich_with_50a(page, record):
    officer_name = record.get("Name")
    officer_rank = record.get("Rank")
    if not officer_name or not officer_rank:
        logging.warning("Missing Name or Rank; skipping 50-a enrichment")
        return

    logging.info(f"Searching 50-a.org for officer {officer_name} ({officer_rank})")
    try:
        page.goto(SITES["FIFTYA"], wait_until="networkidle")
        search_input = page.query_selector("#q")
        if not search_input:
            logging.warning("50-a search box not found")
            return
        search_input.fill(officer_name)
        search_input.press("Enter")
        page.wait_for_selector(".officer.active", timeout=7000)
    except TimeoutError:
        logging.warning(f"No officers found matching {officer_name}")
        return

    # Robust selection: prioritize name match, lightly weight rank if available
    officers = page.query_selector_all(".officer.active")
    target_officer = None
    best_score = -1
    want_rank = _normalize_rank(officer_rank)
    for o in officers:
        name_el = o.query_selector("a.name")
        cmd_el = o.query_selector(".command")
        if not name_el:
            continue
        name_text = name_el.inner_text().strip()
        score = _name_match_score(name_text, record.get("First", ""), record.get("Last", ""))

        # light rank boost (do not require rank to match)
        rank_text = _extract_rank_text(cmd_el.inner_text().strip()) if cmd_el else ""
        if want_rank and want_rank in rank_text:
            score += 1

        if score > best_score:
            best_score = score
            target_officer = o

    if not target_officer:
        logging.warning(f"No candidate officer chosen for {officer_name}")
        return

    target_officer.query_selector("a.name").click()
    try:
        page.wait_for_selector("div.identity", timeout=7000)
    except TimeoutError:
        logging.warning(f"Officer profile did not load for {officer_name}")
        return

    # --- Section 1 ---
    identity = page.query_selector("div.identity")
    if identity:
        record["profile_url"] = page.url
        badge_elem = identity.query_selector("span.badge")
        record["badge"] = badge_elem.inner_text().replace("Badge #", "").strip() if badge_elem else None

        cmd_elem = identity.query_selector("div.command a.command")
        if cmd_elem:
            href = cmd_elem.get_attribute("href")
            record["precinct_link"] = href
            match = re.search(r'\d+', href or "")
            record["precinct_number"] = int(match.group()) if match else None

        service_elem = identity.query_selector("div.service")
        if service_elem:
            text = service_elem.inner_text()
            m = re.search(r"Service\s+started\s+([A-Za-z]+)\s+(\d{4})", text)
            if m:
                month_str, year = m.groups()
                month = datetime.strptime(month_str[:3], "%b").month
                record["service_start"] = f"{month:02}/01/{year}"
            comp_elem = service_elem.query_selector("span.compensation")
            if comp_elem:
                comp_text = comp_elem.inner_text().replace("made $", "").replace(",", "")
                try:
                    record["last_earned"] = int(comp_text.split()[0])
                except Exception:
                    record["last_earned"] = None

        discipline = identity.query_selector("div.discipline")
        record["has_discipline"] = "Y" if discipline and discipline.query_selector("article.message") else "N"

        news = identity.query_selector("div.news")
        record["has_articles"] = "Y" if news and news.query_selector_all("a") else "N"

    # --- Section 2 ---
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

    # --- Section 3 ---
    lawsuits = page.query_selector("div.lawsuits-details")
    if lawsuits:
        text = lawsuits.inner_text()
        m = re.search(r"Named in (\d+) known lawsuits", text)
        record["num_lawsuits"] = int(m.group(1)) if m else 0
        m = re.search(r"\$(\d[\d,]*) total settlements", text)
        record["total_settlements"] = int(m.group(1).replace(",", "")) if m else 0

    logging.info(f"Updated record with 50-a.org full info: {record}")

# === PAYROLL Enrichment (robust row selection) ===
def enrich_with_payroll(page, record):
    first = record.get("First", "")
    last = record.get("Last", "")

    if not first or not last:
        logging.warning("Missing First or Last; skipping payroll enrichment")
        return

    query = f"{first},{last}"
    logging.info(f"Searching payroll for {query}")

    try:
        page.goto(SITES["PAYROLL"], wait_until="networkidle")
        search_input = page.query_selector("input#search-view")
        if not search_input:
            logging.warning("Payroll search box not found")
            return
        search_input.fill(query)
        search_input.press("Enter")
        page.wait_for_selector("table tbody tr", timeout=7000)
    except TimeoutError:
        logging.warning(f"No payroll data found for {query}")
        return

    rows = page.query_selector_all("table tbody tr")
    if not rows:
        logging.warning(f"No payroll rows for {query}")
        return

    # Prepare service_start date if available for tie-breaking
    service_start_dt = _parse_mm01yyyy(record.get("service_start", ""))

    best = None
    best_score = -1
    for row in rows:
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells or len(cells) < 16:
            continue

        year = cells[0]
        agency_name = cells[2]
        last_name = cells[3]
        first_name = cells[4]
        mid_init = cells[5]
        agency_start_date = cells[6]  # MM/DD/YYYY
        title_description = cells[8]

        score = 0
        if _norm(last_name) == _norm(last):
            score += 3
        if _norm(first_name) == _norm(first):
            score += 2
        elif first_name and first and first_name[0].lower() == first[0].lower():
            score += 1

        if year == "2024":
            score += 3
        if agency_name and "police department" in agency_name.lower():
            score += 1

        # Optional gentle boost if title contains normalized rank
        want_rank = _normalize_rank(record.get("Rank", ""))
        if want_rank and want_rank in title_description.lower():
            score += 1

        # Start date proximity (within ~1 year)
        if service_start_dt:
            asd = _parse_mmddyyyy(agency_start_date)
            if asd:
                delta_days = abs((asd - service_start_dt).days)
                if delta_days <= 400:
                    score += 2

        if score > best_score:
            best_score = score
            best = cells

    if best:
        try:
            payroll_data = {
                "leave_status_as_of_june_30": best[9],
                "base_salary": best[10],
                "pay_basis": best[11],
                "regular_hours": best[12],
                "regular_gross_paid": best[13],
                "ot_hours": best[14],
                "total_ot_paid": best[15],
                "total_other_pay": best[16],
            }
            record.update(payroll_data)
            record["Last Earned"] = payroll_data["regular_gross_paid"]
            logging.info(f"Updated record with payroll info: {record}")
        except Exception as e:
            logging.warning(f"Failed to parse payroll row for {query}: {e}")
    else:
        logging.warning(f"No suitable payroll match for {query}")

# === Main Script ===
all_records = []

with sync_playwright() as p:
    logging.info("Launching headless Chromium")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Extract NYPDTRIAL
    all_records = extract_from_nypdtrial(page)

    # Enrich with FIFTYA
    for record in all_records:
        enrich_with_50a(page, record)

    # Enrich with PAYROLL
    page = context.new_page()
    for record in all_records:
        enrich_with_payroll(page, record)

    browser.close()
    logging.info("Browser closed, script finished")

# === Save CSV ===
csv_path = Path(CSV_FILE)
fieldnames = [
    "Date","Time","Rank","First","Last","Room","Case Type",
    "Badge","PCT","PCT URL","Started","Last Earned",
    "Disciplined","Articles",
    "# Complaints","# Allegations","# Substantiated","# Charges",
    "# Unsubstantiated","# Guidelined",
    "# Lawsuits","Total Settlements",
    "Status","Base Salary","Pay Basis","Regular Hours","Regular Gross Paid","OT Hours","Total OT Paid","Total Other Pay"
]

with csv_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in all_records:
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
            "Status": r.get("leave_status_as_of_june_30",0),
            "Base Salary": r.get("base_salary",""),
            "Pay Basis": r.get("pay_basis",""),
            "Regular Hours": r.get("regular_hours",""),
            "Regular Gross Paid": r.get("regular_gross_paid",""),
            "OT Hours": r.get("ot_hours",""),
            "Total OT Paid": r.get("total_ot_paid",""),
            "Total Other Pay": r.get("total_other_pay",""),
        })
