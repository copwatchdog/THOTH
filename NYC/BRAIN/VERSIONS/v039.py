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

# === NYPDTRIAL Extraction ===
def extract_from_nypdtrial(page):
    logging.info(f"Visiting NYPD Trials: {SITES['NYPDTRIAL']}")
    page.goto(SITES["NYPDTRIAL"], wait_until="networkidle")
    tables = page.query_selector_all("table")
    logging.info(f"Found {len(tables)} tables on the NYPD Trials page")

    scored_tables = [(table, score_table_by_keywords(table, KEYWORDS)) for table in tables]
    selected_tables = [t for t, score in scored_tables if score >= THRESHOLD]
    logging.info(f"Selected {len(selected_tables)} tables with threshold >= {THRESHOLD}")

    records = []
    for ti, table in enumerate(selected_tables, start=1):
        data = extract_table(table)
        logging.info(f"Table #{ti}: extracted {len(data)} rows")
        for ridx, record in enumerate(data, start=1):
            if record.get("Name"):
                parts = record["Name"].split()
                record["First"] = parts[0]
                record["Last"] = " ".join(parts[1:]) if len(parts) > 1 else ""
                logging.info(f"Record #{ridx} parsed Name -> First: '{record['First']}' Last: '{record['Last']}'")
            records.append(record)
    logging.info(f"Total trial records extracted: {len(records)}")
    return records

# === FIFTYA Enrichment (robust extraction of badge, precinct, start, last earned) ===
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
    for oidx, o in enumerate(officers, start=1):
        name_el = o.query_selector("a.name")
        if not name_el:
            continue
        name_text = name_el.inner_text().strip()
        cand_first, cand_last = _split_candidate_name(name_text)
        logging.info(f"50-a: candidate#{oidx} '{name_text}' -> First='{cand_first}' Last='{cand_last}'")
        if _norm(cand_first) == _norm(first) and _norm(cand_last) == _norm(last):
            target_officer = o
            logging.info(f"50-a: exact match found on candidate#{oidx} '{name_text}'")
            break

    if not target_officer:
        logging.warning(f"50-a: no exact name match found for '{officer_name}'. Trying partial-last fallback.")
        # fallback: allow candidate last containing record last (useful for suffixes)
        for oidx, o in enumerate(officers, start=1):
            name_el = o.query_selector("a.name")
            if not name_el:
                continue
            name_text = name_el.inner_text().strip()
            cand_first, cand_last = _split_candidate_name(name_text)
            if _norm(cand_first) == _norm(first) and _norm(cand_last).endswith(_norm(last)):
                target_officer = o
                logging.info(f"50-a: fallback partial match found on candidate#{oidx} '{name_text}'")
                break

    if not target_officer:
        logging.warning(f"50-a: still no match for '{officer_name}'. Skipping 50-a enrichment.")
        return

    # Click profile
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

    # Badge extraction - try multiple selectors then regex fallback
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

    # Precinct link & number - try anchors with precinct keywords then regex fallback
    precinct_link = None
    precinct_number = None
    anchor_selectors = ["div.command a.command", "a[href*='precinct']", "a[href*='pct']", "a[href*='precincts']", "a"]
    for sel in anchor_selectors:
        try:
            el = identity.query_selector(sel)
        except Exception:
            el = None
        if el:
            href = el.get_attribute("href") if el.get_attribute else None
            if href and ("precinct" in href.lower() or "pct" in href.lower() or re.search(r'\d{1,3}', href)):
                precinct_link = href
                m = re.search(r'(\d{1,3})', href)
                if m:
                    precinct_number = int(m.group(1))
                logging.info(f"50-a: precinct link found via selector '{sel}': {precinct_link} number:{precinct_number}")
                break
    if not precinct_link:
        # fallback: try extracting "Precinct N" from identity text
        m = re.search(r'Precinct\s+(\d{1,3})', identity_text, re.I)
        if m:
            precinct_number = int(m.group(1))
            logging.info(f"50-a: precinct number extracted from text: {precinct_number}")
    record["precinct_link"] = precinct_link
    record["precinct_number"] = precinct_number

    # Service start extraction (Service started Month YYYY)
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
        # fallback: look for "Started Month YYYY" phrase
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

    # Last earned extraction - try span.compensation then regex fallback (preserve formatting)
    last_earned = None
    comp_elem = identity.query_selector("span.compensation")
    if comp_elem:
        comp_text = comp_elem.inner_text().strip()
        # try to capture the dollar amount intact
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

    # Discipline / articles
    discipline = identity.query_selector("div.discipline")
    record["has_discipline"] = "Y" if discipline and discipline.query_selector("article.message") else "N"
    news = identity.query_selector("div.news")
    record["has_articles"] = "Y" if news and news.query_selector_all("a") else "N"
    logging.info(f"50-a: has_discipline={record['has_discipline']} has_articles={record['has_articles']}")

    # --- Section 2 summary counts ---
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

    # --- Section 3 lawsuits ---
    lawsuits = page.query_selector("div.lawsuits-details")
    if lawsuits:
        text = lawsuits.inner_text()
        m = re.search(r"Named in (\d+) known lawsuits", text)
        record["num_lawsuits"] = int(m.group(1)) if m else 0
        m2 = re.search(r"\$(\d[\d,]*) total settlements", text)
        record["total_settlements"] = int(m2.group(1).replace(",", "")) if m2 else 0
        logging.info(f"50-a: lawsuits={record.get('num_lawsuits')} settlements={record.get('total_settlements')}")

    logging.info(f"50-a: enrichment complete for '{officer_name}' (badge={record.get('badge')}, pct={record.get('precinct_number')}, started={record.get('service_start')}, last_earned={record.get('last_earned')})")

# === PAYROLL Enrichment (direct match using NYPDTRIAL info) ===
def enrich_with_payroll(page, record):
    first = record.get("First", "")
    last = record.get("Last", "")

    if not first or not last:
        logging.warning("Payroll: Missing First or Last; skipping enrichment")
        return

    query = f"{first},{last}"
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

    service_start_dt = _parse_mm01yyyy(record.get("service_start", ""))
    if service_start_dt:
        logging.info(f"Payroll: using service_start tie-breaker = {service_start_dt.date()}")

    chosen = None  # will be (cells, delta_days) or (cells, None)
    for ridx, row in enumerate(rows, start=1):
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells or len(cells) < 17:
            logging.info(f"Payroll: skipping row #{ridx} (insufficient cells)")
            continue

        year = cells[0]
        last_name = cells[3]
        first_name = cells[4]
        mid_init = cells[5]
        agency_start_date = cells[6]  # MM/DD/YYYY

        logging.info(f"Payroll: row#{ridx} -> year={year}, first='{first_name}', last='{last_name}', agency_start='{agency_start_date}'")

        if _norm(first_name) != _norm(first) or _norm(last_name) != _norm(last):
            logging.info(f"Payroll: row#{ridx} name mismatch (candidate '{first_name} {last_name}')")
            continue

        # Name matches. Determine if it's a good pick using service_start if available.
        if service_start_dt:
            asd = _parse_mmddyyyy(agency_start_date)
            if asd:
                delta_days = abs((asd - service_start_dt).days)
                logging.info(f"Payroll: row#{ridx} matched name; agency_start delta_days={delta_days}")
                if not chosen:
                    chosen = (cells, delta_days)
                    logging.info(f"Payroll: row#{ridx} tentatively chosen (delta {delta_days})")
                else:
                    _, current_delta = chosen
                    if current_delta is None or delta_days < current_delta:
                        chosen = (cells, delta_days)
                        logging.info(f"Payroll: row#{ridx} replaces previous chosen (smaller delta {delta_days} < {current_delta})")
            else:
                logging.info(f"Payroll: row#{ridx} matched name but agency_start_date unparsable; keeping as fallback if none chosen")
                if not chosen:
                    chosen = (cells, None)
        else:
            # No service_start to compare — pick the first exact name match deterministically
            chosen = (cells, None)
            logging.info(f"Payroll: row#{ridx} chosen (first exact name match, no service_start present)")
            break

    if chosen:
        cells, delta = chosen
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
            logging.info(f"Payroll: chosen row year={cells[0]} agency_start={cells[6]} status={cells[9]} - payroll fields updated")
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
    all_records = extract_from_nypdtrial(page)
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

logging.info(f"Writing CSV to {csv_path}")
with csv_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    written = 0
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

logging.info(f"CSV write complete. Rows written: {written}")
