import logging
import csv
import re
import os
import gc
import atexit
import random
import sys
import argparse
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError

import psycopg2
from psycopg2.extras import RealDictCursor

# === Configuration ===
SITES = {
    "NYPDTRIAL": "https://www.nyc.gov/site/nypd/bureaus/administrative/trials.page",
    "FIFTYA": "https://50-a.org",
    "PAYROLL": "https://data.cityofnewyork.us/City-Government/Citywide-Payroll-Data-Fiscal-Year-/k397-673e/explore/query/SELECT%0A%20%20%60fiscal_year%60%2C%0A%20%20%60payroll_number%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60last_name%60%2C%0A%20%20%60first_name%60%2C%0A%20%20%60mid_init%60%2C%0A%20%20%60agency_start_date%60%2C%0A%20%20%60work_location_borough%60%2C%0A%20%20%60title_description%60%2C%0A%20%20%60leave_status_as_of_june_30%60%2C%0A%20%20%60base_salary%60%2C%0A%20%20%60pay_basis%60%2C%0A%20%20%60regular_hours%60%2C%0A%20%20%60regular_gross_paid%60%2C%0A%20%20%60ot_hours%60%2C%0A%20%20%60total_ot_paid%60%2C%0A%20%20%60total_other_pay%60%0AWHERE%0A%20%20caseless_one_of%28%0A%20%20%20%20%60agency_name%60%2C%0A%20%20%20%20%22Police%20Department%22%2C%0A%20%20%20%20%22POLICE%20DEPARTMENT%22%0A%20%20%29%0AORDER%20BY%20%60agency_name%60%20ASC%20NULL%20LAST%2C%20%60fiscal_year%60%20DESC%20NULL%20FIRST/page/filter"
}

KEYWORDS = ["Date", "Time", "Rank", "Name", "Trial Room", "Case Type"]
THRESHOLD = 2
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

# Set up paths - uses dynamic resolution to work in any directory location
# Supports both direct execution and HERMES_DIR environment variable override
THOTH_ROOT = os.getenv("THOTH_ROOT") or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOGS_DIR = os.path.join(THOTH_ROOT, "LOGS")
os.makedirs(LOGS_DIR, exist_ok=True)

THOTH_LOG = os.path.join(LOGS_DIR, "thoth.log")

# CSV configuration - filename will be generated after extracting trial dates
CSV_DIR = Path("../CSV")  # Output directory for CSV files
LOCAL_CSV_FILE = "copwatchdog.csv"  # Keep a copy in the current directory

# Payroll cache to avoid re-querying same officer
_payroll_cache = {}

# === Parse Command Line Arguments ===
# Parse args BEFORE setting up logging so we can determine the log mode
parser = argparse.ArgumentParser(description="THOTH: CopWatchDog scraper with incremental re-scrape support")
parser.add_argument(
    "--rescrape-list",
    type=str,
    help="Path to CSV file containing officers to re-scrape (must have First, Last, Badge columns)"
)
parser.add_argument(
    "--enrich-mode",
    type=str,
    help="Path to delta enrichment CSV (source_id,column_name,current_value,priority) for targeted field extraction"
)
parser.add_argument(
    "--version-tag",
    type=str,
    help="Override version tag for re-scrape CSV filename (e.g., '2509' for September 2025)"
)
args = parser.parse_args()

# Determine operation mode
rescrape_mode = args.rescrape_list is not None
enrich_mode = args.enrich_mode is not None
rescrape_targets = []
enrich_targets = {}  # Dict: source_id -> {columns: [], priority: str}
override_version_tag = args.version_tag

# === Setup logging ===
# Standalone mode: overwrite log (fresh start)
# Rescrape/Enrich mode: append to log (continue HERMES workflow)
log_mode = "a" if (rescrape_mode or enrich_mode) else "w"
logging.basicConfig(
    filename=THOTH_LOG,
    filemode=log_mode,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("Them Dogs Gonna Get'm")
if enrich_mode:
    logging.info("=== ENRICH MODE: Appending to existing log ===")
elif rescrape_mode:
    logging.info("=== RESCRAPE MODE: Appending to existing log ===")
else:
    logging.info("=== STANDALONE MODE: Fresh log started ===")

if enrich_mode:
    logging.info(f"ENRICH MODE: Loading delta enrichment list from {args.enrich_mode}")
    try:
        with open(args.enrich_mode, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                source_id = row.get('source_id', '').strip()
                column_name = row.get('column_name', '').strip()
                priority = row.get('priority', 'low').strip()
                
                # Parse source_id to extract identifying info (format: YYMM-badge)
                # e.g., "2512-12345" -> version_tag=2512, badge=12345
                if '-' in source_id:
                    version_tag, badge = source_id.split('-', 1)
                else:
                    logging.warning(f"ENRICH MODE: Malformed source_id: {source_id}, skipping")
                    continue
                
                # Group by source_id
                if source_id not in enrich_targets:
                    enrich_targets[source_id] = {
                        'columns': [],
                        'priority': priority,
                        'badge': badge,
                        'version_tag': version_tag
                    }
                
                # Add column to target list for this officer
                if column_name not in enrich_targets[source_id]['columns']:
                    enrich_targets[source_id]['columns'].append(column_name)
        
        logging.info(f"ENRICH MODE: Loaded {len(enrich_targets)} officers with {sum(len(t['columns']) for t in enrich_targets.values())} fields to enrich")
    except Exception as e:
        logging.error(f"ENRICH MODE: Failed to load enrichment list: {e}")
        sys.exit(1)
elif rescrape_mode:
    logging.info(f"RESCRAPE MODE: Loading target list from {args.rescrape_list}")
    try:
        with open(args.rescrape_list, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Extract identifying info
                target = {
                    'first_name': row.get('first_name', '').strip(),
                    'last_name': row.get('last_name', '').strip(),
                    'badge': row.get('badge', '').strip(),
                    'source_id': row.get('source_id', '').strip()
                }
                rescrape_targets.append(target)
        logging.info(f"RESCRAPE MODE: Loaded {len(rescrape_targets)} officers to re-scrape")
    except Exception as e:
        logging.error(f"RESCRAPE MODE: Failed to load target list: {e}")
        sys.exit(1)
else:
    logging.info("FULL SCRAPE MODE: Extracting all officers from NYPD Trials page")

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
    """
    Normalize a string by converting to lowercase and removing all non-alphanumeric characters.
    
    Args:
        s: The string to normalize
        
    Returns:
        Normalized string (lowercase with only alphanumeric chars)
    """
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
    """
    Parse a name string into first and last name components.
    
    Handles both "Last, First" and "First Last" formats.
    
    Args:
        name_text: A string containing a person's name
        
    Returns:
        Tuple of (first_name, last_name)
    """
    if not name_text:
        return "", ""
    name_text = name_text.strip()
    if "," in name_text:
        # Handle "Last, First" format
        last, rest = [p.strip() for p in name_text.split(",", 1)]
        first = rest.split()[0] if rest else ""
        return first, last
    parts = name_text.split()
    if len(parts) == 1:
        # Only one name part provided
        return parts[0], ""
    # Assume "First Last" format
    return parts[0], " ".join(parts[1:])


def _extract_initial(name_text: str) -> str:
    """
    Extract a single-letter middle initial from a name string, if present.

    Examples:
      'Harrison, Lenita I.' -> 'I'
      'Lenita I. Harrison' -> 'I'

    Returns uppercase initial or empty string when not found.
    """
    if not name_text:
        return ""
    # Look for a single letter followed by a period (common form)
    m = re.search(r"\b([A-Za-z])\.\b", name_text)
    if m:
        return m.group(1).upper()
    # Fallback: single letter at the end or between names without a dot
    m2 = re.search(r"\b([A-Za-z])\b(?=[^A-Za-z]*$)", name_text)
    if m2 and len(m2.group(1)) == 1:
        return m2.group(1).upper()
    return ""

def _parse_precinct_desc(precinct_desc):
    """
    Parse precinct description into three components:
    - Current assignment (precinct/unit name)
    - Current assignment start date
    - Previous assignments (comma-separated list)
    
    Example input: "Quartermaster Section since April 2024 Also served at Housing Bureau, Patrol Services Bureau, Transit Bureau"
    
    Args:
        precinct_desc: Raw precinct description string (can be None)
    
    Returns:
        Tuple of (current_assignment, assignment_start, previous_assignments)
    """
    if not precinct_desc:
        return (None, None, None)
    
    current_assignment = None
    assignment_start = None
    previous_assignments = None
    
    # Pattern: "Unit Name since Month Year Also served at Previous1, Previous2, Previous3"
    # Look for "since" pattern to extract current assignment and start date
    since_match = re.search(r'^(.+?)\s+since\s+([A-Za-z]+\s+\d{4})', precinct_desc, re.I)
    if since_match:
        current_assignment = since_match.group(1).strip()
        assignment_start = since_match.group(2).strip()
    else:
        # No "since" found - treat entire string as current assignment
        current_assignment = precinct_desc.strip()
    
    # Look for "Also served at" pattern to extract previous assignments
    also_match = re.search(r'Also served at\s+(.+)$', precinct_desc, re.I)
    if also_match:
        previous_assignments = also_match.group(1).strip()
        # If we found "Also served at", remove it from current_assignment if it's there
        if current_assignment and "Also served at" in current_assignment:
            current_assignment = re.sub(r'\s*Also served at.+$', '', current_assignment, flags=re.I).strip()
    
    logging.debug(f"Parsed precinct: current='{current_assignment}', start='{assignment_start}', previous='{previous_assignments}'")
    return (current_assignment, assignment_start, previous_assignments)

def _parse_article_html(article_element):
    """
    Parse article data from a 50-a.org news anchor element.
    
    Expected HTML structure in div.news:
    <a href="url">Title</a>, Source, Date<br>
    
    The source and date are TEXT SIBLINGS of the anchor, not inside it.
    
    Args:
        article_element: Playwright element handle for anchor tag
        
    Returns:
        Dict with keys: title, source, date_published, url (or None if parsing fails)
    """
    try:
        # Element should be an anchor tag
        tag_name = article_element.evaluate("el => el.tagName").lower()
        
        if tag_name != "a":
            # Not an anchor, skip
            logging.debug(f"Article parser: skipping non-anchor element ({tag_name})")
            return None
        
        # Extract URL and title from anchor
        url = article_element.get_attribute("href")
        title = article_element.inner_text().strip()
        
        if not url or not title:
            logging.debug(f"Article parser: missing url or title (url={url}, title={title})")
            return None
        
        # Get the text that follows the anchor (sibling text nodes)
        # This contains: ", Source, Date"
        try:
            # Use JavaScript to get the next sibling text content
            sibling_text = article_element.evaluate("""
                (anchor) => {
                    let text = '';
                    let node = anchor.nextSibling;
                    // Collect text until we hit a <br> or another anchor
                    while (node && node.nodeName !== 'BR' && node.nodeName !== 'A') {
                        if (node.nodeType === 3) { // Text node
                            text += node.textContent;
                        }
                        node = node.nextSibling;
                    }
                    return text.trim();
                }
            """)
        except Exception as e:
            logging.debug(f"Article parser: failed to extract sibling text: {e}")
            sibling_text = ""
        
        source = None
        date_published = None
        
        # Parse sibling text: ", Source, Date"
        if sibling_text:
            # Remove leading comma and whitespace
            sibling_text = sibling_text.lstrip(", ").strip()
            
            # Split by comma to get [Source, Date]
            parts = [p.strip() for p in sibling_text.split(",")]
            
            if len(parts) >= 1:
                source = parts[0]
            if len(parts) >= 2:
                date_published = parts[1]
        
        logging.debug(f"Article parsed: title='{title}', source='{source}', date='{date_published}', url='{url}'")
        
        return {
            "title": title,
            "source": source,
            "date_published": date_published,
            "url": url
        }
    except Exception as e:
        logging.warning(f"Article parser: failed to parse article element: {e}")
        return None

def _generate_csv_filename(records, override_version_tag=None):
    """
    Generate CSV filename based on trial dates.
    Format: YYMM-copwatchdog.csv (monthly version tag)
    
    Uses the most recent (latest) trial date found in the records for the YYMM prefix.
    Falls back to current date if no valid dates are found.
    
    In rescrape mode, uses the same filename as initial scrape (not _rescrape suffix)
    to merge updates into the existing monthly CSV.
    
    Args:
        records: List of trial records containing 'Date' field
        override_version_tag: Optional version tag to use (for re-scrapes)
        
    Returns:
        String filename in format YYMM-copwatchdog.csv
    """
    # If override provided (for re-scrapes), use the same base filename (no _rescrape suffix)
    if override_version_tag:
        filename = f"{override_version_tag}-copwatchdog.csv"
        logging.info(f"CSV filename: Using override version_tag {override_version_tag} → {filename}")
        return filename
    
    latest_date = None
    
    for record in records:
        date_str = record.get("Date", "")
        if not date_str:
            continue
        
        # Try parsing the date (assuming format like "11/3/2025" or "M/D/YYYY")
        try:
            trial_date = datetime.strptime(date_str, "%m/%d/%Y")
            if latest_date is None or trial_date > latest_date:
                latest_date = trial_date
        except ValueError:
            # Try other common formats
            for fmt in ["%m-%d-%Y", "%Y-%m-%d", "%d/%m/%Y"]:
                try:
                    trial_date = datetime.strptime(date_str, fmt)
                    if latest_date is None or trial_date > latest_date:
                        latest_date = trial_date
                    break
                except ValueError:
                    continue
    
    # Use the latest trial date if found, otherwise fall back to current date
    if latest_date:
        target_date = latest_date
        logging.info(f"CSV filename: Using latest trial date {target_date.strftime('%m/%d/%Y')}")
    else:
        target_date = datetime.now()
        logging.warning(f"CSV filename: No valid trial dates found, using current date {target_date.strftime('%m/%d/%Y')}")
    
    # Generate YYMM format (monthly version tag)
    month_prefix = f"{str(target_date.year)[2:]}{target_date.month:02d}"
    filename = f"{month_prefix}-copwatchdog.csv"
    logging.info(f"Generated CSV filename: {filename}")
    return filename

def load_existing_articles(articles_csv_path):
    """
    Load existing articles from articles.csv to prevent duplicates.
    
    Args:
        articles_csv_path: Path to the articles.csv file
        
    Returns:
        Tuple of (existing_articles_list, existing_url_badge_pairs_set, next_article_id)
    """
    existing_articles = []
    existing_url_badge_pairs = set()  # Track (url, badge) combinations
    next_article_id = 1
    
    if articles_csv_path.exists():
        try:
            with articles_csv_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_articles.append(row)
                    url = row.get("url", "")
                    badge = row.get("badge", "")
                    if url and badge:
                        # Track URL+badge combination to allow same article for different officers
                        existing_url_badge_pairs.add((url, badge))
                    # Track highest article_id to generate next ID
                    try:
                        article_id = int(row.get("article_id", 0))
                        if article_id >= next_article_id:
                            next_article_id = article_id + 1
                    except ValueError:
                        pass
            
            logging.info(f"Articles: Loaded {len(existing_articles)} existing articles from {articles_csv_path}")
            logging.info(f"Articles: Next article_id will be {next_article_id}")
        except Exception as e:
            logging.warning(f"Articles: Failed to load existing articles from {articles_csv_path}: {e}")
    else:
        logging.info(f"Articles: No existing articles.csv found at {articles_csv_path}, will create new file")
    
    return existing_articles, existing_url_badge_pairs, next_article_id

def save_articles_csv(articles_csv_path, articles_list):
    """
    Write articles to articles.csv with proper fieldnames.
    
    Args:
        articles_csv_path: Path to the articles.csv file
        articles_list: List of article dictionaries to write
        
    Returns:
        Number of articles written
    """
    fieldnames = [
        "article_id",
        "badge",
        "first_name",
        "last_name",
        "title",
        "source",
        "date_published",
        "url"
    ]
    
    try:
        with articles_csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for article in articles_list:
                writer.writerow(article)
        
        logging.info(f"Articles: Wrote {len(articles_list)} articles to {articles_csv_path}")
        return len(articles_list)
    except Exception as e:
        logging.error(f"Articles: Failed to write articles.csv: {e}")
        return 0

# === Payroll Name Matching Helpers ===
def _strip_suffix(name: str) -> str:
    """
    Remove common name suffixes (Jr, Sr, II, etc.) from a name.
    
    Args:
        name: A name that might contain a suffix
        
    Returns:
        The name without the suffix, if a suffix was found
    """
    if not name:
        return ""
    parts = name.lower().split()
    if parts and parts[-1] in SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)

def _match_last_name(source_last: str, candidate_last: str) -> bool:
    """
    Compare last names, handling suffixes and allowing partial matches.
    
    This function strips suffixes and checks if either name is contained within 
    the other after normalization (removing non-alphanumeric chars).
    
    Args:
        source_last: The source last name to match
        candidate_last: The candidate last name to compare against
        
    Returns:
        True if the names match by containment after normalization
    """
    if not source_last or not candidate_last:
        logging.debug(f"Last-name match skipped due to missing value: source='{source_last}', candidate='{candidate_last}'")
        return False
    source_last_clean = _strip_suffix(source_last)
    candidate_last_clean = _strip_suffix(candidate_last)
    result = (_norm(source_last_clean) in _norm(candidate_last_clean) or
              _norm(candidate_last_clean) in _norm(source_last_clean))
    logging.debug(f"Last-name match: source='{source_last}' candidate='{candidate_last}' -> {result}")
    return result

# === NYPDTRIAL Extraction ===
def extract_from_nypdtrial(page, retries=5, timeout=30000):  # Increased timeout to 30 seconds and retries to 5
    logging.info(f"Visiting NYPD Trials: {SITES['NYPDTRIAL']}")
    attempt = 0
    gc.collect()  # Force garbage collection before starting
    while attempt < retries:
        try:
            logging.info(f"Trails: Attempt {attempt + 1}/{retries} to load NYPD Trials page...")
            page.goto(SITES["NYPDTRIAL"], timeout=timeout, wait_until="networkidle")
            logging.info("Trails: Page loaded successfully")
            # Add a verification step
            if page.query_selector("table"):
                logging.info("Trails: Found table elements on the page")
                break
            else:
                logging.warning("Trails: Page loaded but no tables found, may need to retry")
                attempt += 1
        except TimeoutError:
            attempt += 1
            logging.warning(f"Trails: NYPD Trials page load timed out after {timeout}ms (attempt {attempt}/{retries})")
            if attempt < retries:
                logging.info("50-a: Waiting 5 seconds before retrying...")
                page.wait_for_timeout(5000)  # Wait 5 seconds between attempts
        except Exception as e:
            attempt += 1
            logging.error(f"Trails: Unexpected error loading page: {str(e)}")
            if attempt >= retries:
                logging.error(f"Trails: Failed to load NYPD Trials page after {retries} attempts")
                return []
            logging.info("Trails: Waiting 5 seconds before retrying...")
            page.wait_for_timeout(5000)  # Wait 5 seconds between attempts
    
    if attempt >= retries:
        logging.error(f"Trails: Failed to load NYPD Trials page after {retries} attempts")
        return []

    tables = page.query_selector_all("table")
    logging.info(f"Trails: Found {len(tables)} tables on the NYPD Trials page")

    scored_tables = [(table, score_table_by_keywords(table, KEYWORDS)) for table in tables]
    selected_tables = [t for t, score in scored_tables if score >= THRESHOLD]
    logging.info(f"Trails: Selected {len(selected_tables)} tables with threshold >= {THRESHOLD}")

    records = []
    for ti, table in enumerate(selected_tables, start=1):
        data = extract_table(table)
        logging.info(f"Trails: Table #{ti}: extracted {len(data)} rows")
        for row_idx, record in enumerate(data, start=1):
            if record.get("Name"):
                parts = record["Name"].split()
                record["First"] = parts[0]
                record["Last"] = " ".join(parts[1:]) if len(parts) > 1 else ""
                # Extract middle initial if present in the name string
                record["Initial"] = _extract_initial(record["Name"])
                logging.info(f"Trails: Record #{row_idx} parsed Name -> First: '{record['First']}' Last: '{record['Last']}'")
            records.append(record)
    logging.info(f"Trails: Total trial records extracted: {len(records)}")
    return records

# === FIFTYA Enrichment ===
def enrich_with_50a(page, record, is_rescrape=False):
    """
    Enrich record with data from 50-a.org
    
    Args:
        page: Playwright page object
        record: Officer record dictionary
        is_rescrape: If True, apply status codes (NOT_FOUND, UNVERIFIED) for missing data
                     If False (first run), leave fields as NULL
    
    Returns:
        List of article dictionaries extracted from officer's news section
    """
    # Fields that 50-a enrichment populates
    FIFTYA_FIELDS = ["race", "gender", "tax_id", "email", "badge", 
                     "current_assignment", "assignment_start", "previous_assignments",
                     "precinct_link", "precinct_number", "service_start", "last_earned"]
    
    # Check if this is enrich mode with targeted columns
    enrich_columns = record.get("enrich_columns", [])
    if enrich_columns:
        # Only scrape fields that are in the target list
        FIFTYA_FIELDS = [f for f in FIFTYA_FIELDS if f in enrich_columns]
        logging.info(f"50-a: ENRICH MODE - targeting {len(FIFTYA_FIELDS)} fields: {FIFTYA_FIELDS}")
    
    officer_name = record.get("Name")
    first = record.get("First")
    last = record.get("Last")
    if not officer_name or not first or not last:
        logging.warning("50-a: Missing Name/First/Last; skipping enrichment")
        return []

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
        return []

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
        logging.warning(f"50-a: still no match for '{officer_name}'.")
        # Only set NOT_FOUND status during rescrape (Phase 2)
        # On first run, fields remain NULL to trigger future rescrape
        if is_rescrape:
            logging.info(f"50-a: rescrape mode - setting NOT_FOUND status for 50-a fields")
            # Dynamically set NOT_FOUND for critical 50-a fields that are empty
            for field in ["race", "gender", "tax_id", "email"]:
                if not record.get(field):
                    record[field] = "NOT_FOUND"
        return []

    try:
        target_officer.query_selector("a.name").click()
        page.wait_for_selector("div.identity", timeout=7000)
        logging.info("50-a: officer profile loaded")
    except TimeoutError:
        logging.warning("50-a: officer profile did not load in time after click")
        return []

    identity = page.query_selector("div.identity")
    if not identity:
        logging.warning("50-a: 'div.identity' not found on profile")
        return []

    identity_text = identity.inner_text().strip()

    # Extract profile URL (page URL on 50-a.org)
    profile_url = None
    try:
        # Get current page URL after clicking into officer profile
        current_url = page.url
        if current_url and '/officer/' in current_url:
            profile_url = current_url
            logging.info(f"50-a: Profile URL captured: {profile_url}")
    except Exception as e:
        logging.warning(f"50-a: Failed to capture profile URL: {e}")
    record["profile_url"] = profile_url

    # Extract officer image URL if available
    officer_image = None
    image_link = identity.query_selector("a.is-pulled-right.ml-1.is-hidden-mobile")
    if image_link:
        href = image_link.get_attribute("href")
        if href:
            # Convert relative URL to absolute URL
            if href.startswith("http"):
                officer_image = href
            else:
                officer_image = SITES["FIFTYA"].rstrip("/") + href
            logging.info(f"50-a: Officer image found: {officer_image}")
    record["officer_image"] = officer_image

    # Extract Race and Gender (e.g., "Badge #4748, White Male")
    race = None
    gender = None
    race_gender_match = re.search(r'Badge\s*#?\d+,\s*([A-Za-z\s]+?)\s+(Male|Female)', identity_text, re.I)
    if race_gender_match:
        race = race_gender_match.group(1).strip()
        gender = race_gender_match.group(2).strip()
        logging.info(f"50-a: {race} {gender}")
    record["race"] = race
    record["gender"] = gender

    # Extract Email
    email = None
    email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', identity_text)
    if email_match:
        email = email_match.group(1)
        logging.info(f"50-a: Email: {email}")
    record["email"] = email

    # Extract Tax ID (e.g., "Tax #965911")
    tax_id = None
    tax_match = re.search(r'Tax\s*#?\s*(\d+)', identity_text, re.I)
    if tax_match:
        tax_id = tax_match.group(1)
        logging.info(f"50-a: Tax: #{tax_id}")
    record["tax_id"] = tax_id

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
            logging.info(f"50-a: Badge: #{badge}")
    else:
        logging.info(f"50-a: Badge: #{badge}")
    record["badge"] = badge

    # Extract Precinct Description and parse into three fields
    precinct_desc_raw = None
    precinct_link = None
    precinct_number = None
    
    # Try to get full precinct description from identity text
    precinct_desc_match = re.search(r'(Police Officer|Detective|Sergeant|Lieutenant|Captain)\s+at\s+(.+?)(?:Service started|$)', identity_text, re.I | re.DOTALL)
    if precinct_desc_match:
        precinct_desc_raw = precinct_desc_match.group(2).strip()
        # Clean up newlines and extra whitespace
        precinct_desc_raw = re.sub(r'\s+', ' ', precinct_desc_raw).strip()
        logging.info(f"50-a: Raw precinct desc: {precinct_desc_raw}")
    
    # Parse precinct description into three components
    current_assignment, assignment_start, previous_assignments = _parse_precinct_desc(precinct_desc_raw)
    record["current_assignment"] = current_assignment
    record["assignment_start"] = assignment_start
    record["previous_assignments"] = previous_assignments
    
    logging.info(f"50-a: Current Assignment: '{current_assignment}' | Start: '{assignment_start}' | Previous: '{previous_assignments}'")


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
            logging.info(f"50-a: Started {month_str} {year}")
        except Exception:
            service_start = None
    else:
        m2 = re.search(r"Started\s+([A-Za-z]+)\s+(\d{4})", identity_text, re.I)
        if m2:
            month_str, year = m2.groups()
            try:
                month = datetime.strptime(month_str[:3], "%b").month
                service_start = f"{month:02}/01/{year}"
                logging.info(f"50-a: Started {month_str} {year}")
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
            logging.info(f"50-a: Made {last_earned} last year")
        else:
            last_earned = comp_text
            logging.info(f"50-a: Made {last_earned} last year")
    else:
        m = re.search(r'made\s*\$([\d,]+(?:\.\d+)?)', identity_text, re.I)
        if m:
            last_earned = f"${m.group(1)}"
            logging.info(f"50-a: Made {last_earned} last year")
    record["last_earned"] = last_earned

    discipline = identity.query_selector("div.discipline")
    record["has_discipline"] = "Y" if discipline and discipline.query_selector("article.message") else "N"
    news = identity.query_selector("div.news")
    record["has_articles"] = "Y" if news and news.query_selector_all("a[href^='http']") else "N"

    # Extract articles from div.news if present
    articles = []
    if news:
        # 50-a.org structure: <a href="url">Title</a>, Source, Date<br>
        # Extract all anchor tags directly (exclude the header anchor #articles)
        news_items = news.query_selector_all("a[href^='http']")
        
        logging.info(f"50-a: Found {len(news_items)} potential news items for '{officer_name}'")
        
        for item in news_items:
            article_data = _parse_article_html(item)
            if article_data:
                # Link article to officer
                article_data["badge"] = record.get("badge", "")
                article_data["first_name"] = record.get("First", "")
                article_data["last_name"] = record.get("Last", "")
                articles.append(article_data)
                logging.info(f"50-a: Extracted article '{article_data['title']}' for '{officer_name}'")

    # Log substantiated allegations if present
    substantiated_div = page.query_selector("div.substantiated")
    if substantiated_div:
        allegations = []
        for item in substantiated_div.query_selector_all("li"):
            allegations.append(item.inner_text().strip())
        if allegations:
            allegations_str = ", ".join(allegations)
            logging.info(f"50-a: Substantiated Allegations: {allegations_str}")

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
        if m2:
            settlement_value = int(m2.group(1).replace(",", ""))
            record["total_settlements"] = f"${settlement_value:,}"
        else:
            record["total_settlements"] = 0
        logging.info(f"50-a: lawsuits={record.get('num_lawsuits')} settlements={record.get('total_settlements')}")

    # Mark successful enrichment
    record["enrichment_status_50a"] = "FOUND"
    
    # Set UNVERIFIED status for fields that should have data but extraction failed
    # Only apply during rescrape (Phase 2) - on first run, fields remain NULL
    if is_rescrape:
        if not record.get("race"):
            record["race"] = "UNVERIFIED"
            logging.info(f"50-a: race field set to UNVERIFIED (extraction failed)")
        if not record.get("gender"):
            record["gender"] = "UNVERIFIED"
            logging.info(f"50-a: gender field set to UNVERIFIED (extraction failed)")
    
    logging.info(f"50-a: enrichment complete for '{officer_name}' (badge={record.get('badge')}, pct={record.get('precinct_number')}, started={record.get('service_start')}, last_earned={record.get('last_earned')})")
    return articles

# === PAYROLL Enrichment ===
def enrich_with_payroll(page, record, is_rescrape=False):
    """
    Enrich record with NYC Payroll data
    
    Args:
        page: Playwright page object
        record: Officer record dictionary
        is_rescrape: If True, apply status codes (NOT_FOUND, UNVERIFIED) for missing data
                     If False (first run), leave fields as NULL
    """
    # Fields that payroll enrichment populates
    PAYROLL_FIELDS = ["leave_status_as_of_june_30", "base_salary", "pay_basis", 
                      "regular_hours", "regular_gross_paid", "ot_hours", 
                      "total_ot_paid", "total_other_pay"]
    
    # Check if this is enrich mode with targeted columns
    enrich_columns = record.get("enrich_columns", [])
    if enrich_columns:
        # Only scrape fields that are in the target list
        PAYROLL_FIELDS = [f for f in PAYROLL_FIELDS if f in enrich_columns]
        logging.info(f"Payroll: ENRICH MODE - targeting {len(PAYROLL_FIELDS)} fields: {PAYROLL_FIELDS}")
        if not PAYROLL_FIELDS:
            logging.info(f"Payroll: No payroll fields in enrich target list, skipping")
            return
    
    first = record.get("First", "")
    last = record.get("Last", "")

    if not first or not last:
        logging.warning("Payroll: Missing First or Last; skipping enrichment")
        return

    # Check cache first - reuse successful payroll data for duplicate officers
    cache_key = (first.lower(), last.lower(), record.get("service_start", ""))
    if cache_key in _payroll_cache:
        cached_data = _payroll_cache[cache_key]
        record.update(cached_data)
        logging.info(f"Payroll: reused cached data for '{first} {last}' (service_start={record.get('service_start')})")
        return

    # Include middle initial in the payroll query when available to improve matching
    initial = record.get("Initial", "")
    if initial:
        query = f"{first} {last} {initial}"
    else:
        query = f"{first} {last}"
    logging.info(f"Payroll: Searching for '{query}'")
    # Note: do NOT try a single submit here; instead perform a full navigation+submit on each attempt

    # We'll attempt the search + parse up to 3 times (initial + 2 retries)
    max_attempts = 3
    attempt = 0
    chosen = None
    current_year = datetime.now().year
    priority_year = str(current_year - 1)
    fallback_year = str(current_year - 2)
    logging.info(f"Payroll: targeting {priority_year} first, then {fallback_year}")

    service_start_dt = _parse_mm01yyyy(record.get("service_start", ""))
    if service_start_dt:
        logging.info(f"Payroll: using service_start tie-breaker = {service_start_dt.date()}")

    # Scan up to N rows per attempt to avoid spinning forever on huge result sets
    max_rows_per_attempt = 10

    while attempt < max_attempts and not chosen:
        attempt += 1
        
        # Non-linear exponential backoff: 0s, 2s, 5s
        if attempt > 1:
            wait_time = int(2000 * (1.5 ** (attempt - 2)))
            logging.info(f"Payroll: waiting {wait_time}ms before attempt {attempt} for '{query}'")
            page.wait_for_timeout(wait_time)
        
        logging.info(f"Payroll: attempt {attempt}/{max_attempts} for '{query}'")
        try:
            # Do a full re-entry each attempt: navigate to the payroll site and submit the search
            try:
                page.goto(SITES["PAYROLL"], wait_until="networkidle")
                logging.info(f"Payroll: navigated to payroll site for attempt {attempt} for '{query}'")
                # find the search input and run the query
                search_input = page.query_selector("input#search-view")
                if not search_input:
                    logging.warning(f"Payroll: search input not found on attempt {attempt} for '{query}' - will retry")
                    # small wait before next attempt to avoid tight loop
                    page.wait_for_timeout(500)
                    continue
                search_input.fill(query)
                search_input.press("Enter")
                page.wait_for_selector("table tbody tr", timeout=7000)
                logging.info(f"Payroll: search submitted on attempt {attempt} for '{query}'")

                # Quick verification: ensure the search input took and results are relevant.
                try:
                    applied_val = ""
                    try:
                        applied_val = search_input.input_value()
                    except Exception:
                        applied_val = search_input.get_attribute("value") if hasattr(search_input, 'get_attribute') else ""

                    norm_applied = _norm(applied_val or "")
                    norm_last = _norm(last)
                    norm_first = _norm(first)

                    # Check the first few rows for the target name; if none match and the input value
                    # doesn't contain the name, treat as a failed search and retry.
                    preliminary_rows = page.query_selector_all("table tbody tr")[:5]
                    found_in_rows = False
                    for pr in preliminary_rows:
                        try:
                            row_text = " ".join([c.inner_text().strip() for c in pr.query_selector_all("td")])
                        except Exception:
                            row_text = pr.inner_text().strip()
                        if (norm_last and norm_last in _norm(row_text)) or (norm_first and norm_first in _norm(row_text)):
                            found_in_rows = True
                            break

                    if not found_in_rows and norm_last and norm_last not in norm_applied and norm_first and norm_first not in norm_applied:
                        logging.warning(f"Payroll: search input did not apply for '{query}' on attempt {attempt} (input='{applied_val}'); will retry")
                        page.wait_for_timeout(500)
                        continue
                except Exception as e:
                    logging.debug(f"Payroll: verification check failed on attempt {attempt} for '{query}': {e}")
            except TimeoutError:
                logging.warning(f"Payroll: navigation/search timed out on attempt {attempt} for '{query}'")
                continue
            except Exception as e:
                logging.info(f"Payroll: navigation/search encountered error on attempt {attempt} for '{query}': {e}")
                continue

            # Re-query rows from the DOM each attempt
            rows = page.query_selector_all("table tbody tr")
            logging.info(f"Payroll: found {len(rows)} table rows for '{query}' on attempt {attempt}")
            if not rows:
                logging.warning(f"Payroll: no rows returned for '{query}' on attempt {attempt}")
                continue

            for row_idx, row in enumerate(rows, start=1):
                if row_idx > max_rows_per_attempt:
                    logging.info(f"Payroll: reached {max_rows_per_attempt} rows on attempt {attempt}, stopping scan for this attempt")
                    break

                cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
                if not cells or len(cells) < 17:
                    logging.debug(f"Payroll: skipping row #{row_idx} on attempt {attempt} (insufficient cells)")
                    continue

                year = cells[0]
                last_name = cells[3]
                first_name = cells[4]
                agency_start_date = cells[6]

                logging.info(
                    f"Payroll: attempt {attempt} row#{row_idx} -> year={year}, first='{first_name}', "
                    f"last='{last_name}', agency_start='{agency_start_date}'"
                )

                # Only consider priority and fallback years
                if year not in (priority_year, fallback_year):
                    continue

                if not _match_last_name(last, last_name):
                    logging.debug(f"Payroll: attempt {attempt} row#{row_idx} last-name mismatch (candidate '{first_name} {last_name}')")
                    continue

                # Immediate accept for priority year
                if year == priority_year:
                    chosen = (cells, None)
                    logging.info(f"Payroll: attempt {attempt} row#{row_idx} is {year}, chosen immediately")
                    break

                # Fallback year: use tie-breaker by service_start delta
                if year == fallback_year:
                    delta_days = None
                    if service_start_dt:
                        asd = _parse_mmddyyyy(agency_start_date)
                        if asd:
                            delta_days = abs((asd - service_start_dt).days)
                            logging.info(f"Payroll: attempt {attempt} row#{row_idx} matched {fallback_year}; agency_start delta_days={delta_days}")

                    if not chosen:
                        chosen = (cells, delta_days)
                        logging.info(f"Payroll: attempt {attempt} row#{row_idx} tentatively chosen as fallback")
                    else:
                        _, current_delta = chosen
                        if delta_days is not None and (current_delta is None or delta_days < current_delta):
                            chosen = (cells, delta_days)
                            logging.info(
                                f"Payroll: attempt {attempt} row#{row_idx} replaces previous fallback (smaller delta {delta_days} < {current_delta})"
                            )

            # end for rows
        except TimeoutError:
            logging.warning(f"Payroll: attempt {attempt} timed out for '{query}'")
        except Exception as e:
            logging.warning(f"Payroll: attempt {attempt} encountered error for '{query}': {e}")

    # end while attempts

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
            
            # Cache successful payroll data
            _payroll_cache[cache_key] = payroll_data.copy()
            
            # Mark successful enrichment
            record["enrichment_status_payroll"] = "FOUND"
            
            # Set UNVERIFIED status for payroll fields that should have data but are missing
            # Only during rescrape (Phase 2) - on first run, fields remain NULL
            if is_rescrape:
                if not record.get("base_salary"):
                    record["base_salary"] = "UNVERIFIED"
                    logging.info(f"Payroll: base_salary field set to UNVERIFIED (extraction failed)")
                if not record.get("pay_basis"):
                    record["pay_basis"] = "UNVERIFIED"
                    logging.info(f"Payroll: pay_basis field set to UNVERIFIED (extraction failed)")
            
            logging.info(
                f"Payroll: chosen row year={cells[0]} agency_start={cells[6]} status={cells[9]} "
                f"- payroll fields updated and cached"
            )
        except Exception as e:
            logging.warning(f"Payroll: failed to parse chosen row for '{query}': {e}")
    else:
        logging.warning(f"Payroll: no suitable payroll match found for '{query}' — will attempt one refresh-and-retry")
        # Try one safe refresh and retry in case the site returned inconsistent results
        try:
            page.reload(wait_until="networkidle")
            logging.info(f"Payroll: page reloaded for retry for '{query}'")
            # Small wait to allow dynamic content to settle
            page.wait_for_timeout(1000)
            # Re-run the search input fill/press sequence
            search_input = page.query_selector("input#search-view")
            if not search_input:
                logging.info(f"Payroll: search input not found after reload for '{query}' — will navigate and attempt full submit")
                try:
                    page.goto(SITES["PAYROLL"], wait_until="networkidle")
                    logging.info(f"Payroll: navigated to payroll site for final retry for '{query}'")
                    page.wait_for_timeout(500)
                    search_input = page.query_selector("input#search-view")
                except Exception as e:
                    logging.warning(f"Payroll: navigation failed during final retry for '{query}': {e}")

            if search_input:
                logging.info(f"Payroll: re-submitting search on final retry for '{query}'")
                try:
                    search_input.fill(query)
                    search_input.press("Enter")
                    page.wait_for_selector("table tbody tr", timeout=7000)
                except TimeoutError:
                    logging.warning(f"Payroll: final retry search timed out for '{query}'")
            # collect rows after attempting to re-submit (or just reading what's on the page)
            rows = page.query_selector_all("table tbody tr")
            logging.info(f"Payroll: retry found {len(rows)} rows for '{query}'")
            # Only attempt to find a match using the exact same logic as above
            for row_idx, row in enumerate(rows, start=1):
                if row_idx > 25:
                    break
                cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
                if not cells or len(cells) < 17:
                    continue
                year = cells[0]
                last_name = cells[3]
                first_name = cells[4]
                agency_start_date = cells[6]
                if year not in (priority_year, fallback_year):
                    continue
                if not _match_last_name(last, last_name):
                    continue
                # prefer priority year on retry as well
                if year == priority_year:
                    chosen = (cells, None)
                    logging.info(f"Payroll(retry): row#{row_idx} is {year}, chosen immediately")
                    break
                if year == fallback_year and not chosen:
                    chosen = (cells, None)
                    logging.info(f"Payroll(retry): row#{row_idx} chosen as fallback")
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
                    
                    # Cache successful retry data
                    _payroll_cache[cache_key] = payroll_data.copy()
                    
                    logging.info(f"Payroll(retry): payroll fields updated from retry for '{query}' and cached")
                except Exception as e:
                    logging.warning(f"Payroll(retry): failed to parse chosen row for '{query}': {e}")
        except TimeoutError:
            logging.warning(f"Payroll: retry timed out for '{query}'")
        except Exception as e:
            logging.warning(f"Payroll: retry encountered error for '{query}': {e}")
    
    # If payroll data still not found, set status codes for payroll fields
    # Only during rescrape (Phase 2) - on first run, fields remain NULL
    if not record.get("base_salary") and not record.get("pay_basis"):
        if is_rescrape:
            logging.info(f"Payroll: rescrape mode - No data found for '{query}', setting NOT_FOUND status for payroll fields")
            # Dynamically set NOT_FOUND for all payroll fields
            for field in ["base_salary", "pay_basis", "regular_hours", "regular_gross_paid", 
                         "ot_hours", "total_ot_paid", "total_other_pay"]:
                if not record.get(field):
                    record[field] = "NOT_FOUND"


# === Main Script ===
all_records = []
all_articles = []  # Collect articles during enrichment

with sync_playwright() as p:
    logging.info("Launching headless Chromium")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Extract NYPDTRIAL or build from rescrape/enrich list
    if enrich_mode:
        logging.info("ENRICH MODE: Building officer list from delta enrichment CSV")
        
        # Connect to database to fetch officer names
        try:
            conn = psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5433'),
                database=os.getenv('DB_NAME', 'copwatch'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASS', '')
            )
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch officer names for all source_ids in one query
            source_ids = list(enrich_targets.keys())
            cursor.execute(
                "SELECT source_id, first_name, last_name, badge FROM cwd_raw.officers_raw WHERE source_id = ANY(%s)",
                (source_ids,)
            )
            officer_data = {row['source_id']: row for row in cursor.fetchall()}
            cursor.close()
            conn.close()
            
            logging.info(f"ENRICH MODE: Fetched names for {len(officer_data)} officers from database")
            
        except Exception as e:
            logging.error(f"ENRICH MODE: Database connection failed: {e}")
            logging.error("Cannot proceed without officer names - exiting")
            sys.exit(1)
        
        # Build records with names from database
        for source_id, target_info in enrich_targets.items():
            if source_id not in officer_data:
                logging.warning(f"ENRICH MODE: source_id {source_id} not found in database, skipping")
                continue
            
            officer = officer_data[source_id]
            record = {
                'Name': f"{officer['first_name']} {officer['last_name']}",
                'First': officer['first_name'],
                'Last': officer['last_name'],
                'badge': officer['badge'] or target_info['badge'],
                'source_id': source_id,
                'version_tag': target_info['version_tag'],
                'enrich_columns': target_info['columns'],
                'priority': target_info['priority'],
                'Date': '',
                'Time': '',
                'Rank': '',
                'Trial Room': '',
                'Case Type': 'Enrichment'
            }
            all_records.append(record)
        
        logging.info(f"ENRICH MODE: Built {len(all_records)} officer records for enrichment")
        logging.info(f"ENRICH MODE: Total fields to enrich: {sum(len(r.get('enrich_columns', [])) for r in all_records)}")
    elif rescrape_mode:
        logging.info("RESCRAPE MODE: Building officer list from target CSV (skipping NYPD Trials page)")
        # Build minimal records from target list - enrichment will fill in the rest
        for target in rescrape_targets:
            record = {
                'Name': f"{target['first_name']} {target['last_name']}",  # Required by enrich_with_50a
                'First': target['first_name'],
                'Last': target['last_name'],
                'badge': target['badge'],
                'source_id': target['source_id'],
                'Date': '',  # Not needed for rescrape
                'Time': '',
                'Rank': '',  # Will be filled by 50-a enrichment
                'Trial Room': '',
                'Case Type': 'Re-scrape'
            }
            all_records.append(record)
        logging.info(f"RESCRAPE MODE: Built {len(all_records)} officer records from target list")
    else:
        # Full scrape mode - extract from NYPD Trials page
        all_records = extract_from_nypdtrial(page, retries=3, timeout=5000)
        logging.info(f"Main: extracted {len(all_records)} records from NYPDTRIAL")

    # Enrich with FIFTYA
    logging.info("Main: beginning 50-a enrichment pass")
    for idx, record in enumerate(all_records, start=1):
        logging.info(f"Main: 50-a enrich record #{idx} - {record.get('Name')}")
        articles = enrich_with_50a(page, record, is_rescrape=rescrape_mode)
        all_articles.extend(articles)  # Collect articles from this officer
        # Random jitter between 50-a queries to reduce server-side throttling
        jitter = int(random.uniform(150, 600))
        page.wait_for_timeout(jitter)

    # Enrich with PAYROLL
    logging.info("Main: beginning payroll enrichment pass")
    page = context.new_page()
    for idx, record in enumerate(all_records, start=1):
        logging.info(f"Main: payroll enrich record #{idx} - First='{record.get('First')}' Last='{record.get('Last')}'")
        enrich_with_payroll(page, record, is_rescrape=rescrape_mode)
        # Random jitter between payroll queries to reduce server-side throttling
        jitter = int(random.uniform(150, 600))
        page.wait_for_timeout(jitter)

    browser.close()
    logging.info("Browser closed, Dogs returned")

    # === Apply N/A status for non-applicable fields ===
    # Only during rescrape (Phase 2) - on first run, fields remain NULL
    if rescrape_mode:
        logging.info("Main: rescrape mode - applying N/A status for rank-based field exclusions")
        for idx, record in enumerate(all_records, start=1):
            rank = record.get("Rank", "").lower()
            is_lieutenant_or_higher = any(r in rank for r in ["lieutenant", "captain", "deputy", "chief", "inspector"])
            
            # Lieutenants and higher ranks don't have badge numbers
            if is_lieutenant_or_higher and not record.get("badge"):
                record["badge"] = "N/A"
                logging.info(f"Main: record #{idx} ({record.get('Name')}) - set badge=N/A (rank: {record.get('Rank')})")

# === Save CSV ===
# Generate CSV filename based on actual trial dates
CSV_FILE = _generate_csv_filename(all_records, override_version_tag)

# Ensure the CSV directory exists
CSV_DIR.mkdir(parents=True, exist_ok=True)
csv_path = CSV_DIR / CSV_FILE
local_csv_path = Path(LOCAL_CSV_FILE)

# === Merge with existing CSV if in rescrape mode ===
if rescrape_mode and csv_path.exists():
    logging.info(f"RESCRAPE MODE: Merging with existing CSV at {csv_path}")
    try:
        existing_records = []
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_records = list(reader)
        
        logging.info(f"Loaded {len(existing_records)} existing records from {csv_path}")
        
        # Create a map of rescraped officers by (First, Last) for quick lookup
        rescrape_map = {}
        for record in all_records:
            first = record.get("First", "").strip().lower()
            last = record.get("Last", "").strip().lower()
            key = (first, last)
            rescrape_map[key] = record
        
        logging.info(f"Rescraped {len(rescrape_map)} unique officers")
        
        # Merge: Update existing records with rescraped data, keep non-rescraped records as-is
        merged_records = []
        updated_count = 0
        for existing in existing_records:
            first = existing.get("First", "").strip().lower()
            last = existing.get("Last", "").strip().lower()
            key = (first, last)
            
            if key in rescrape_map:
                # This officer was rescraped - fill in ALL enrichment fields
                # Keep trial data (Date, Time, Room, Case Type, Rank) from existing
                rescraped = rescrape_map[key]
                
                # Fill in enrichment fields from rescraped data (internal field names)
                existing["Badge"] = rescraped.get("badge", existing.get("Badge", ""))
                existing["PCT"] = rescraped.get("precinct_number", existing.get("PCT", ""))
                existing["PCT URL"] = rescraped.get("precinct_link", existing.get("PCT URL", ""))
                existing["Race"] = rescraped.get("race", existing.get("Race", ""))
                existing["Gender"] = rescraped.get("gender", existing.get("Gender", ""))
                existing["Tax ID"] = rescraped.get("tax_id", existing.get("Tax ID", ""))
                existing["Email"] = rescraped.get("email", existing.get("Email", ""))
                existing["Current Assignment"] = rescraped.get("current_assignment", existing.get("Current Assignment", ""))
                existing["Assignment Start"] = rescraped.get("assignment_start", existing.get("Assignment Start", ""))
                existing["Previous Assignments"] = rescraped.get("previous_assignments", existing.get("Previous Assignments", ""))
                existing["Officer Image"] = rescraped.get("officer_image", existing.get("Officer Image", ""))
                existing["Profile URL"] = rescraped.get("profile_url", existing.get("Profile URL", ""))
                existing["Started"] = rescraped.get("service_start", existing.get("Started", ""))
                existing["Last Earned"] = rescraped.get("last_earned", existing.get("Last Earned", ""))
                existing["Disciplined"] = rescraped.get("has_discipline", existing.get("Disciplined", ""))
                existing["Articles"] = rescraped.get("has_articles", existing.get("Articles", ""))
                existing["# Complaints"] = rescraped.get("num_complaints", existing.get("# Complaints", ""))
                existing["# Allegations"] = rescraped.get("num_allegations", existing.get("# Allegations", ""))
                existing["# Substantiated"] = rescraped.get("num_substantiated", existing.get("# Substantiated", ""))
                existing["# Charges"] = rescraped.get("num_substantiated_charges", existing.get("# Charges", ""))
                existing["# Unsubstantiated"] = rescraped.get("num_unsubstantiated", existing.get("# Unsubstantiated", ""))
                existing["# Guidelined"] = rescraped.get("num_within_guidelines", existing.get("# Guidelined", ""))
                existing["# Lawsuits"] = rescraped.get("num_lawsuits", existing.get("# Lawsuits", ""))
                existing["Total Settlements"] = rescraped.get("total_settlements", existing.get("Total Settlements", ""))
                existing["Status"] = rescraped.get("leave_status_as_of_june_30", existing.get("Status", ""))
                existing["Base Salary"] = rescraped.get("base_salary", existing.get("Base Salary", ""))
                existing["Pay Basis"] = rescraped.get("pay_basis", existing.get("Pay Basis", ""))
                existing["Regular Hours"] = rescraped.get("regular_hours", existing.get("Regular Hours", ""))
                existing["Regular Gross Paid"] = rescraped.get("regular_gross_paid", existing.get("Regular Gross Paid", ""))
                existing["OT Hours"] = rescraped.get("ot_hours", existing.get("OT Hours", ""))
                existing["Total OT Paid"] = rescraped.get("total_ot_paid", existing.get("Total OT Paid", ""))
                existing["Total Other Pay"] = rescraped.get("total_other_pay", existing.get("Total Other Pay", ""))
                
                merged_records.append(existing)
                updated_count += 1
                logging.info(f"Merged enrichment for {first.title()} {last.title()}")
            else:
                # This officer was not rescraped - keep existing data unchanged
                merged_records.append(existing)
        
        # Replace all_records with merged data
        all_records = merged_records
        logging.info(f"Merge complete: {updated_count} records updated, {len(merged_records)} total records")
        
    except Exception as e:
        logging.error(f"Failed to merge with existing CSV: {e}")
        logging.info("Proceeding with rescraped records only")

fieldnames = [
    "Date","Time","Rank","First","Last","Room","Case Type",
    "Badge","PCT","PCT URL","Race","Gender","Tax ID","Email",
    "Current Assignment","Assignment Start","Previous Assignments",
    "Officer Image","Profile URL","Started","Last Earned",
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
                "Date":                 r.get("Date", ""),
                "Time":                 r.get("Time", ""),
                "Rank":                 r.get("Rank", ""),
                "First":                r.get("First", ""),
                "Last":                 r.get("Last", ""),
                "Room":                 r.get("Room", r.get("Trial Room", "")),
                "Case Type":            r.get("Case Type", ""),
                "Badge":                r.get("Badge", r.get("badge", "")),
                "PCT":                  r.get("PCT", r.get("precinct_number", "")),
                "PCT URL":              r.get("PCT URL", r.get("precinct_link", "")),
                "Race":                 r.get("Race", r.get("race", "")),
                "Gender":               r.get("Gender", r.get("gender", "")),
                "Tax ID":               r.get("Tax ID", r.get("tax_id", "")),
                "Email":                r.get("Email", r.get("email", "")),
                "Current Assignment":   r.get("Current Assignment", r.get("current_assignment", "")),
                "Assignment Start":     r.get("Assignment Start", r.get("assignment_start", "")),
                "Previous Assignments": r.get("Previous Assignments", r.get("previous_assignments", "")),
                "Officer Image":        r.get("Officer Image", r.get("officer_image", "")),
                "Profile URL":          r.get("Profile URL", r.get("profile_url", "")),
                "Started":              r.get("Started", r.get("service_start", "")),
                "Last Earned":          r.get("Last Earned", r.get("last_earned", "")),
                "Disciplined":          r.get("Disciplined", r.get("has_discipline", "N")),
                "Articles":             r.get("Articles", r.get("has_articles", "N")),
                "# Complaints":         r.get("# Complaints", r.get("num_complaints", 0)),
                "# Allegations":        r.get("# Allegations", r.get("num_allegations", 0)),
                "# Substantiated":      r.get("# Substantiated", r.get("num_substantiated", 0)),
                "# Charges":            r.get("# Charges", r.get("num_substantiated_charges", 0)),
                "# Unsubstantiated":    r.get("# Unsubstantiated", r.get("num_unsubstantiated", 0)),
                "# Guidelined":         r.get("# Guidelined", r.get("num_within_guidelines", 0)),
                "# Lawsuits":           r.get("# Lawsuits", r.get("num_lawsuits", 0)),
                "Total Settlements":    r.get("Total Settlements", r.get("total_settlements", "")),
                "Status":               r.get("Status", r.get("leave_status_as_of_june_30", "")),
                "Base Salary":          r.get("Base Salary", r.get("base_salary", "")),
                "Pay Basis":            r.get("Pay Basis", r.get("pay_basis", "")),
                "Regular Hours":        r.get("Regular Hours", r.get("regular_hours", "")),
                "Regular Gross Paid":   r.get("Regular Gross Paid", r.get("regular_gross_paid", "")),
                "OT Hours":             r.get("OT Hours", r.get("ot_hours", "")),
                "Total OT Paid":        r.get("Total OT Paid", r.get("total_ot_paid", "")),
                "Total Other Pay":      r.get("Total Other Pay", r.get("total_other_pay", "")),
            })
            written += 1
        return written

# Conditional output based on operation mode
if enrich_mode:
    # === ENRICH MODE: Output enrichment CSV (source_id, column_name, new_value) ===
    logging.info("ENRICH MODE: Generating enrichment CSV output")
    
    # Map internal field names to database column names
    FIELD_TO_COLUMN = {
        'profile_url': 'profile_url',
        'race': 'race',
        'gender': 'gender',
        'tax_id': 'tax_id',
        'email': 'email',
        'badge': 'badge',
        'precinct_number': 'precinct_number',
        'current_assignment': 'current_assignment',
        'assignment_start': 'assignment_start',
        'service_start': 'service_start',
        'last_earned': 'last_earned',
        'base_salary': 'base_salary',
        'pay_basis': 'pay_basis'
    }
    
    enrichment_path = CSV_DIR / f"enrichment_{override_version_tag or 'output'}.csv"
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    
    with enrichment_path.open("w", newline="", encoding="utf-8") as f:
        enrichment_writer = csv.writer(f)
        enrichment_writer.writerow(["source_id", "column_name", "new_value"])
        
        enrichment_count = 0
        for record in all_records:
            source_id = record.get('source_id')
            target_columns = record.get('enrich_columns', [])
            
            for column in target_columns:
                # Find the internal field name that maps to this column
                internal_field = None
                for field, col in FIELD_TO_COLUMN.items():
                    if col == column:
                        internal_field = field
                        break
                
                if not internal_field:
                    logging.warning(f"ENRICH MODE: Unknown column '{column}' for {source_id}, skipping")
                    continue
                
                # Get the value from the record
                new_value = record.get(internal_field, '')
                
                # Only write if we found a value (not empty or NOT_FOUND)
                if new_value and new_value not in ['', 'NOT_FOUND', 'N/A']:
                    enrichment_writer.writerow([source_id, column, new_value])
                    enrichment_count += 1
                    logging.info(f"ENRICH MODE: {source_id}.{column} = {new_value}")
                else:
                    # Mark as not found
                    enrichment_writer.writerow([source_id, column, 'NOT_FOUND'])
                    logging.info(f"ENRICH MODE: {source_id}.{column} = NOT_FOUND")
    
    logging.info(f"=== THOTH ENRICH MODE Complete ===")
    logging.info(f"Enrichment CSV file ({enrichment_path}): {enrichment_count} fields enriched")
    
else:
    # === NORMAL MODE: Output standard copwatchdog CSV ===
    # Write to both locations
    monthly_written = write_csv_file(csv_path, all_records)
    local_written = write_csv_file(local_csv_path, all_records)

# === Save Articles CSV ===
# Skip articles in enrich mode (not needed for targeted enrichment)
if not enrich_mode:
    articles_csv_path = CSV_DIR / "articles.csv"
    logging.info(f"Articles: Processing {len(all_articles)} articles scraped from 50-a.org")

# Load existing articles and get next article_id
existing_articles, existing_url_badge_pairs, next_article_id = load_existing_articles(articles_csv_path)

# Filter out duplicate articles (by URL+badge combination) and assign article_id
new_articles = []
duplicate_count = 0
for article in all_articles:
    url = article.get("url", "")
    badge = article.get("badge", "")
    url_badge_pair = (url, badge)
    
    # Only skip if this exact URL+badge combination already exists
    # This allows the same article to be linked to multiple officers
    if url and badge and url_badge_pair not in existing_url_badge_pairs:
        article["article_id"] = next_article_id
        next_article_id += 1
        new_articles.append(article)
        existing_url_badge_pairs.add(url_badge_pair)  # Track for this batch
    else:
        duplicate_count += 1
        logging.debug(f"Articles: Skipping duplicate article: {url} for badge {badge}")

    logging.info(f"Articles: {len(new_articles)} new articles, {duplicate_count} duplicates skipped")

    # Combine existing + new articles
    combined_articles = existing_articles + new_articles

    # Write articles.csv
    if combined_articles:
        articles_written = save_articles_csv(articles_csv_path, combined_articles)
        logging.info(f"Articles CSV file ({articles_csv_path}): {articles_written} rows")
    else:
        logging.info("Articles: No articles to write")

# === Final Summary ===
if enrich_mode:
    logging.info(f"=== THOTH ENRICH MODE Complete ===")
    logging.info("Enrichment CSV ready for HERMES enrich_from_deltas.sh")
else:
    logging.info(f"=== THOTH Mission Complete ===")
    logging.info(f"Monthly CSV file ({csv_path}): {monthly_written} rows")
    logging.info(f"Local CSV file ({local_csv_path}): {local_written} rows")
    logging.info("Successful Operation - Ready for HERMES ETL")
