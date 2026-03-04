import datetime as dt
import logging
import time
import sys
import re
import html as html_lib
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import requests

# Ensure the script can locate the 'src' module when run from the root directory
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils import (
    build_vietnamese_name_key,
    load_config,
    load_tickers,
    map_role_to_category,
    normalize_person_name,
    normalize_ticker,
)

logger = logging.getLogger(__name__)

def fetch_vietstock_html(ticker: str, timeout: int, max_retries: int, retry_delay: float) -> str:
    """
    Fetches raw HTML from Vietstock using session management and header spoofing.
    
    Why this approach?
    Vietstock employs CSRF tokens and tracks sessions. Using requests.Session() 
    helps maintain TCP connection pooling and persists necessary cookies across retries.
    """
    url = f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm"
    
    # Standard browser headers to avoid basic bot detection
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm",
        "Connection": "keep-alive"
    }
    
    session = requests.Session()
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                # Soft Block Detection: Vietstock might return a 200 OK status 
                # but with an empty or drastically stripped HTML body if rate limits are hit.
                if len(resp.text) < 1000:
                    logger.warning(f"Vietstock returned suspiciously short content for {ticker}. Possible soft block.")
                    time.sleep(retry_delay) # Extended backoff
                    continue
                return resp.text
        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}")
            
        time.sleep(retry_delay) # Standard retry delay
    return ""

def parse_board_html(html_text: str, ticker: str) -> List[Dict[str, Any]]:
    """
    Extracts tabular data using targeted string manipulation and regex.
    
    Why manual parsing instead of BeautifulSoup?
    Financial sites often contain malformed or deeply nested HTML tables that can 
    cause DOM parsers to fail. Direct string targeting provides rigid control over extraction.
    """
    if not html_text: return []
    
    # Isolate the main table container
    table_start = html_text.find('<div class="table-container')
    if table_start == -1: return []
    table_end = html_text.find('</table>', table_start)
    table_html = html_text[table_start:table_end]
    
    records = []
    current_report_date = None 
    rows = table_html.split('<tr')[1:] # Skip the first split artifact
    
    for row in rows:
        cols = row.split('<td')
        if len(cols) < 2: continue
            
        clean_cols = []
        for col in cols[1:]: 
            idx = col.find('>')
            content = col[idx+1:] if idx != -1 else col
            content = re.sub(r'<[^>]+>', '', content) # Strip all HTML tags
            content = html_lib.unescape(content).strip()
            clean_cols.append(content)
            
        if not clean_cols: continue
            
        # Handle rowspan logic where the first column (date) spans multiple rows
        if re.search(r'\d{2}/\d{2}/\d{4}', clean_cols[0]):
            current_report_date = clean_cols[0]
            offset = 1 # Shift index to accommodate the date column
        else:
            offset = 0 # No date column in this row
        
        try:
            raw_name = clean_cols[offset] if len(clean_cols) > offset else ""
            # Skip invalid, empty, or placeholder rows right at the parsing stage
            if not raw_name or raw_name == "*** ***" or "Họ và tên" in raw_name:
                continue
                
            records.append({
                "ticker": ticker,
                "report_date": current_report_date,
                "person_name_raw": raw_name,
                "role_title_raw": clean_cols[offset + 1] if len(clean_cols) > offset + 1 else "",
                "yob_raw": clean_cols[offset + 2] if len(clean_cols) > offset + 2 else "",
                "edu_raw": clean_cols[offset + 3] if len(clean_cols) > offset + 3 else "",
                "shares_raw": clean_cols[offset + 4] if len(clean_cols) > offset + 4 else "",
                "tenure_raw": clean_cols[offset + 5] if len(clean_cols) > offset + 5 else ""
            })
        except Exception:
            continue
            
    return records

def clean_placeholder(value: str) -> str | None:
    """
    Sanitizes undisclosed or restricted data represented by placeholders.
    
    Why is this critical?
    Vietstock hides data using '***' or '-'. If left uncleaned, these strings will 
    force downstream Pandas numeric columns (like 'shares' or 'age') into 'object' 
    types, violating the Parquet schema requirements.
    """
    if not value:
        return None
    v = value.strip()
    # List of known garbage strings used by Vietstock
    placeholders = ["***", "-", "*** ***", "N/A", "Chưa cập nhật"]
    if v in placeholders or not v:
        return None
    return v

def normalize_records(records: List[Dict[str, Any]], exchange: str) -> pd.DataFrame:
    """
    Applies Entity Resolution, calculates metrics, and enforces strict data types.
    """
    if not records: return pd.DataFrame()
    
    ts = dt.datetime.now(dt.timezone.utc)
    curr_year = dt.datetime.now().year
    rows = []

    for r in records:
        # Validate primary entity; drop record if identity is completely masked
        raw_name = clean_placeholder(r["person_name_raw"])
        if not raw_name:
            continue
            
        # Strip generic honorifics before passing to the canonical normalizer
        clean_name = re.sub(r'^(Ông|Bà)\s+', '', raw_name, flags=re.IGNORECASE)
        
        # Transform Year of Birth (YOB) into Age metric
        yob_str = clean_placeholder(r["yob_raw"])
        age = None
        if yob_str and yob_str.isdigit():
            yob_val = int(yob_str)
            # Basic sanity check for valid human ages
            if 1940 < yob_val < curr_year:
                age = curr_year - yob_val
        
        # Strip thousands separators (commas) to safely cast shares to numeric
        shares_str = clean_placeholder(r.get("shares_raw", "0"))
        shares_val = 0
        if shares_str:
            shares_str = shares_str.replace(",", "")
            shares_val = int(shares_str) if shares_str.isdigit() else 0

        rows.append({
            "ticker": normalize_ticker(r["ticker"]),
            "exchange": exchange,
            "person_name": raw_name,
            "person_name_canonical": normalize_person_name(clean_name),
            "person_name_key": build_vietnamese_name_key(normalize_person_name(clean_name)),
            "role": clean_placeholder(r["role_title_raw"]),
            "role_category": map_role_to_category(r["role_title_raw"]),
            "source": "vietstock",
            "scraped_at": ts,
            "age": age,
            "education": clean_placeholder(r.get("edu_raw")),
            "shares": shares_val,
            "tenure": clean_placeholder(r.get("tenure_raw"))
        })
    
    df = pd.DataFrame(rows)
    if not df.empty:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    return df

def main():
    config = load_config(project_root / "config.yaml")

    scraping_cfg = config["scraping"]["vietstock"]

    request_delay = scraping_cfg["request_delay_seconds"]
    retry_delay = scraping_cfg["retry_delay_seconds"]
    timeout = scraping_cfg["timeout_seconds"]
    max_retries = scraping_cfg["max_retries"]
    
    raw_dir = project_root / "data" / "raw" / "vietstock"
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    tickers_df = pd.read_csv(config["tickers"]["file"])
    ticker_map = tickers_df.set_index("ticker")["exchange"].to_dict()
    
    all_dfs = []
    tickers_failed = []

    for ticker in load_tickers(config):
        logger.info(f"Vietstock scraping ticker={ticker}")
        html = fetch_vietstock_html(
                                    ticker=ticker,
                                    timeout=timeout,
                                    max_retries=max_retries,
                                    retry_delay=retry_delay)
        
        if html:
            # Save raw HTML snapshot for data provenance and debugging audit trails
            ts_str = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            raw_html_path = raw_dir / f"{ticker}_{ts_str}.html"
            raw_html_path.write_text(html, encoding="utf-8")

            recs = parse_board_html(html, ticker)
            if recs:
                df = normalize_records(recs, ticker_map.get(ticker, "HOSE"))
                if not df.empty:
                    all_dfs.append(df)
            else:
                tickers_failed.append(ticker)
        else:
            tickers_failed.append(ticker)
            
        time.sleep(request_delay) # Mandatory rate-limiting compliance

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
    else:
        combined = pd.DataFrame()

    # Fallback: Create an empty DataFrame with the exact schema.
    # This acts as a defensive programming mechanism to prevent Task 3 (Merge) 
    # from crashing via KeyError if Vietstock completely blocks the scraper.
    if combined.empty:
        cols = [
            "ticker", "exchange", "report_date", "person_name", "person_name_canonical", 
            "person_name_key", "role", "role_category", "source", "scraped_at", 
            "age", "education", "shares", "tenure"
        ]
        combined = pd.DataFrame(columns=cols)

    # Output directly to the required evaluation path
    output_path = project_root / "data" / "raw" / "vietstock_board.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    
    logger.info(f"Task 2 Complete! Output Parquet: {output_path}")
    logger.info(f"Total records processed: {len(combined)}")
    if tickers_failed:
        logger.warning(f"Failed to fetch or parse tickers: {', '.join(tickers_failed)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()