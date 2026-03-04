from __future__ import annotations
import datetime as dt
import logging
import time
import json
import sys
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

def fetch_cafef_api(ticker: str, timeout: int, max_retries: int, retry_delay: float) -> str:
    """
    Fetches board data directly from CafeF's internal AJAX endpoint.
    
    Why AJAX instead of HTML scraping?
    - The JSON response is cleaner, faster, and avoids complex BeautifulSoup parsing.
    - We bypass dynamic rendering issues caused by JavaScript on the main page.
    """
    url = f"https://s.cafef.vn/Ajax/PageNew/ListCeo.ashx?Symbol={ticker}&PositionGroup=0"
    
    # Comprehensive header spoofing to mimic a real browser request and avoid 403 Forbidden errors.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "X-Requested-With": "XMLHttpRequest", # Crucial for AJAX endpoint validation
        # IMPORTANT: Referer must dynamically match the target ticker to pass origin checks
        "Referer": f"https://s.cafef.vn/hoso/cong-ty/{ticker}/ban-lanh-dao-cong-ty.chn",
        "Connection": "keep-alive"
    }
    
    # Use requests.Session() to persist cookies (if any are set by the server) across retries
    session = requests.Session()
    
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                # Detect soft blocks: CafeF sometimes returns a 200 OK but with empty "Data":[] 
                # when it detects high request frequencies.
                if '"Data":[]' in resp.text:
                    logger.warning(f"CafeF returned empty data for {ticker}. Likely a soft block. (Attempt {attempt+1}/{max_retries})")
                    # Apply exponential backoff to recover from soft block
                    time.sleep(retry_delay) 
                    continue
                return resp.text
            elif resp.status_code == 403:
                logger.error(f"IP Blocked by CafeF for {ticker}. Need to change IP or wait.")
                break
        except Exception as e:
            logger.error(f"Network error on {ticker}: {e}")
        
        # Standard delay before retry
        time.sleep(retry_delay)
    return ""

def parse_cafef_board(json_str: str, ticker: str) -> List[Dict[str, Any]]:
    """
    Extracts relevant leadership fields from the deeply nested CafeF JSON structure.
    """
    if not json_str: return []
    try:
        data_list = json.loads(json_str).get("Data") or []
    except json.JSONDecodeError: 
        logger.error(f"Failed to decode JSON for {ticker}")
        return []

    records = []
    # Data is grouped by role tiers (e.g., Board of Directors, Management Board)
    for group in data_list:
        values = group.get("values")
        if not isinstance(values, list): continue
        for p in values:
            name = (p.get("Name") or "").strip()
            if not name: continue
            
            # Education info is often nested inside an array of CeoSchools objects
            edu = ""
            schools = p.get("CeoSchools")
            if isinstance(schools, list) and len(schools) > 0:
                edu = (schools[0].get("CeoTitle") or "").strip()

            records.append({
                "ticker": ticker,
                "person_name_raw": name,
                "role_title_raw": (p.get("Position") or "").strip(),
                "age_raw": str(p.get("old", 0)), # 'old' field represents age in CafeF
                "education_raw": edu
            })
    return records

def normalize_cafef_records(records: List[Dict[str, Any]], exchange: str) -> pd.DataFrame:
    """
    Applies Entity Resolution (Name Normalization) and Schema Type Casting.
    Ensures output strictly matches the required DataCore schema.
    """
    if not records: return pd.DataFrame()
    
    # Generate ISO 8601 timestamp per requirements
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    
    for rec in records:
        p_raw = rec["person_name_raw"]
        p_can = normalize_person_name(p_raw)
        
        # Safe casting for age to avoid ValueError on empty or invalid strings
        age_raw = rec["age_raw"]
        age_val = int(age_raw) if age_raw and age_raw.isdigit() and int(age_raw) > 0 else None
        
        rows.append({
            "ticker": normalize_ticker(rec["ticker"]),
            "exchange": exchange,
            "person_name": p_raw,
            "person_name_canonical": p_can,
            "person_name_key": build_vietnamese_name_key(p_can),
            "role": rec["role_title_raw"],
            "role_category": map_role_to_category(rec["role_title_raw"]),
            "source": "cafef",
            "scraped_at": ts,
            "age": age_val,
            "education": rec["education_raw"]
        })
    
    df = pd.DataFrame(rows)
    if not df.empty:
        # Cast to proper datetime format as mandated by the Target Schema
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    return df

def main():
    config = load_config(project_root / "config.yaml")

    scraping_cfg = config["scraping"]["cafef"]

    request_delay = scraping_cfg["request_delay_seconds"]
    retry_delay = scraping_cfg["retry_delay_seconds"]
    timeout = scraping_cfg["timeout_seconds"]
    max_retries = scraping_cfg["max_retries"]
    
    # Directory setup for raw JSON snapshots (useful for auditing and debugging)
    raw_dir = project_root / "data" / "raw" / "cafef"
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Load universe of tickers to process
    tickers_df = pd.read_csv(config["tickers"]["file"])
    ticker_map = tickers_df.set_index("ticker")["exchange"].to_dict()
    
    all_dfs = []
    tickers_failed = []
    
    for ticker in load_tickers(config):
        logger.info(f"CafeF scraping ticker={ticker}")
        json_data = fetch_cafef_api(
                                    ticker=ticker,
                                    timeout=timeout,
                                    max_retries=max_retries,
                                    retry_delay=retry_delay)   
        
        if json_data:
            # Save raw JSON snapshot for data provenance and troubleshooting
            ts_str = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            raw_file_path = raw_dir / f"{ticker}_{ts_str}.json"
            raw_file_path.write_text(json_data, encoding="utf-8")

            recs = parse_cafef_board(json_data, ticker)
            if recs:
                df = normalize_cafef_records(recs, ticker_map.get(ticker, "HOSE"))
                if not df.empty:
                    all_dfs.append(df)
            else:
                tickers_failed.append(ticker)
        else:
            tickers_failed.append(ticker)
                
        # Mandatory delay to be respectful to the source server
        time.sleep(request_delay) 

    # Combine all individual ticker DataFrames
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
    else:
        # Fallback: Create an empty DataFrame with the exact schema to prevent downstream pipeline crashes
        cols = [
            "ticker", "exchange", "person_name", "person_name_canonical", 
            "person_name_key", "role", "role_category", "source", "scraped_at", 
            "age", "education"
        ]
        combined = pd.DataFrame(columns=cols)

    # Output directly to data/raw/cafef_board.parquet per evaluation script requirements
    output_path = project_root / "data" / "raw" / "cafef_board.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
            
    logger.info(f"Task 1 Complete! Output Parquet: {output_path}")
    logger.info(f"Total records processed: {len(combined)}")
    if tickers_failed:
        logger.warning(f"Failed to fetch or parse tickers: {', '.join(tickers_failed)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()