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

# Đảm bảo script tìm thấy module src
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

def fetch_cafef_api(ticker: str) -> str:
    """Tải dữ liệu bằng API với Header giả lập trình duyệt chuyên sâu."""
    url = f"https://s.cafef.vn/Ajax/PageNew/ListCeo.ashx?Symbol={ticker}&PositionGroup=0"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "X-Requested-With": "XMLHttpRequest",
        # QUAN TRỌNG: Referer phải khớp với ticker đang cào
        "Referer": f"https://s.cafef.vn/hoso/cong-ty/{ticker}/ban-lanh-dao-cong-ty.chn",
        "Connection": "keep-alive"
    }
    
    # Sử dụng Session để giữ Cookie (nếu có) qua các lần retry
    session = requests.Session()
    
    for attempt in range(3):
        try:
            resp = session.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                # Kiểm tra nội dung rỗng giả (Success:true nhưng Data:[])
                if '"Data":[]' in resp.text:
                    logger.warning(f"CafeF returned empty data for {ticker}. Likely a soft block. (Attempt {attempt+1}/3)")
                    # Nghỉ lâu hơn và đổi User-Agent nếu cần
                    time.sleep(7) 
                    continue
                return resp.text
            elif resp.status_code == 403:
                logger.error(f"IP Blocked by CafeF for {ticker}. Need to change IP or wait.")
                break
        except Exception as e:
            logger.error(f"Network error on {ticker}: {e}")
        
        time.sleep(2)
    return ""

def parse_cafef_board(json_str: str, ticker: str) -> List[Dict[str, Any]]:
    """Trích xuất dữ liệu từ JSON."""
    if not json_str: return []
    try:
        data_list = json.loads(json_str).get("Data") or []
    except: return []

    records = []
    for group in data_list:
        values = group.get("values")
        if not isinstance(values, list): continue
        for p in values:
            name = (p.get("Name") or "").strip()
            if not name: continue
            
            edu = ""
            schools = p.get("CeoSchools")
            if isinstance(schools, list) and len(schools) > 0:
                edu = (schools[0].get("CeoTitle") or "").strip()

            records.append({
                "ticker": ticker,
                "person_name_raw": name,
                "role_title_raw": (p.get("Position") or "").strip(),
                "age_raw": str(p.get("old", 0)),
                "education_raw": edu
            })
    return records

def normalize_cafef_records(records: List[Dict[str, Any]], exchange: str) -> pd.DataFrame:
    """Chuẩn hóa dữ liệu và ép kiểu datetime chuẩn Schema."""
    if not records: return pd.DataFrame()
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    
    for rec in records:
        p_raw = rec["person_name_raw"]
        p_can = normalize_person_name(p_raw)
        
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
        # Ép kiểu datetime để đúng với Required Output Schema của Task 1
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    return df

def main():
    config = load_config(project_root / "config.yaml")
    
    # Chỉ định nghĩa thư mục raw
    raw_dir = project_root / "data" / "raw" / "cafef"
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    tickers_df = pd.read_csv(config["tickers"]["file"])
    ticker_map = tickers_df.set_index("ticker")["exchange"].to_dict()
    
    all_dfs = []
    tickers_failed = []
    
    for ticker in load_tickers(config):
        logger.info(f"CafeF scraping ticker={ticker}")
        json_data = fetch_cafef_api(ticker)
        
        if json_data:
            # Lưu snapshot snapshot JSON để audit
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
                
        time.sleep(1.5) # Be respectful [cite: 37]

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
    else:
        combined = pd.DataFrame()

    # Fallback tạo DataFrame rỗng chuẩn schema nếu bị lỗi hoàn toàn (tránh sập Task 3)
    if combined.empty:
        cols = [
            "ticker", "exchange", "person_name", "person_name_canonical", 
            "person_name_key", "role", "role_category", "source", "scraped_at", 
            "age", "education"
        ]
        combined = pd.DataFrame(columns=cols)

    # LƯU CHÍNH XÁC THEO LỆNH EVALUATION CỦA ĐỀ BÀI
    output_path = project_root / "data" / "raw" / "cafef_board.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
            
    logger.info(f"Task 1 Complete! Output Parquet: {output_path}")
    logger.info(f"Total records processed: {len(combined)}")
    if tickers_failed:
        logger.warning(f"Failed to fetch or parse tickers: {', '.join(tickers_failed)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    main()