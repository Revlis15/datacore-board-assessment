import re
import yaml
import logging
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Loads configuration from a YAML file and validates its structure.
    
    Why: Implements a fail-fast mechanism. Validating top-level keys immediately 
    prevents downstream pipeline crashes caused by missing configuration paths.
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        loaded = yaml.safe_load(f)
    
    # Fail-fast validation check
    missing_keys = [key for key in ("paths",) if key not in loaded]
    if missing_keys:
        raise ValueError(f"Config missing required top-level keys: {', '.join(missing_keys)}")
    return loaded

def load_tickers(config: Dict[str, Any]) -> List[str]:
    """
    Loads and returns the list of target tickers to process.
    
    Why: Externalizing the ticker list allows the pipeline to scale dynamically.
    The 'max_tickers' parameter supports rapid PoC testing and debugging without 
    running the entire market universe.
    """
    import pandas as pd
    ticker_file = Path(config["tickers"]["file"])
    if not ticker_file.exists():
        logger.error(f"Ticker file not found: {ticker_file}")
        return []
    
    df = pd.read_csv(ticker_file)
    tickers = df["ticker"].unique().tolist()
    
    # Support fast testing if max_tickers is defined in config.yaml
    max_t = config["tickers"].get("max_tickers")
    return tickers[:max_t] if max_t else tickers

def normalize_ticker(ticker: Any) -> str:
    """
    Standardizes ticker formatting (uppercase, stripped).
    """
    return str(ticker).strip().upper()

def normalize_person_name(name: str) -> str:
    """
    Normalizes Vietnamese names for Entity Resolution.
    Strips honorifics, handles Vietnamese diacritics, and converts to lowercase.
    
    Why: Data sources frequently mix honorifics (e.g., 'Ông', 'GS.TS') with names. 
    Standardizing the string ensures accurate cross-source merging.
    """
    if not name or name == "null":
        return ""
    
    # 1. Lowercase for uniform processing
    name = name.lower().strip()
    
    # 2. MANUAL REPLACEMENT FOR 'đ': 
    # Why: The Vietnamese 'đ' (Latin Small Letter D with Stroke) does not decompose 
    # into 'd' + diacritic under Unicode NFD normalization. It must be explicitly mapped.
    name = name.replace('đ', 'd')
    
    # 3. Strip complex honorifics and academic titles
    honorifics_pattern = r'^((ông|bà|anh|chị|em|ts|ths|gs|pgs|dr|mr|ms|mrs|th\.s|gs\.ts|pgs\.ts)[\.\s]*)+'
    name = re.sub(honorifics_pattern, '', name)
    
    # 4. Remove Vietnamese diacritics using Unicode NFD decomposition
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    
    # 5. Clean up any remaining special characters
    name = re.sub(r'[^a-z\s]', '', name)
    return " ".join(name.split())

def build_vietnamese_name_key(normalized_name: str) -> str:
    """
    Generates a deterministic joining key by removing whitespace.
    
    Why: Protects the merge logic from inconsistent spacing variations 
    (e.g., "Nguyen Van A" vs. "Nguyen  Van A") between different platforms.
    """
    return normalized_name.replace(" ", "")

def map_role_to_category(raw_title: str) -> str:
    """
    Maps raw Vietnamese job titles to standardized English corporate governance roles.
    
    Why: Source systems use highly variable job titles (e.g., 'Chủ tịch HĐQT', 'Chủ tịch').
    Categorizing these into standard English buckets (CHAIRMAN, CEO, SUPERVISOR) 
    makes the dataset immediately queryable for downstream analytics.
    """
    role = str(raw_title).lower().strip()
    if not role or role == "null":
        return "OTHER"
        
    # --- Group 1: Board of Directors (HĐQT) ---
    if re.search(r"chủ tịch.*hđqt|chu tich.*hdqt", role):
        return "VICE_CHAIRMAN" if re.search(r"phó|pho", role) else "CHAIRMAN"
    
    if re.search(r"thành viên.*hđqt|thanh vien.*hdqt|ủy viên.*hđqt|uy vien.*hdqt|hđqt", role):
        return "DIRECTOR"
        
    # --- Group 2: Executive Management (CEO & Deputies) ---
    if re.search(r"phó.*(tổng giám đốc|tgđ|tgd)", role):
        if re.search(r"tài chính|kế toán|cfo", role):
            return "DEPUTY_CEO_FINANCE"
        if re.search(r"sản xuất|vận hành", role):
            return "DEPUTY_CEO_OPERATIONS"
        if re.search(r"thường trực", role):
            return "DEPUTY_CEO_PERMANENT"
        return "DEPUTY_CEO"
        
    if re.search(r"tổng giám đốc|tong giam doc|ceo|tgđ|tgd", role):
        return "CEO"

    # --- Group 3: Supervisory & Audit (UBKTNB) ---
    # Prioritizes capturing the Head ('Trưởng') of the committee
    if re.search(r"trưởng ban.*kiểm soát|trưởng bks|trưởng ban.*kiểm toán|trưởng.*ubktnb|trưởng.*ủy ban kiểm tra nội bộ", role):
        return "SUPERVISOR_HEAD"
    
    if re.search(r"thành viên.*kiểm soát|thành viên.*bks|kiểm soát viên|kiểm toán.*nội bộ|thành viên.*kiểm toán|ubktnb|ủy ban kiểm tra nội bộ", role):
        return "SUPERVISOR"

    # --- Group 4: Governance & Legal ---
    if re.search(r"phụ trách quản trị|công bố thông tin|thư ký", role):
        return "GOVERNANCE_OFFICER"

    # --- Group 5: Finance & Accounting ---
    if re.search(r"kế toán trưởng|ke toan truong|ktt", role):
        return "CHIEF_ACCOUNTANT"
    if re.search(r"giám đốc tài chính|cfo", role):
        return "CFO"
        
    # --- Group 6: Other Executive Directors ---
    if re.search(r"giám đốc|giam doc|gđ|gd", role):
        return "DEPUTY_DIRECTOR" if re.search(r"phó|pho", role) else "DIRECTOR_EXEC"

    return "OTHER"