import re
import yaml
import logging
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def load_config(config_path: Path) -> Dict[str, Any]:
    """Load configuration from YAML file and validate structure."""
    with open(config_path, 'r', encoding='utf-8') as f:
        loaded = yaml.safe_load(f)
    
    # Giữ nguyên checks này để tránh sập hệ thống như đã thảo luận
    missing_keys = [key for key in ("paths",) if key not in loaded]
    if missing_keys:
        raise ValueError(f"Config missing required top-level keys: {', '.join(missing_keys)}")
    return loaded

def load_tickers(config: Dict[str, Any]) -> List[str]:
    """Load and return the list of tickers to process."""
    import pandas as pd
    ticker_file = Path(config["tickers"]["file"])
    if not ticker_file.exists():
        logger.error(f"Ticker file not found: {ticker_file}")
        return []
    
    df = pd.read_csv(ticker_file)
    tickers = df["ticker"].unique().tolist()
    
    # Hỗ trợ test nhanh nếu có max_tickers trong config
    max_t = config["tickers"].get("max_tickers")
    return tickers[:max_t] if max_t else tickers

def normalize_ticker(ticker: Any) -> str:
    """Standardize ticker format."""
    return str(ticker).strip().upper()

def normalize_person_name(name: str) -> str:
    """
    Chuẩn hóa tên người: Xóa danh xưng, xử lý chữ Đ, xóa dấu và chuyển về lowercase.
    Sửa lỗi: 'Đinh Bộ Lễ' -> 'dinh bo le' (không bị mất chữ d).
    """
    if not name or name == "null":
        return ""
    
    # 1. Chuyển về chữ thường trước
    name = name.lower().strip()
    
    # 2. XỬ LÝ RIÊNG CHỮ Đ: Phải làm trước khi xóa dấu Unicode
    # Chữ 'đ' thường không bị tách dấu theo chuẩn NFD nên cần replace thủ công
    name = name.replace('đ', 'd')
    
    # 3. Xóa danh xưng (Honorifics)
    honorifics_pattern = r'^((ông|bà|anh|chị|em|ts|ths|gs|pgs|dr|mr|ms|mrs|th\.s|gs\.ts|pgs\.ts)[\.\s]*)+'
    name = re.sub(honorifics_pattern, '', name)
    
    # 4. Xóa dấu tiếng Việt bằng chuẩn NFD
    name = unicodedata.normalize('NFD', name)
    name = "".join(c for c in name if unicodedata.category(c) != 'Mn')
    
    # 5. Dọn dẹp ký tự đặc biệt còn sót lại
    name = re.sub(r'[^a-z\s]', '', name)
    return " ".join(name.split())

def build_vietnamese_name_key(normalized_name: str) -> str:
    """Tạo key không khoảng trắng để so khớp (Deterministic Key)."""
    return normalized_name.replace(" ", "")

def map_role_to_category(raw_title: str) -> str:
    """
    Map raw Vietnamese job titles to standardized English roles using Regex.
    Đã bổ sung: Trưởng/Thành viên UBKTNB (Ủy ban Kiểm tra Nội bộ).
    """
    role = str(raw_title).lower().strip()
    if not role or role == "null":
        return "OTHER"
        
    # --- Nhóm 1: Hội đồng quản trị (HĐQT) ---
    if re.search(r"chủ tịch.*hđqt|chu tich.*hdqt", role):
        return "VICE_CHAIRMAN" if re.search(r"phó|pho", role) else "CHAIRMAN"
    
    if re.search(r"thành viên.*hđqt|thanh vien.*hdqt|ủy viên.*hđqt|uy vien.*hdqt|hđqt", role):
        return "DIRECTOR"
        
    # --- Nhóm 2: Ban Điều hành cấp cao (CEO & Phó) ---
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

    # --- Nhóm 3: Kiểm soát & Kiểm toán (Bao gồm UBKTNB) ---
    # Ưu tiên bắt cấp Trưởng (có 'trưởng' và 'ubktnb' hoặc 'bks')
    if re.search(r"trưởng ban.*kiểm soát|trưởng bks|trưởng ban.*kiểm toán|trưởng.*ubktnb|trưởng.*ủy ban kiểm tra nội bộ", role):
        return "SUPERVISOR_HEAD"
    
    # Thành viên kiểm soát / kiểm toán / UBKTNB
    if re.search(r"thành viên.*kiểm soát|thành viên.*bks|kiểm soát viên|kiểm toán.*nội bộ|thành viên.*kiểm toán|ubktnb|ủy ban kiểm tra nội bộ", role):
        return "SUPERVISOR"

    # --- Nhóm 4: Quản trị & Pháp định ---
    if re.search(r"phụ trách quản trị|công bố thông tin|thư ký", role):
        return "GOVERNANCE_OFFICER"

    # --- Nhóm 5: Tài chính & Kế toán ---
    if re.search(r"kế toán trưởng|ke toan truong|ktt", role):
        return "CHIEF_ACCOUNTANT"
    if re.search(r"giám đốc tài chính|cfo", role):
        return "CFO"
        
    # --- Nhóm 6: Các vị trí Giám đốc/Quản lý khác ---
    if re.search(r"giám đốc|giam doc|gđ|gd", role):
        return "DEPUTY_DIRECTOR" if re.search(r"phó|pho", role) else "DIRECTOR_EXEC"

    return "OTHER"