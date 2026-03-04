import logging
from pathlib import Path
import pandas as pd

# Định vị thư mục gốc
project_root = Path(__file__).resolve().parents[1]

def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

logger = setup_logger()

def load_datasets(raw_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Đọc dữ liệu từ Task 1 và Task 2."""
    df_cafef = pd.DataFrame()
    df_vst = pd.DataFrame()
    
    cafef_path = raw_dir / "cafef_board.parquet"
    vst_path = raw_dir / "vietstock_board.parquet"
    
    if cafef_path.exists():
        df_cafef = pd.read_parquet(cafef_path)
    if vst_path.exists():
        df_vst = pd.read_parquet(vst_path)
        
    return df_cafef, df_vst

def squash_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    YÊU CẦU: "produce a single golden row"
    Hàm này gộp các chức vụ kiêm nhiệm của cùng 1 người thành 1 dòng duy nhất.
    Ví dụ: 'Thành viên HĐQT' và 'Phó TGĐ' -> 'Thành viên HĐQT / Phó TGĐ'
    """
    if df.empty: return df
    
    group_cols = ['ticker', 'exchange', 'person_name_canonical', 'person_name_key']
    
    # Lấy giá trị đầu tiên cho các cột thông thường
    agg_dict = {col: 'first' for col in df.columns if col not in group_cols and col != 'role'}
    
    # Riêng cột role: nối chuỗi các chức vụ lại với nhau
    agg_dict['role'] = lambda x: ' / '.join(pd.Series(x).dropna().astype(str).unique())
    
    return df.groupby(group_cols, as_index=False).agg(agg_dict)

def resolve_conflicts(row: pd.Series) -> pd.Series:
    """
    Quy tắc giải quyết xung đột (Bám sát yêu cầu mục 3b):
    1. Tên (person_name): Ưu tiên CafeF.
    2. Chức vụ (role): Ưu tiên CafeF.
    3. Trạng thái (source_agreement): both, conflict, cafef_only, vietstock_only.
    """
    merge_status = row['_merge']
    
    # 1. Trạng thái: Chỉ có ở CafeF
    if merge_status == 'left_only':
        row['source_agreement'] = 'cafef_only'
        row['confidence_score'] = 0.8
        row['person_name_golden'] = row['person_name_cafef']
        row['role_golden'] = row['role_cafef']
        row['age_golden'] = row['age_cafef']
        row['education_golden'] = row['education_cafef']
        
    # 2. Trạng thái: Chỉ có ở Vietstock
    elif merge_status == 'right_only':
        row['source_agreement'] = 'vietstock_only'
        row['confidence_score'] = 0.8
        row['person_name_golden'] = row['person_name_vst']
        row['role_golden'] = row['role_vst']
        row['age_golden'] = row['age_vst']
        row['education_golden'] = row['education_vst']
        
    # 3. Trạng thái: Có ở cả 2 nguồn (both / conflict)
    else:
        row['person_name_golden'] = row['person_name_cafef'] 
        
        # Vietstock có nhiều dữ liệu bổ sung hơn, nên lấy từ Vietstock làm chuẩn
        row['age_golden'] = row['age_vst'] if pd.notna(row['age_vst']) else row['age_cafef']
        row['education_golden'] = row['education_vst'] if pd.notna(row['education_vst']) and str(row['education_vst']).strip() != "" else row['education_cafef']

        # So sánh chức danh đã nối chuỗi
        role_cafef = str(row['role_cafef']).lower()
        role_vst = str(row['role_vst']).lower()
        
        # Nếu chức danh bao hàm lẫn nhau (hoặc giống nhau) thì coi như đồng thuận
        if role_cafef == role_vst or role_cafef in role_vst or role_vst in role_cafef:
            row['source_agreement'] = 'both'
            row['confidence_score'] = 1.0
            # Strategy: Lấy CafeF làm chuẩn, trừ khi Vietstock chi tiết hơn
            row['role_golden'] = row['role_cafef'] if len(role_cafef) >= len(role_vst) else row['role_vst']
        else:
            row['source_agreement'] = 'conflict'
            row['confidence_score'] = 0.6
            row['role_golden'] = row['role_cafef'] # Strategy: Lấy CafeF khi xung đột

    return row

def main():
    raw_dir = project_root / "data" / "raw"
    final_dir = project_root / "data" / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting Task 3: Merge & Deduplicate datasets...")
    df_cafef, df_vst = load_datasets(raw_dir)
    
    if df_cafef.empty and df_vst.empty:
        logger.error("Cả 2 nguồn đều không có dữ liệu để gộp!")
        return

    # BƯỚC QUAN TRỌNG: Gộp kiêm nhiệm để đảm bảo 1 người chỉ có 1 dòng (Single Golden Row)
    df_cafef = squash_duplicates(df_cafef)
    df_vst = squash_duplicates(df_vst)

    join_cols = ['ticker', 'exchange', 'person_name_key', 'person_name_canonical']
    
    # Mở rộng (Outer Join)
    merged_df = pd.merge(
        df_cafef, 
        df_vst, 
        on=join_cols, 
        how='outer', 
        suffixes=('_cafef', '_vst'),
        indicator=True
    )
    
    # Bổ sung các cột chỉ có ở Vietstock nếu chưa có
    for col in ['shares', 'tenure']:
        if col not in merged_df.columns:
            merged_df[col] = None

    # Áp dụng logic
    golden_df = merged_df.apply(resolve_conflicts, axis=1)
    
    # Lấy các cột Bonus
    golden_df['shares_golden'] = golden_df['shares'] if 'shares' in golden_df.columns else None
    golden_df['tenure_golden'] = golden_df['tenure'] if 'tenure' in golden_df.columns else None
    
    # Định hình lại cột theo Data Dictionary
    final_columns = [
        'ticker', 'exchange', 'person_name_golden', 'person_name_canonical', 
        'role_golden', 'age_golden', 'education_golden', 'shares_golden', 'tenure_golden',
        'source_agreement', 'confidence_score'
    ]
    
    golden_df = golden_df[final_columns].rename(columns={
        'person_name_golden': 'person_name',
        'role_golden': 'role',
        'age_golden': 'age',
        'education_golden': 'education',
        'shares_golden': 'shares',
        'tenure_golden': 'tenure'
    })
    
    # LƯU FILE GOLDEN VÀO ĐÚNG ĐƯỜNG DẪN YÊU CẦU
    output_path = final_dir / "board_golden.parquet"
    golden_df.to_parquet(output_path, index=False)
    
    logger.info(f"Task 3 Complete! Golden dataset generated at: {output_path}")
    
    # Báo cáo
    logger.info("-" * 30)
    logger.info(" QUALITY REPORT STATS ")
    logger.info(f"Total Unique Golden Records: {len(golden_df)}")
    logger.info("Source Agreement Distribution:")
    for key, val in golden_df['source_agreement'].value_counts().items():
        logger.info(f" - {key}: {val} rows ({val/len(golden_df)*100:.1f}%)")
    logger.info("-" * 30)

if __name__ == "__main__":
    main()