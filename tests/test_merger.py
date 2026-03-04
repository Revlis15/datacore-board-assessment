import unittest
import pandas as pd
import sys
from pathlib import Path

# Thêm project root vào sys.path để import từ src
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.merge import squash_duplicates, resolve_conflicts

class TestMerger(unittest.TestCase):

    def test_squash_duplicates(self):
        """Kiểm tra gộp 1 người có nhiều chức vụ thành 1 dòng duy nhất."""
        data = [
            {'ticker': 'AAA', 'exchange': 'HOSE', 'person_name_canonical': 'nguyen van a', 
             'person_name_key': 'nguyenvana', 'role': 'Chủ tịch HĐQT', 'age': 50},
            {'ticker': 'AAA', 'exchange': 'HOSE', 'person_name_canonical': 'nguyen van a', 
             'person_name_key': 'nguyenvana', 'role': 'Tổng Giám đốc', 'age': 50}
        ]
        df = pd.DataFrame(data)
        result = squash_duplicates(df)
        
        # Kết quả phải chỉ còn 1 dòng
        self.assertEqual(len(result), 1)
        # Chức vụ phải được nối lại bằng dấu /
        self.assertIn("Chủ tịch HĐQT / Tổng Giám đốc", result.iloc[0]['role'])

    def test_resolve_conflicts_both_sources(self):
        """Kiểm tra ưu tiên CafeF và lấp đầy dữ liệu từ Vietstock."""
        # Giả lập 1 dòng sau khi đã merge (có cột _merge)
        row_data = {
            '_merge': 'both',
            'person_name_cafef': 'Nguyễn Văn A',
            'role_cafef': 'Chủ tịch',
            'age_cafef': None,
            'education_cafef': 'Thạc sĩ',
            'person_name_vst': 'Ông Nguyễn Văn A',
            'role_vst': 'Thành viên',
            'age_vst': 55.0,
            'education_vst': 'Tiến sĩ',
            'shares': 1000,
            'tenure': '2020-2025'
        }
        row = pd.Series(row_data)
        
        result = resolve_conflicts(row)
        
        # 1. Tuổi phải lấy từ Vietstock vì CafeF bị None
        self.assertEqual(result['age_golden'], 55.0)
        # 2. Học vấn ưu tiên Vietstock (theo logic file merge của bạn)
        self.assertEqual(result['education_golden'], 'Tiến sĩ')
        # 3. Trạng thái phải là conflict vì role 'Chủ tịch' != 'Thành viên'
        self.assertEqual(result['source_agreement'], 'conflict')
        self.assertEqual(result['confidence_score'], 0.6)

if __name__ == '__main__':
    unittest.main()