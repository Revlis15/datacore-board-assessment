import unittest
import sys
from pathlib import Path

# Thêm thư mục gốc vào path để import được src
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from src.utils import normalize_person_name, build_vietnamese_name_key, map_role_to_category

class TestUtils(unittest.TestCase):

    def test_normalize_person_name_with_d(self):
        """Kiểm tra lỗi chữ Đ không bị biến mất."""
        case_1 = "Ông Đinh Bộ Lễ"
        case_2 = "Bà Đặng Thị Ngọc Thịnh"
        
        self.assertEqual(normalize_person_name(case_1), "dinh bo le")
        self.assertEqual(normalize_person_name(case_2), "dang thi ngoc thinh")

    def test_normalize_person_name_honorifics(self):
        """Kiểm tra việc loại bỏ danh xưng."""
        self.assertEqual(normalize_person_name("ThS. Nguyễn Văn A"), "nguyen van a")
        self.assertEqual(normalize_person_name("GS.TS Trần Văn B"), "tran van b")

    def test_name_key_consistency(self):
        """Kiểm tra tính nhất quán của khóa so khớp."""
        name_1 = "nguyen van a"
        name_2 = "Nguyen Van A"
        key_1 = build_vietnamese_name_key(normalize_person_name(name_1))
        key_2 = build_vietnamese_name_key(normalize_person_name(name_2))
        self.assertEqual(key_1, key_2)

    def test_map_role_ubktnb(self):
        """Kiểm tra việc phân loại các role mới thêm (UBKTNB)."""
        # Trưởng ban
        self.assertEqual(map_role_to_category("Trưởng ban UBKTNB"), "SUPERVISOR_HEAD")
        self.assertEqual(map_role_to_category("Trưởng ủy ban kiểm tra nội bộ"), "SUPERVISOR_HEAD")
        
        # Thành viên
        self.assertEqual(map_role_to_category("Thành viên UBKTNB"), "SUPERVISOR")
        self.assertEqual(map_role_to_category("Ủy viên UBKTNB"), "SUPERVISOR")

    def test_map_role_management(self):
        """Kiểm tra phân loại Ban điều hành."""
        self.assertEqual(map_role_to_category("Tổng Giám đốc"), "CEO")
        self.assertEqual(map_role_to_category("Kế toán trưởng"), "CHIEF_ACCOUNTANT")

if __name__ == '__main__':
    unittest.main()