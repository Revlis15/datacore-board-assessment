import unittest
import sys
from pathlib import Path

# Ensure the 'src' module is discoverable from the project root
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils import normalize_person_name, build_vietnamese_name_key, map_role_to_category

class TestUtils(unittest.TestCase):

    def test_normalize_person_name_with_d(self):
        """
        Validates the manual substitution of the Vietnamese 'Đ/đ'.
        
        Why: Standard Unicode NFD normalization fails to decompose 'Đ' into a base 
        character and a diacritic, often causing it to be dropped. This test ensures 
        the critical first character of names like 'Đinh' remains intact for matching.
        """
        case_1 = "Ông Đinh Bộ Lễ"
        case_2 = "Bà Đặng Thị Ngọc Thịnh"
        
        self.assertEqual(normalize_person_name(case_1), "dinh bo le")
        self.assertEqual(normalize_person_name(case_2), "dang thi ngoc thinh")

    def test_normalize_person_name_honorifics(self):
        """
        Validates the aggressive removal of academic and social honorifics.
        
        Why: If 'GS.TS' (Prof. Dr.) is left in the string, a merge against a source 
        that only lists the raw name will fail, resulting in duplicate entity rows.
        """
        self.assertEqual(normalize_person_name("ThS. Nguyễn Văn A"), "nguyen van a")
        self.assertEqual(normalize_person_name("GS.TS Trần Văn B"), "tran van b")

    def test_name_key_consistency(self):
        """
        Validates the idempotency and consistency of the deterministic join key.
        
        Why: Verifies that variations in capitalization and spacing ultimately resolve 
        to the exact same continuous string, safeguarding the outer join logic.
        """
        name_1 = "nguyen van a"
        name_2 = "Nguyen Van A"
        key_1 = build_vietnamese_name_key(normalize_person_name(name_1))
        key_2 = build_vietnamese_name_key(normalize_person_name(name_2))
        self.assertEqual(key_1, key_2)

    def test_map_role_ubktnb(self):
        """
        Validates regex mapping for specialized Internal Audit/Supervisory committees (UBKTNB).
        
        Why: Ensures that newly introduced corporate governance roles are accurately 
        bucketed into standard analytical categories rather than falling into 'OTHER'.
        """
        # Head of Committee
        self.assertEqual(map_role_to_category("Trưởng ban UBKTNB"), "SUPERVISOR_HEAD")
        self.assertEqual(map_role_to_category("Trưởng ủy ban kiểm tra nội bộ"), "SUPERVISOR_HEAD")
        
        # Committee Members
        self.assertEqual(map_role_to_category("Thành viên UBKTNB"), "SUPERVISOR")
        self.assertEqual(map_role_to_category("Ủy viên UBKTNB"), "SUPERVISOR")

    def test_map_role_management(self):
        """
        Validates mapping logic for standard executive management roles.
        """
        self.assertEqual(map_role_to_category("Tổng Giám đốc"), "CEO")
        self.assertEqual(map_role_to_category("Kế toán trưởng"), "CHIEF_ACCOUNTANT")

if __name__ == '__main__':
    unittest.main()