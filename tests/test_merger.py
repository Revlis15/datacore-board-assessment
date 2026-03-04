import unittest
import pandas as pd
import sys
from pathlib import Path

# Append project root to sys.path to enable absolute imports from the 'src' package
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.merge import squash_duplicates, resolve_conflicts

class TestMerger(unittest.TestCase):

    def test_squash_duplicates(self):
        """
        Validates the Entity Consolidation logic (squashing concurrent roles into a single Golden Row).
        
        Why: Simulates a real-world scenario where a board member holds multiple executive positions. 
        This ensures the pipeline maintains a strict 1:1 entity-to-row ratio without losing 
        granular role data, strictly fulfilling the 'single golden row' requirement.
        """
        data = [
            {'ticker': 'AAA', 'exchange': 'HOSE', 'person_name_canonical': 'nguyen van a', 
             'person_name_key': 'nguyenvana', 'role': 'Chủ tịch HĐQT', 'age': 50},
            {'ticker': 'AAA', 'exchange': 'HOSE', 'person_name_canonical': 'nguyen van a', 
             'person_name_key': 'nguyenvana', 'role': 'Tổng Giám đốc', 'age': 50}
        ]
        df = pd.DataFrame(data)
        result = squash_duplicates(df)
        
        # Assert that the DataFrame was successfully deduplicated to a single row
        self.assertEqual(len(result), 1)
        # Verify that the roles were concatenated with a slash delimiter
        self.assertIn("Chủ tịch HĐQT / Tổng Giám đốc", result.iloc[0]['role'])

    def test_resolve_conflicts_both_sources(self):
        """
        Validates the Priority-based Conflict Resolution and Data Enrichment strategy.
        
        Why: This tests the core intelligence of the ETL pipeline. It ensures the system correctly 
        resolves field-level disagreements, applies data quality metrics (confidence_score, 
        source_agreement flag), and successfully enriches records using cross-source data.
        """
        # Mock a row simulating the output of an Outer Join across both sources
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
        
        # 1. Enrichment: Age must be backfilled from Vietstock since CafeF is Null
        self.assertEqual(result['age_golden'], 55.0)
        
        # 2. Enrichment: Education prioritizes Vietstock's richer data layer
        self.assertEqual(result['education_golden'], 'Tiến sĩ')
        
        # 3. Conflict Detection: Roles 'Chủ tịch' (Chairman) and 'Thành viên' (Member) differ, 
        # triggering a conflict state [cite: 77, 80] and lowering the confidence score.
        self.assertEqual(result['source_agreement'], 'conflict')
        self.assertEqual(result['confidence_score'], 0.6)

if __name__ == '__main__':
    unittest.main()