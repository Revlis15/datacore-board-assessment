# 📊 Data Quality & Merge Report

## 1. Executive Summary

- **Total Tickers Processed:** 50 tickers (HOSE, HNX, UPCOM).
- **Total Unique Golden Records:** **662 records**.
- **Source Agreement Distribution:**
  - **Conflict:** 335 rows (50.6%) - Primarily due to variations in role title detail.
  - **CafeF Only:** 178 rows (26.9%) - Personnel updated only on CafeF.
  - **Both:** 108 rows (16.3%) - Perfect match in both identity and role.
  - **Vietstock Only:** 41 rows (6.2%) - Personnel found only on Vietstock.

## 2. Conflict Resolution Strategy

The pipeline implements a **Priority-based Resolution** approach:

- **Role Conflict:** Differences often arise from varying levels of detail (e.g., "Vice CEO" vs. "Deputy General Director"). The system defaults to the **CafeF** string for consistency but assigns a `confidence_score = 0.6`.
- **Data Enrichment:** Fields such as `age`, `shares`, and `tenure` are prioritized from **Vietstock** to enrich the primary CafeF records where data is missing.

## 3. Observed Data Patterns & Handling

- **Placeholder Scrubbing:** Vietstock frequently uses `***` or `-` for undisclosed data. The pipeline converts these to `null` to maintain dataset integrity.
- **Name Normalization:** Fixed critical encoding issues with the character 'Đ' and removed complex honorifics (e.g., GS.TS, ThS.BS). Successfully validated via a **7/7 OK** unit test suite.
- **Redundancy Management:** The `squash_duplicates` logic ensures individuals holding multiple roles within the same firm are merged into a single "Golden Row."
