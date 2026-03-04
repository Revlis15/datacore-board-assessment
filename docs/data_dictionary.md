# 📖 Data Dictionary - `board_golden.parquet`

| Field                   | Type      | Description                                                    | Priority Source / Notes                                     |
| :---------------------- | :-------- | :------------------------------------------------------------- | :---------------------------------------------------------- |
| `ticker`                | `string`  | Stock ticker symbol (e.g., AAA, FPT).                          | Primary Key for linkage.                                    |
| `exchange`              | `string`  | Listing exchange: HOSE, HNX, or UPCOM.                         |                                                             |
| `person_name`           | `string`  | Full name of the board member.                                 | Prioritizes CafeF formatting.                               |
| `person_name_canonical` | `string`  | Normalized name: lowercase, no diacritics, honorifics removed. | Used for Entity Resolution.                                 |
| `role`                  | `string`  | Unified job title after merging concurrent roles.              | Separated by " / " for dual roles.                          |
| `age`                   | `float`   | Estimated age as of 2026.                                      | Merged from Vietstock/CafeF.                                |
| `education`             | `string`  | Educational background or professional certifications.         | Captured from supplementary info.                           |
| `shares`                | `integer` | Number of shares held individually.                            | Sourced from Vietstock.                                     |
| `tenure`                | `string`  | Term of office or duration of service.                         | Sourced from Vietstock.                                     |
| `source_agreement`      | `string`  | Consistency status between sources.                            | values: `both`, `conflict`, `cafef_only`, `vietstock_only`. |
| `confidence_score`      | `float`   | Reliability score (0.6 - 1.0).                                 | Calculated based on merge logic.                            |
