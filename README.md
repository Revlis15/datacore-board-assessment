# Vietnamese Board of Directors Data Pipeline

This project implements a proof-of-concept data pipeline that collects, cleans, and merges board of directors data for Vietnamese listed companies.

The pipeline scrapes leadership data from two public sources:

- CafeF
- Vietstock

The goal is to create a clean, research-ready **golden dataset** of Vietnamese corporate board members.

This project was developed as part of the **DataCore Research Intern Technical Assessment**.

# Project Overview

Many academic studies require board of directors data for Vietnamese listed companies.  
Currently, researchers must manually collect this information from websites such as CafeF and Vietstock.

This project demonstrates how to build an automated data engineering pipeline to:

1. Scrape board data from CafeF
2. Scrape board data from Vietstock
3. Normalize and clean the data
4. Merge both sources into a single "golden" dataset

The final output is a Parquet dataset suitable for downstream research or analytics.

# Repository Structure

```
datacore-board-assessment/
├── README.md
├── requirements.txt
├── config.yaml
│
├── src/
│   ├── scrape_cafef.py
│   ├── scrape_vietstock.py
│   ├── merge.py
│   └── utils.py
│
├── tools/
│   └── generate_tickers.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── final/
│
├── docs/
│   ├── data_dictionary.md
│   └── data_quality_report.md
│
├── tests/
│   ├── test_utils.py
│   └── test_merger.py
│
└── notebooks/
    └── eda_board_data.ipynb
```

# Setup Instructions

The project was tested with:

```
Python 3.10+
```

## Clone repository

```
git clone <repository-url>
cd datacore-board-assessment
```

## Install dependencies

```
pip install -r requirements.txt
```

Dependencies include:

- pandas
- requests
- PyYAML
- pyarrow
- vnstock
- beautifulsoup4

# Configuration

Runtime parameters are externalized in `config.yaml` to avoid hardcoded values and ensure the pipeline can be easily configured.

Example configuration:

```yaml
paths:
  raw_dir: data/raw
  processed_dir: data/processed

tickers:
  file: data/tickers.csv
  max_tickers: 50

scraping:
  cafef:
    request_delay_seconds: 1.5
    retry_delay_seconds: 5
    timeout_seconds: 20
    max_retries: 3

  vietstock:
    request_delay_seconds: 2
    retry_delay_seconds: 5
    timeout_seconds: 30
    max_retries: 5
```

Scraping parameters are defined separately for each source because CafeF and Vietstock require different request strategies and retry policies.

# How to Run

Run the pipeline in the following order.

## 1 Generate ticker universe

```
python tools/generate_tickers.py
```

This script retrieves Vietnamese stock listings using **vnstock** and generates tickers across:

- HOSE
- HNX
- UPCOM

Output:

```
data/tickers.csv
```

## 2 Scrape CafeF board data

```
python src/scrape_cafef.py
```

This script:

- Calls the CafeF AJAX endpoint
- Parses board member information
- Normalizes names and roles

Output:

```
data/raw/cafef_board.parquet
```

## 3 Scrape Vietstock board data

```
python src/scrape_vietstock.py
```

This script:

- Loads the Vietstock board page
- Parses leadership tables
- Cleans placeholder values such as `***` or `-`

Output:

```
data/raw/vietstock_board.parquet
```

## 4 Merge datasets

```
python src/merge.py
```

This step merges CafeF and Vietstock datasets into a single **golden dataset**.

Output:

```
data/final/board_golden.parquet
```

## 5 Run unit tests

```
python -m unittest discover tests
```

Tests validate:

- name normalization
- role classification
- merge conflict logic

# Name Normalization

Vietnamese names often appear with:

- diacritics
- honorific prefixes
- inconsistent spacing

Examples:

```
Ông Nguyễn Văn A
ThS. Nguyễn Văn A
Nguyen Van A
```

The pipeline normalizes names by:

- removing honorifics
- converting to lowercase
- stripping Vietnamese diacritics
- standardizing whitespace

Example:

```
Ông Đinh Bộ Lễ → dinh bo le
```

# Deterministic Matching Key

To improve cross-source matching, a deterministic key is generated:

```
person_name_key = normalized_name_without_spaces
```

Example:

```
nguyen van a → nguyenvana
```

This enables reliable joins between CafeF and Vietstock datasets.

# Merge Strategy

Datasets are merged using:

```
(ticker, exchange, person_name_key)
```

Each merged record receives a `source_agreement` value:

| value          | meaning                       |
| -------------- | ----------------------------- |
| both           | record exists in both sources |
| cafef_only     | only found in CafeF           |
| vietstock_only | only found in Vietstock       |
| conflict       | disagreement between sources  |

# Conflict Resolution Rules

When sources disagree:

1. Person Name  
   Prefer CafeF formatting.

2. Role  
   Prefer CafeF role title.

3. Additional attributes  
   Prefer Vietstock for:

- age
- education
- shares
- tenure

# Confidence Scoring

Each record receives a confidence score:

| condition          | score |
| ------------------ | ----- |
| both sources agree | 1.0   |
| single source      | 0.8   |
| conflict           | 0.6   |

This metric helps quantify data reliability.

# Final Dataset

The merged dataset:

```
data/final/board_golden.parquet
```

Schema documentation:

- [Data Dictionary](docs/data_dictionary.md) – schema and description of the final dataset

Example fields:

- ticker
- exchange
- person_name
- person_name_canonical
- role
- age
- education
- shares
- tenure
- source_agreement
- confidence_score

# Data Quality Report

Detailed analysis is provided in:

- [Data Quality Report](docs/data_quality_report.md) – merge statistics and data quality analysis

The report includes:

- total records per source
- match rate
- conflict rate
- common unmatched names

# Known Limitations

1. **Anti-bot Protection & Rate Limiting**
   Vietstock employs strict CSRF token validation and session tracking. High request frequencies or running the script without proper delays can occasionally trigger soft blocks (returning empty HTML tables).
2. **Role Ambiguity & Edge Cases**
   Highly specific or legally complex Vietnamese titles (e.g., "Đại diện phần vốn góp", "Thành viên độc lập HĐQT") are challenging to map perfectly using Regex and may default to broader categories like `OTHER` or `DIRECTOR`.
3. **Incomplete Source Data**
   For newly listed UPCOM companies or highly restricted firms, both CafeF and Vietstock occasionally lack demographic data (Age, Education, Shares). In these cases, the `board_golden.parquet` file will legitimately contain `null` values.

# Future Improvements (With More Time)

If granted more time to scale this proof-of-concept to the entire Vietnamese stock market (~1,600 tickers), I would implement the following improvements:

1. **Pipeline Orchestration:** Containerize the extraction and transformation scripts using Docker and schedule them via Apache Airflow to ensure the golden dataset is continuously updated without manual intervention.
2. **Advanced Entity Resolution:** Upgrade the deterministic string-matching approach (`person_name_key`) to incorporate fuzzy matching algorithms (e.g., Levenshtein distance) or lightweight ML models to handle typos and severe spelling discrepancies between platforms.
3. **Robust Proxy Rotation:** Integrate a proxy pool and automated User-Agent rotation mechanism for the Vietstock scraper to completely bypass IP bans and scale the scraping concurrency.

# Conclusion

This project demonstrates a complete data engineering pipeline for constructing a Vietnamese board of directors dataset.

The solution includes:

- robust web scraping
- deterministic entity resolution
- conflict-aware dataset merging
- reproducible pipeline execution

The resulting dataset provides a foundation for corporate governance research and financial analysis.
