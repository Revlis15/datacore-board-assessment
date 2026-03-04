from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from vnstock import listing_companies


EXCHANGE_TARGETS: Dict[str, int] = {
    "HOSE": 20,
    "HNX": 15,
    "UPCOM": 15,
}  # Tổng 60 mã


def get_tickers_for_exchange(listing_df: pd.DataFrame, exchange: str, count: int) -> pd.DataFrame:
    """
    Retrieve a deterministic subset of tickers for a given exchange.

    The function:
    - fetches symbols for the given exchange from vnstock
    - filters rows to the requested exchange
    - sorts tickers alphabetically
    - selects the first `count` tickers
    - returns a DataFrame with columns: ticker, exchange

    :param listing_df: DataFrame of listings from vnstock.listing_companies().
    :param exchange: Exchange code, e.g. "HOSE", "HNX", "UPCOM".
    :param count: Number of tickers to select.
    :return: DataFrame with columns ["ticker", "exchange"].
    :raises RuntimeError: If fewer than `count` tickers are available.
    """
    df = listing_df.copy()

    # vnstock.listing_companies(live=True, source='Wifeed') returns
    # a 'ticker' column and an exchange code in 'comGroupCode'.
    if "ticker" not in df.columns or "comGroupCode" not in df.columns:
        raise RuntimeError(
            f"vnstock listing data does not contain expected 'ticker'/'comGroupCode' columns for exchange {exchange!r}"
        )

    df = df[df["comGroupCode"].astype(str).str.upper() == exchange.upper()]

    # Deduplicate and sort deterministically.
    df_unique = (
        df[["ticker"]]
        .drop_duplicates()
        .assign(exchange=exchange)
        .sort_values("ticker")
        .reset_index(drop=True)
    )

    if len(df_unique) < count:
        raise RuntimeError(
            f"Not enough tickers for exchange {exchange!r}: "
            f"requested {count}, available {len(df_unique)}"
        )

    selected = df_unique.head(count).copy()
    return selected[["ticker", "exchange"]]


def generate_tickers() -> pd.DataFrame:
    """
    Generate a deterministic, balanced list of Vietnamese stock tickers.

    The function:
    - initializes vnstock Listing
    - retrieves tickers for HOSE, HNX, and UPCOM
    - selects a balanced subset per EXCHANGE_TARGETS
    - concatenates into a single DataFrame with columns: ticker, exchange

    :return: DataFrame of 50 tickers with schema [ticker, exchange].
    :raises RuntimeError: If vnstock initialization or retrieval fails.
    """
    try:
        listing_df = listing_companies(live=True, source="Wifeed")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Failed to retrieve listings from vnstock.listing_companies") from exc

    frames: List[pd.DataFrame] = []
    for exchange, count in EXCHANGE_TARGETS.items():
        frames.append(get_tickers_for_exchange(listing_df, exchange, count))

    tickers_df = pd.concat(frames, ignore_index=True)
    tickers_df = tickers_df[["ticker", "exchange"]]
    return tickers_df


def save_tickers_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save the tickers DataFrame to CSV at the given path.

    The CSV schema is:
    ticker,exchange

    :param df: DataFrame containing ticker and exchange columns.
    :param output_path: Destination file path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    """
    Entry point for generating Vietnamese stock tickers.

    - Generates a deterministic list of 50 tickers across HOSE, HNX, UPCOM.
    - Saves the result to data/tickers.csv relative to the project root.
    - Prints how many tickers were generated.
    - Exits with a non-zero code and message if vnstock fails.
    """
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    output_path = project_root / "data" / "tickers.csv"

    try:
        tickers_df = generate_tickers()
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    save_tickers_to_csv(tickers_df, output_path)
    print(f"Generated {len(tickers_df)} tickers to {output_path}")


if __name__ == "__main__":
    main()