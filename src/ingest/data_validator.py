"""
data_validator.py
Author    : R01 — Dennis (Data Ingestion Engineer)
Purpose   : Reusable validation functions for NOAA GHCND and OpenMeteo
            ingested DataFrames.  Used by both ingest scripts before
            saving to parquet.
Milestone : M1 — Data Ingestion & Validation
"""

from __future__ import annotations

import pandas as pd
from loguru import logger


class DataValidationError(Exception):
    """Raised when a critical validation rule fails."""


def check_completeness(df: pd.DataFrame, date_col: str = "date",
                       gap_days: int = 30) -> list[dict]:
    """
    Detect gaps larger than `gap_days` in the date column.

    Parameters
    ----------
    df       : DataFrame with a date-like column.
    date_col : Name of the date column.
    gap_days : Flag gaps strictly larger than this many days.

    Returns
    -------
    List of dicts describing each gap found.
    """
    if date_col not in df.columns:
        logger.warning("Date column '%s' not found — skipping completeness check.", date_col)
        return []

    dates = pd.to_datetime(df[date_col]).sort_values().drop_duplicates()
    if dates.empty:
        return []

    gaps = []
    diffs = dates.diff().dropna()
    large_gaps = diffs[diffs.dt.days > gap_days]
    for ts, gap in large_gaps.items():
        idx = dates.index.get_loc(ts)
        prev_date = dates.iloc[idx - 1] if idx > 0 else None
        gaps.append({
            "gap_start": str(prev_date)[:10] if prev_date is not None else None,
            "gap_end":   str(dates.loc[ts])[:10],
            "gap_days":  int(gap.days),
        })
        logger.warning("Date gap of %d days between %s and %s",
                       gap.days, gaps[-1]["gap_start"], gaps[-1]["gap_end"])
    return gaps


def check_value_ranges(df: pd.DataFrame) -> list[str]:
    """
    Validate numeric columns against physical plausibility bounds.

    Checks:
      precipitation columns  : 0 – 500 mm
      temperature columns    : -10 – 45 °C

    Returns
    -------
    List of warning strings for out-of-range rows.
    """
    warnings: list[str] = []

    precip_cols = [c for c in df.columns if "precip" in c.lower() or c.upper() == "PRCP"]
    temp_cols   = [c for c in df.columns
                   if any(t in c.lower() for t in ("tmax", "tmin", "temperature", "temp"))]

    for col in precip_cols:
        s = pd.to_numeric(df[col], errors="coerce")
        bad = s[(s < 0) | (s > 500)].count()
        if bad:
            msg = f"{bad} rows in '{col}' outside [0, 500] mm"
            warnings.append(msg)
            logger.warning(msg)

    for col in temp_cols:
        s = pd.to_numeric(df[col], errors="coerce")
        bad = s[(s < -10) | (s > 45)].count()
        if bad:
            msg = f"{bad} rows in '{col}' outside [-10, 45] °C"
            warnings.append(msg)
            logger.warning(msg)

    return warnings


def generate_report(df: pd.DataFrame, date_col: str = "date") -> dict:
    """
    Return a summary dict: shape, dtypes, null counts, date range, and
    completeness / range warnings.

    Raises DataValidationError if > 20 % of rows are null in any key column.
    """
    report: dict = {
        "rows": len(df),
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "date_range": None,
        "gaps": [],
        "range_warnings": [],
    }

    if date_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        report["date_range"] = {
            "min": str(dates.min())[:10],
            "max": str(dates.max())[:10],
        }
        report["gaps"] = check_completeness(df, date_col)

    report["range_warnings"] = check_value_ranges(df)

    # Critical failure: any column > 20 % nulls
    for col, n_null in report["null_counts"].items():
        if n_null / max(len(df), 1) > 0.20:
            raise DataValidationError(
                f"Column '{col}' has {n_null}/{len(df)} null values "
                f"({n_null/len(df)*100:.1f}%) — exceeds 20% threshold."
            )

    logger.info("Validation report: %d rows, date range %s",
                report["rows"], report.get("date_range"))
    return report
