# =============================================================================
# common/bfp.py
# Loads all BFP (Base Floor Price) parquet files and registers them as
# Spark temp views so SQL queries can reference them by name.
# =============================================================================

import os
import pandas as pd
from datetime import datetime

from pyspark.sql import SparkSession

from common.config import BFP_OUTPUT, MAPPING_EXCEL
from common.logger import get_logger

log = get_logger(__name__)


def load_bfp_views(spark: SparkSession) -> None:
    """
    Load Q1 2025, Q2 2025, and current BFP parquet files and register
    them as temp views: bfp_q1_2025, bfp_q2_2025, bfp_top58.
    """
    view_map = {
        "bfp_q1_2025": BFP_OUTPUT["Q1_2025"],
        "bfp_q2_2025": BFP_OUTPUT["Q2_2025"],
        "bfp_top58":   BFP_OUTPUT["CURRENT"],
    }

    for view_name, path in view_map.items():
        log.info("Loading BFP view '%s' from %s", view_name, path)
        # Read via pandas then convert to Spark to avoid ADLS HTTP 412 errors
        # when the parquet was just written/overwritten by refresh_current_bfp.
        # Spark's conditional ETag check fails against a freshly-updated file.
        pdf = pd.read_parquet(path)
        df = spark.createDataFrame(pdf).cache()
        df.createOrReplaceTempView(view_name)
        log.info("  Registered temp view: %s (%d rows)", view_name, df.count())


def refresh_current_bfp(excel_sheet: str = "Q2 2025") -> None:
    """
    Rebuild the current-period BFP parquet from the Top 58 SKU Excel file.
    Appends any weeks not already present, starting from week 6 of current year.

    Args:
        excel_sheet: Sheet name in the Top 58 SKU Excel to read from.
    """
    output_path = BFP_OUTPUT["CURRENT"]
    excel_path  = MAPPING_EXCEL["BFP"]

    log.info("Refreshing current BFP from sheet '%s'", excel_sheet)

    current_date = datetime.now()
    current_week = current_date.isocalendar()[1]
    current_year = current_date.isocalendar()[0]

    df_excel = pd.read_excel(excel_path, sheet_name=excel_sheet)
    df_excel = _clean_dataframe(df_excel)

    all_weeks = []
    for week in range(6, current_week + 1):
        df_week = df_excel.copy()
        df_week["week"] = week
        df_week["year"] = current_year
        all_weeks.append(df_week)

    df_new = pd.concat(all_weeks, ignore_index=True)

    if os.path.exists(output_path):
        df_existing = pd.read_parquet(output_path)
        existing_keys = set(zip(df_existing["year"], df_existing["week"]))
        df_new = df_new[~df_new.apply(lambda r: (r["year"], r["week"]) in existing_keys, axis=1)]
        df_new = pd.concat([df_existing, df_new], ignore_index=True)
        log.info("  Appended new weeks; total rows now: %d", len(df_new))
    else:
        log.info("  No existing file — writing fresh: %d rows", len(df_new))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_new.to_parquet(output_path, index=False)
    log.info("  Saved to %s", output_path)


def rebuild_historical_bfp(quarter: str, excel_sheet: str, year: int) -> None:
    """
    Write a static historical BFP parquet for a given quarter.

    Args:
        quarter:     "Q1" or "Q2"
        excel_sheet: Sheet name in the Top 58 SKU Excel to read from.
        year:        Calendar year this quarter belongs to.
    """
    key         = f"{quarter}_{year}"
    output_path = BFP_OUTPUT[key]
    excel_path  = MAPPING_EXCEL["BFP"]

    log.info("Rebuilding historical BFP for %s from sheet '%s'", key, excel_sheet)

    df = pd.read_excel(excel_path, sheet_name=excel_sheet)
    df = _clean_dataframe(df)
    df["Quartal"] = quarter
    df["year"]    = year

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    log.info("  Saved %d rows to %s", len(df), output_path)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Cast object columns to str and drop unnamed columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    return df
