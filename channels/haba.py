# =============================================================================
# channels/haba.py
# HABA channel — WATSON, GUARDIAN.
# Defines all retailer-specific config and runs the full channel pipeline.
#
# Notes:
#   - No multiplier applied in this channel
#   - No competitor data in this channel
# =============================================================================

from datetime import datetime

from common.config import BASE_PATH, MAPPING_EXCEL
from common.logger import get_logger
from common.mapping import extract_column_pair_mapping
from common.bfp import load_bfp_views
from common.retailer import (
    load_retailer_df,
    build_bfp_joined_df,
    check_missing_mappings,
    build_final_view,
    save_retailer,
)
from common.price_track import (
    select_price_track_cols,
    build_combined_price_track,
    save_price_track,
    resolve_per_account_windows,
)

log = get_logger(__name__)

_date_str = datetime.now().strftime("%y%m%d")

RETAILERS = {
    "WATSON": {
        "mapping_col":    "WATSON",
        "has_competitor":  False,
        "source_path":    f"{BASE_PATH}/Price Scraping/Haba/Watsons",
        "mapping_path":   f"{BASE_PATH}/Mapping/WATSON_Definition.parquet",
        "filename_regex": r"WATSON\d*_(\d{6})",
        "account_name":   "WATSON",
        "view_name":      "watson",
        "mapping_view":   "mapping_watson",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/HABA/WATSON/WATSON_Price_Track_{_date_str}.parquet",
    },
    "GUARDIAN": {
        "mapping_col":    "GUARDIAN",
        "has_competitor":  False,
        # GUARDIAN uses BFP LMT — a different price floor column
        "source_path":    f"{BASE_PATH}/Price Scraping/Haba/Guardian",
        "mapping_path":   f"{BASE_PATH}/Mapping/GUARDIAN_Definition.parquet",
        "filename_regex": r"GUARDIAN\d*_(\d{6})",
        "account_name":   "GUARDIAN",
        "view_name":      "guardian",
        "mapping_view":   "mapping_guardian",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/HABA/GUARDIAN/GUARDIAN_Price_Track_{_date_str}.parquet",
    },
    # "DANDAN": {
    #     "mapping_col":    "DANDAN",
    #     "has_competitor":  False,
    #     "source_path":    f"{BASE_PATH}/Price Scraping/Haba/DANDAN",
    #     "mapping_path":   f"{BASE_PATH}/Mapping/DANDAN_Definition.parquet",
    #     "filename_regex": r"DANDAN\d*_(\d{6})",
    #     "account_name":   "DANDAN",
    #     "view_name":      "dandan",
    #     "mapping_view":   "mapping_dandan",
    #     "product_col":    "productName",
    #     "sku_col":        "SKUName",
    #     "bfp_col":        "BFP Summary Minis",
    #     "drop_cols":      [],
    #     "save_path":      f"{BASE_PATH}/Price Track/Historical/HABA/DANDAN/DANDAN_Price_Track_{_date_str}.parquet",
    # },
}


def run(
    spark,
    from_date=None,
    to_date=None,
    full_refresh: bool = False,
) -> None:
    """Execute the full HABA pipeline: mapping → BFP → per-retailer → combined."""

    if full_refresh:
        log.info("=== HABA: full_refresh=True — processing all scrape files ===")
        windows = {cfg["account_name"].lower(): (None, None) for cfg in RETAILERS.values()}
        save_mode = "overwrite"
    else:
        account_names = [cfg["account_name"] for cfg in RETAILERS.values()]
        windows = resolve_per_account_windows(spark, "HABA", account_names, from_date, to_date)
        save_mode = "append"

    # 1. Rebuild mapping parquet from Excel
    log.info("=== HABA: Extracting mapping definitions ===")

    # Build retailer column dict; only include competitor_col if present
    retailers_cols = {}
    for k, v in RETAILERS.items():
        cols = {"mapping_col": v["mapping_col"]}
        if v.get("competitor_col"):
            cols["competitor_col"] = v["competitor_col"]
        retailers_cols[k] = cols

    mapping_pdfs = extract_column_pair_mapping(
        excel_path=MAPPING_EXCEL["HABA"],
        retailers=retailers_cols,
        mapping_sheet="Mapping",
        mapping_sku_col="TOP 58",
        has_multiplier=False,
    )

    # 2. Load BFP temp views
    log.info("=== HABA: Loading BFP views ===")
    load_bfp_views(spark)

    # 3. Process each retailer
    retailer_dfs = []
    for code, cfg in RETAILERS.items():
        log.info("=== HABA: Processing %s ===", code)

        acct_key = cfg["account_name"].lower()
        acct_start, acct_end = windows[acct_key]
        log.info("  %s window: %s → %s", code, acct_start, acct_end)

        df = load_retailer_df(
            spark=spark,
            source_path=cfg["source_path"],
            account_name=cfg["account_name"],
            filename_regex=cfg["filename_regex"],
            drop_cols=cfg["drop_cols"],
            start_date=acct_start,
            end_date=acct_end,
        )
        if df is None:
            log.warning("  Skipping %s — no files in window", code)
            continue
        df.createOrReplaceTempView(cfg["view_name"])

        # Load mapping — prefer in-memory DataFrames (avoids FUSE sync issues),
        # fall back to reading parquet from disk
        if mapping_pdfs and code in mapping_pdfs:
            mapping_df = spark.createDataFrame(mapping_pdfs[code])
        else:
            mapping_df = spark.read.parquet(cfg["mapping_path"])
        mapping_df.createOrReplaceTempView(cfg["mapping_view"])

        joined = build_bfp_joined_df(
            spark=spark,
            view_name=cfg["view_name"],
            mapping_view=cfg["mapping_view"],
            product_col=cfg["product_col"],
            sku_col=cfg["sku_col"],
            bfp_col=cfg["bfp_col"],
            has_multiplier=False,
        )

        has_comp = cfg.get("has_competitor", True)
        check_missing_mappings(
            spark=spark,
            view_name=cfg["view_name"],
            mapping_view=cfg["mapping_view"],
            product_col=cfg["product_col"],
            sku_col=cfg["sku_col"],
            bfp_col=cfg["bfp_col"],
            has_competitor=has_comp,
        )

        final = build_final_view(joined, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_competitor=has_comp)
        save_retailer(final, cfg["save_path"], label=code)

        retailer_dfs.append(
            select_price_track_cols(final, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_location=False)
        )

    if not retailer_dfs:
        log.warning("=== HABA: No retailer data in window — skipping combined write ===")
        return

    # 4. Union and save combined price track
    log.info("=== HABA: Building combined Price Track ===")
    combined = build_combined_price_track(retailer_dfs, has_location=False)
    save_price_track(spark, combined, channel="HABA", mode=save_mode, windows=windows)
    log.info("=== HABA: Pipeline complete ===")
