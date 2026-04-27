# =============================================================================
# channels/hsm.py
# HSM (Hypermarket/Supermarket) channel — Hypermart, TipTop, Superindo.
# Defines all retailer-specific config and runs the full channel pipeline.
#
# Notes:
#   - TipTop uses BFP LMT instead of BFP Summary Minis
#   - Superindo uses combinedProductName as the product join column
#   - No multiplier applied in this channel
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
    "HYP": {
        "mapping_col":    "HYP",
        "competitor_col": "HYPERMART",
        "has_competitor":  True,
        "source_path":    f"{BASE_PATH}/Price Scraping/Hsm/Hypermart",
        "mapping_path":   f"{BASE_PATH}/Mapping/HYP_Definition.parquet",
        "filename_regex": r"HYPERMART\d*_(\d{6})",
        "account_name":   "HYPERMART",
        "view_name":      "hypermart",
        "mapping_view":   "mapping_hypermart",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/HSM/HYP/HYP_Price_Track_{_date_str}.parquet",
    },
    "TIP_TOP": {
        "mapping_col":    "TIP TOP",
        "competitor_col": "TIPTOP",
        "has_competitor":  True,
        # TipTop uses BFP LMT — a different price floor column
        "source_path":    f"{BASE_PATH}/Price Scraping/Hsm/Tiptop",
        "mapping_path":   f"{BASE_PATH}/Mapping/TIP_TOP_Definition.parquet",
        "filename_regex": r"TIPTOP\d*_(\d{6})",
        "account_name":   "TIPTOP",
        "view_name":      "tiptop",
        "mapping_view":   "mapping_tiptop",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP LMT",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/HSM/TIPTOP/TIPTOP_Price_Track_{_date_str}.parquet",
    },
    "LSI": {
        "mapping_col":    "LSI",
        "competitor_col": "SUPERINDO",
        "has_competitor":  True,
        # Superindo uses combinedProductName (not productName) as the join key
        "source_path":    f"{BASE_PATH}/Price Scraping/Hsm/Superindo",
        "mapping_path":   f"{BASE_PATH}/Mapping/LSI_Definition.parquet",
        "filename_regex": r"SUPERINDO\d*_(\d{6})",
        "account_name":   "SUPERINDO",
        "view_name":      "superindo",
        "mapping_view":   "mapping_superindo",
        "product_col":    "combinedProductName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/HSM/LSI/LSI_Price_Track_{_date_str}.parquet",
    },
}


def run(
    spark,
    from_date=None,
    to_date=None,
    full_refresh: bool = False,
) -> None:
    """Execute the full HSM pipeline: mapping → BFP → per-retailer → combined."""

    if full_refresh:
        log.info("=== HSM: full_refresh=True — processing all scrape files ===")
        windows = {cfg["account_name"].lower(): (None, None) for cfg in RETAILERS.values()}
        save_mode = "overwrite"
    else:
        account_names = [cfg["account_name"] for cfg in RETAILERS.values()]
        windows = resolve_per_account_windows(spark, "HSM", account_names, from_date, to_date)
        save_mode = "append"

    # 1. Rebuild mapping parquet from Excel
    log.info("=== HSM: Extracting mapping definitions ===")

    # Determine whether any retailer in this channel has competitors
    any_has_competitor = any(v.get("has_competitor", True) for v in RETAILERS.values())

    # Build retailer column dict; only include competitor_col if present
    retailers_cols = {}
    for k, v in RETAILERS.items():
        cols = {"mapping_col": v["mapping_col"]}
        if v.get("competitor_col"):
            cols["competitor_col"] = v["competitor_col"]
        retailers_cols[k] = cols

    mapping_pdfs = extract_column_pair_mapping(
        excel_path=MAPPING_EXCEL["HSM"],
        retailers=retailers_cols,
        mapping_sheet="Mapping",
        mapping_sku_col="TOP 58",
        competitor_sheet="competitor price" if any_has_competitor else None,
        competitor_sku_col="COMPETITOR" if any_has_competitor else None,
        has_multiplier=False,
    )

    # 2. Load BFP temp views
    log.info("=== HSM: Loading BFP views ===")
    load_bfp_views(spark)

    # 3. Process each retailer
    retailer_dfs = []
    for code, cfg in RETAILERS.items():
        log.info("=== HSM: Processing %s ===", code)

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

        # Superindo: handle both old (concat/CONCAT) and new (combinedProductName)
        # column names — older scrape files used "concat" which was later renamed
        if code == "LSI" and "combinedProductName" not in df.columns:
            from pyspark.sql.functions import col as _col
            if "concat" in df.columns:
                df = df.withColumnRenamed("concat", "combinedProductName")
                log.info("  Renamed 'concat' → 'combinedProductName' for Superindo")
            elif "CONCAT" in df.columns:
                df = df.withColumnRenamed("CONCAT", "combinedProductName")
                log.info("  Renamed 'CONCAT' → 'combinedProductName' for Superindo")
            else:
                log.warning("  Superindo: neither 'combinedProductName' nor 'concat' found in columns: %s", df.columns)

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
            select_price_track_cols(final, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_location=True)
        )

    if not retailer_dfs:
        log.warning("=== HSM: No retailer data in window — skipping combined write ===")
        return

    # 4. Union and save combined price track
    log.info("=== HSM: Building combined Price Track ===")
    combined = build_combined_price_track(retailer_dfs)
    save_price_track(spark, combined, channel="HSM", mode=save_mode, windows=windows)
    log.info("=== HSM: Pipeline complete ===")
