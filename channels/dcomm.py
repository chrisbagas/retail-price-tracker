# =============================================================================
# channels/dcomm.py
# DCOMM (eCommerce) channel — Lazada, Blibli, Tokopedia, Shopee.
# Defines all retailer-specific config and runs the full channel pipeline.
# =============================================================================

from datetime import datetime

from common.config import BASE_PATH, MAPPING_EXCEL
from common.logger import get_logger
from common.mapping import extract_column_pair_mapping, extract_manual_multiplier_mapping
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

# ---------------------------------------------------------------------------
# Retailer configuration
# Each entry defines the per-retailer column names in the mapping Excel,
# the source scrape path, filename regex, and output save path.
# ---------------------------------------------------------------------------

_date_str = datetime.now().strftime("%y%m%d")

RETAILERS = {
    "LAZADA": {
        # Mapping Excel columns
        "mapping_col":    "LAZADA",
        "competitor_col": "LAZADA",
        "multiplier_col": "LAZADA MULTIPLIER",
        "has_competitor":  True,
        # Spark processing
        "source_path":    f"{BASE_PATH}/Price Scraping/Ecomm/Lazada",
        "mapping_path":   f"{BASE_PATH}/Mapping/LAZADA_Definition.parquet",
        "filename_regex": r"LAZADA[^/]*_(\d{6})\.parquet",          # extracted from last segment of path
        "account_name":   "lazada",
        "view_name":      "lazada",
        "mapping_view":   "mapping_lazada",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      [],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/DCOMM/LAZADA/LAZADA_Price_Track_{_date_str}.parquet",
    },
    "BLIBLI": {
        "mapping_col":    "BLIBLI",
        "competitor_col": "BLIBLI",
        "multiplier_col": "BLIBLI MULTIPLIER",
        "has_competitor":  True,
        "source_path":    f"{BASE_PATH}/Price Scraping/Ecomm/Blibli",
        "mapping_path":   f"{BASE_PATH}/Mapping/BLIBLI_Definition.parquet",
        "filename_regex": r"BLIBLI[^/]*_(\d{6})\.parquet",
        "account_name":   "blibli",
        "view_name":      "blibli",
        "mapping_view":   "mapping_blibli",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      ["status"],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/DCOMM/BLIBLI/BLIBLI_Price_Track_{_date_str}.parquet",
    },
    "TOKPED": {
        "mapping_col":    "TOKPED",
        "competitor_col": "TOKOPEDIA",
        "multiplier_col": "TOKPED MULTIPLIER",
        "has_competitor":  True,
        "source_path":    f"{BASE_PATH}/Price Scraping/Ecomm/Tokopedia",
        "mapping_path":   f"{BASE_PATH}/Mapping/TOKPED_Definition.parquet",
        "filename_regex": r"TOKOPEDIA[^/]*_(\d{6})\.parquet",
        "account_name":   "tokped",
        "view_name":      "tokped",
        "mapping_view":   "mapping_tokped",
        "product_col":    "productName",
        "sku_col":        "SKUName",
        "bfp_col":        "BFP Summary Minis",
        "drop_cols":      ["status"],
        "save_path":      f"{BASE_PATH}/Price Track/Historical/DCOMM/TOKOPEDIA/TOKOPEDIA_Price_Track_{_date_str}.parquet",
    },
    # "SHOPEE": {
    #     "mapping_col":    "SHOPEE",
    #     "competitor_col": "SHOPEE",
    #     "multiplier_col": "SHOPEE MULTIPLIER",
    #     "has_competitor":  True,
    #     "source_path":    f"{BASE_PATH}/Price Scraping/Ecomm/Shopee",
    #     "mapping_path":   f"{BASE_PATH}/Mapping/SHOPEE_Definition.parquet",
    #     "filename_regex": r"SHOPEE[^/]*_(\d{6})\.parquet",
    #     "account_name":   "shopee",
    #     "view_name":      "shopee",
    #     "mapping_view":   "mapping_shopee",
    #     "product_col":    "productName",
    #     "sku_col":        "SKUName",
    #     "bfp_col":        "BFP Summary Minis",
    #     "drop_cols":      ["isSoldOut"],
    #     "save_path":      f"{BASE_PATH}/Price Track/Historical/DCOMM/SHOPEE/SHOPEE_Price_Track_{_date_str}.parquet",
    # },
}


def run(
    spark,
    refresh_mapping: bool = True,
    from_date=None,
    to_date=None,
    full_refresh: bool = False,
) -> None:
    """Execute the full DCOMM pipeline: mapping → BFP → per-retailer → combined.

    Incremental behavior:
      - `full_refresh=True`: ignore history, reprocess every scrape file and
        replace the output (original behavior).
      - `from_date` / `to_date` set: reprocess only that window and merge into
        the existing output.
      - Neither set: resolve window to (last_processed_date + 1, today).
    """

    if full_refresh:
        log.info("=== DCOMM: full_refresh=True — processing all scrape files ===")
        windows = {cfg["account_name"].lower(): (None, None) for cfg in RETAILERS.values()}
        save_mode = "overwrite"
    else:
        account_names = [cfg["account_name"] for cfg in RETAILERS.values()]
        windows = resolve_per_account_windows(spark, "DCOMM", account_names, from_date, to_date)
        save_mode = "append"

    # 1. Rebuild mapping parquet from Excel (skip if files already exist)
    mapping_pdfs = None
    if refresh_mapping:
        log.info("=== DCOMM: Extracting mapping definitions ===")

        # Determine whether any retailer in this channel has competitors
        any_has_competitor = any(v.get("has_competitor", True) for v in RETAILERS.values())

        # Build retailer column dict; only include competitor_col if present
        retailers_cols = {}
        for k, v in RETAILERS.items():
            cols = {"mapping_col": v["mapping_col"], "multiplier_col": v["multiplier_col"]}
            if v.get("competitor_col"):
                cols["competitor_col"] = v["competitor_col"]
            retailers_cols[k] = cols

        mapping_pdfs = extract_column_pair_mapping(
            excel_path=MAPPING_EXCEL["DCOMM"],
            retailers=retailers_cols,
            mapping_sheet="Mapping",
            mapping_sku_col="TOP 58",
            competitor_sheet="Mapping SPI CPI Dcomm" if any_has_competitor else None,
            competitor_sku_col="COMP" if any_has_competitor else None,
            has_multiplier=True,
        )
    else:
        log.info("=== DCOMM: Skipping mapping extraction (using existing parquet files) ===")

    # 2. Load manual multiplier overrides (edge-case bundles not caught by derived logic)
    log.info("=== DCOMM: Loading manual multiplier mapping ===")
    manual_mult_pdf = extract_manual_multiplier_mapping(MAPPING_EXCEL["DCOMM"])
    manual_mult_df = spark.createDataFrame(manual_mult_pdf) if not manual_mult_pdf.empty else None

    # 3. Load BFP temp views (shared across all retailers)
    log.info("=== DCOMM: Loading BFP views ===")
    load_bfp_views(spark)

    # 3. Process each retailer
    retailer_dfs = []
    raw_retailer_dfs = []
    for code, cfg in RETAILERS.items():
        log.info("=== DCOMM: Processing %s ===", code)

        acct_key = cfg["account_name"].lower()
        acct_start, acct_end = windows[acct_key]
        log.info("  %s window: %s → %s", code, acct_start, acct_end)

        # Load raw scrape data (optionally limited to the requested window)
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
        mapping_df.cache()
        mapping_df.createOrReplaceTempView(cfg["mapping_view"])
        log.info("  Registered mapping view: %s (%d rows)", cfg["mapping_view"], mapping_df.count())

        # Join to BFP periods — also returns pre-division raw df
        joined, joined_raw = build_bfp_joined_df(
            spark=spark,
            view_name=cfg["view_name"],
            mapping_view=cfg["mapping_view"],
            product_col=cfg["product_col"],
            sku_col=cfg["sku_col"],
            bfp_col=cfg["bfp_col"],
            has_multiplier=True,
            contains_mapping_match=True,
            manual_multiplier_df=manual_mult_df,
            return_raw=True,
        )

        # Alert on missing mappings
        has_comp = cfg.get("has_competitor", True)
        check_missing_mappings(
            spark=spark,
            view_name=cfg["view_name"],
            mapping_view=cfg["mapping_view"],
            product_col=cfg["product_col"],
            sku_col=cfg["sku_col"],
            bfp_col=cfg["bfp_col"],
            has_competitor=has_comp,
            contains_mapping_match=True,
        )

        # Filter and save processed retailer file
        final = build_final_view(joined, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_competitor=has_comp)
        save_retailer(final, cfg["save_path"], label=code)

        # Prepare for combined processed price track
        retailer_dfs.append(
            select_price_track_cols(final, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_location=False)
        )

        # Prepare for combined raw price track (no multiplier division, no free/gratis filter)
        final_raw = build_final_view(joined_raw, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_competitor=has_comp)
        raw_retailer_dfs.append(
            select_price_track_cols(final_raw, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_location=False)
        )

    if not retailer_dfs:
        log.warning("=== DCOMM: No retailer data in window — skipping combined write ===")
        return

    # 4. Union and save combined processed price track
    log.info("=== DCOMM: Building combined Price Track ===")
    combined = build_combined_price_track(retailer_dfs)
    save_price_track(spark, combined, channel="DCOMM", mode=save_mode, windows=windows)

    # 5. Union and save combined raw price track (pre-division, includes free/gratis)
    log.info("=== DCOMM: Building combined Raw Price Track ===")
    combined_raw = build_combined_price_track(raw_retailer_dfs)
    save_price_track(spark, combined_raw, channel="DCOMM_RAW", mode=save_mode, windows=windows)

    log.info("=== DCOMM: Pipeline complete ===")
