# =============================================================================
# channels/minis.py
# Minis channel — Alfamart, Indomaret, Midi.
# Defines all retailer-specific config and runs the full channel pipeline.
#
# Notes:
#   - Minis mapping uses a sheet-per-retailer format (not column-pair)
#   - Minis mapping uses "SKU NAME" (with space) not "SKUName"
#   - Indomaret requires schema inference from the newest file due to schema drift
#   - No multiplier applied in this channel
# =============================================================================

from datetime import datetime

from common.config import BASE_PATH, MAPPING_EXCEL
from common.logger import get_logger
from common.mapping import extract_sheet_mapping
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

# Minis mapping sheets → output parquet names
MAPPING_SHEETS = {
    "ALF-Definition":  "ALF_Definition",
    "IDM-Definition":  "IDM_Definition",
    "MIDI-Definition": "MIDI_Definition",
}

RETAILERS = {
    "ALF": {
        "source_path":        f"{BASE_PATH}/Price Scraping/Minis/Alfagift",
        "mapping_path":       f"{BASE_PATH}/Mapping/ALF_Definition.parquet",
        "filename_regex":     r"ALFAGIFT\d*_(\d{6})",
        "account_name":       "ALFAMART",
        "view_name":          "alfamart",
        "mapping_view":       "mapping_alfamart",
        "product_col":        "ProductName",
        "sku_col":            "SKUName",
        "bfp_col":            "BFP Summary Minis",
        "drop_cols":          [],
        "use_newest_schema":  False,
        "save_path":          f"{BASE_PATH}/Price Track/Historical/Alf/ALF_Price_Track_{_date_str}.parquet",
    },
    "IDM": {
        "source_path":        f"{BASE_PATH}/Price Scraping/Minis/Indomaret",
        "mapping_path":       f"{BASE_PATH}/Mapping/IDM_Definition.parquet",
        "filename_regex":     r"INDOMARET\d*_(\d{6})",
        "account_name":       "INDOMARET",
        "view_name":          "indomaret",
        "mapping_view":       "mapping_indomaret",
        "product_col":        "ProductName",
        "sku_col":            "SKUName",
        "bfp_col":            "BFP Summary Minis",
        "drop_cols":          [],
        # Indomaret has schema drift across files — always use newest schema
        "use_newest_schema":  True,
        "save_path":          f"{BASE_PATH}/Price Track/Historical/IDM/IDM_Price_Track_{_date_str}.parquet",
    },
    "MIDI": {
        "source_path":        f"{BASE_PATH}/Price Scraping/Minis/Midi",
        "mapping_path":       f"{BASE_PATH}/Mapping/MIDI_Definition.parquet",
        "filename_regex":     r"MIDI\d*_(\d{6})",
        "account_name":       "MIDI",
        "view_name":          "midi",
        "mapping_view":       "mapping_midi",
        "product_col":        "ProductName",
        "sku_col":            "SKUName",
        "bfp_col":            "BFP Summary Minis",
        "drop_cols":          [],
        "use_newest_schema":  False,
        "save_path":          f"{BASE_PATH}/Price Track/Historical/MIDI/MIDI_Price_Track_{_date_str}.parquet",
    },
}


def run(
    spark,
    from_date=None,
    to_date=None,
    full_refresh: bool = False,
) -> None:
    """Execute the full Minis pipeline: mapping → BFP → per-retailer → combined."""

    if full_refresh:
        log.info("=== MINIS: full_refresh=True — processing all scrape files ===")
        windows = {cfg["account_name"].lower(): (None, None) for cfg in RETAILERS.values()}
        save_mode = "overwrite"
    else:
        account_names = [cfg["account_name"] for cfg in RETAILERS.values()]
        windows = resolve_per_account_windows(spark, "MINIS", account_names, from_date, to_date)
        save_mode = "append"

    # 1. Rebuild mapping parquet from Excel (sheet-per-retailer format)
    log.info("=== MINIS: Extracting mapping definitions ===")
    mapping_pdfs = extract_sheet_mapping(
        excel_path=MAPPING_EXCEL["MINIS"],
        sheet_retailer_map=MAPPING_SHEETS,
    )

    # 2. Load BFP temp views
    log.info("=== MINIS: Loading BFP views ===")
    load_bfp_views(spark)

    # 3. Process each retailer
    retailer_dfs = []
    for code, cfg in RETAILERS.items():
        log.info("=== MINIS: Processing %s ===", code)

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
        # Minis mapping keys use output_name format (e.g. "ALF_Definition")
        mapping_key = f"{code}_Definition"
        if mapping_pdfs and mapping_key in mapping_pdfs:
            mapping_df = spark.createDataFrame(mapping_pdfs[mapping_key])
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

        check_missing_mappings(
            spark=spark,
            view_name=cfg["view_name"],
            mapping_view=cfg["mapping_view"],
            product_col=cfg["product_col"],
            sku_col=cfg["sku_col"],
            bfp_col=cfg["bfp_col"],
        )

        final = build_final_view(joined, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"])
        save_retailer(final, cfg["save_path"], label=code)

        retailer_dfs.append(
            select_price_track_cols(final, sku_col=cfg["sku_col"], bfp_col=cfg["bfp_col"], has_location=True)
        )

    if not retailer_dfs:
        log.warning("=== MINIS: No retailer data in window — skipping combined write ===")
        return

    # 4. Union and save combined price track
    log.info("=== MINIS: Building combined Price Track ===")
    combined = build_combined_price_track(retailer_dfs)
    save_price_track(spark, combined, channel="MINIS", mode=save_mode, windows=windows)
    log.info("=== MINIS: Pipeline complete ===")
