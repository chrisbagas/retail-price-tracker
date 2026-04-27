# =============================================================================
# common/config.py
# Central configuration for all paths, BFP period boundaries, and constants.
# Update paths and BFP periods here — nowhere else.
#
# Set environment variables to configure for your environment:
#   PRICE_TRACKER_BASE_PATH  — Databricks Volume base path
#   PRICE_TRACKER_HIVE_DB    — Hive/Unity Catalog database name
# =============================================================================

import os

# --- Base volume path ---
# Override via environment variable for your deployment.
# Example: /Volumes/<catalog>/<schema>/<volume>/Price Tracker Data
BASE_PATH      = os.environ.get(
    "PRICE_TRACKER_BASE_PATH",
    "/Volumes/<your-catalog>/<your-schema>/<your-volume>/Price Tracker Data",
)
BASE_PATH_HIVE = os.environ.get("PRICE_TRACKER_HIVE_DB", "<your_hive_database>")

# --- BFP period boundaries (ISO week numbers) ---
BFP_PERIODS = {
    "Q1_2025": {
        "parquet": f"{BASE_PATH}/Mapping/BFP/Top58_Q1_2025.parquet",
        "filter": "year < 2025 OR (year = 2025 AND week < 21)",
    },
    "Q2_2025": {
        "parquet": f"{BASE_PATH}/Mapping/BFP/Top58_Q2_2025.parquet",
        "filter": "(year = 2025 AND week >= 21) OR (year = 2026 AND week < 6)",
    },
    "CURRENT": {
        "parquet": f"{BASE_PATH}/Mapping/BFP/Top58_2026.parquet",
        "filter": None,  # Current period: joined on week + year
    },
}

# --- Mapping Excel sources ---
MAPPING_EXCEL = {
    "DCOMM": f"{BASE_PATH}/Mapping/Buffer/Mapping HSM-dComm.xlsx",
    "HSM":   f"{BASE_PATH}/Mapping/Buffer/Mapping HSM-dComm.xlsx",
    "HABA":  f"{BASE_PATH}/Mapping/Buffer/Mapping HSM-dComm.xlsx",
    "MINIS": f"{BASE_PATH}/MINIS Mapped out LIVE.xlsx",
    "BFP":   f"{BASE_PATH}/Top 58 SKU.xlsx",
}

# --- Parquet output folder for mapping definitions ---
MAPPING_OUTPUT_DIR = f"{BASE_PATH}/Mapping"

# --- BFP output paths ---
BFP_OUTPUT = {
    "CURRENT": f"{BASE_PATH}/Mapping/BFP/Top58_2026.parquet",
    "Q1_2025": f"{BASE_PATH}/Mapping/BFP/Top58_Q1_2025.parquet",
    "Q2_2025": f"{BASE_PATH}/Mapping/BFP/Top58_Q2_2025.parquet",
}

# --- Price track combined output paths ---
PRICE_TRACK_OUTPUT = {
    "DCOMM": f"{BASE_PATH}/Price Track/Price_Track_DCOMM.parquet",
    "DCOMM_RAW": f"{BASE_PATH}/Price Track/Price_Track_DCOMM_Raw.parquet",
    "HSM":   f"{BASE_PATH}/Price Track/Price_Track_HSM.parquet",
    "HABA":  f"{BASE_PATH}/Price Track/Price_Track_HABA.parquet",
    "MINIS": f"{BASE_PATH}/Price Track/Price_Track_Minis.parquet",
}
PRICE_TRACK_OUTPUT_TABLE = {
    "DCOMM": f"{BASE_PATH_HIVE}.price_track_dcomm",
    "DCOMM_RAW": f"{BASE_PATH_HIVE}.price_track_dcomm_raw",
    "HSM":   f"{BASE_PATH_HIVE}.price_track_hsm",
    "HABA":  f"{BASE_PATH_HIVE}.price_track_haba",
    "MINIS": f"{BASE_PATH_HIVE}.price_track_minis",
}

# --- Scrape date format used in filenames ---
FILENAME_DATE_FORMAT = "yyMMdd"
DISPLAY_DATE_FORMAT  = "M/d/yyyy"
