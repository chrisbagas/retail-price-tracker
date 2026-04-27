# Retail Price Tracking Pipeline

A PySpark/Databricks pipeline that tracks retail prices across multiple sales channels by joining scraped product data against Base Floor Price (BFP) thresholds. Outputs a combined price track parquet and Hive/Unity Catalog table per channel.

## Channels

| Channel | Retailers |
|---------|-----------|
| `DCOMM` | Lazada, Blibli, Tokopedia |
| `HSM`   | Hypermart, TipTop, Superindo |
| `MINIS` | Alfamart, Indomaret, Midi |
| `HABA`  | Watson, Guardian |

## Requirements

- Python 3.9+
- PySpark (provided by Databricks runtime)
- `pandas`, `openpyxl` (for Excel mapping extraction)
- Access to a Databricks Volume containing scrape parquets and mapping Excel files

## Configuration

Edit [`common/config.py`](common/config.py) or set environment variables before running:

| Variable | Description |
|----------|-------------|
| `PRICE_TRACKER_BASE_PATH` | Databricks Volume path to the data root |
| `PRICE_TRACKER_HIVE_DB`   | Hive/Unity Catalog database for output tables |

Example:

```bash
export PRICE_TRACKER_BASE_PATH="/Volumes/my_catalog/my_schema/my_volume/Price Tracker Data"
export PRICE_TRACKER_HIVE_DB="my_database"
```

## Data layout (under `BASE_PATH`)

```
Price Tracker Data/
├── Mapping/
│   ├── Buffer/          ← mapping Excel files
│   ├── BFP/             ← BFP parquets (Q1, Q2, current)
│   └── *_Definition.parquet
├── Price Scraping/
│   ├── Ecomm/           ← DCOMM scrapes
│   ├── Hsm/             ← HSM scrapes
│   ├── Minis/           ← MINIS scrapes
│   └── Haba/            ← HABA scrapes
└── Price Track/
    ├── Price_Track_*.parquet
    └── Historical/
```

## Running the pipeline

**Local / CI:**

```bash
python main.py --channel DCOMM
python main.py --channel HSM --refresh-bfp true --refresh-mapping false
python main.py --channel MINIS --from-date 2025-01-01 --to-date 2025-03-31
python main.py --channel HABA --full-refresh true
```

**Databricks Job (Python script task):** pass the same flags as job parameters.

### CLI arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--channel` | required | `DCOMM`, `HSM`, `MINIS`, or `HABA` |
| `--refresh-bfp` | `false` | Rebuild current-period BFP parquet from Excel |
| `--refresh-mapping` | `true` | Re-extract mapping parquets from Excel |
| `--from-date` | last processed | Start of processing window (`YYYY-MM-DD`) |
| `--to-date` | today | End of processing window (`YYYY-MM-DD`) |
| `--full-refresh` | `false` | Reprocess all scrape files and overwrite output |

### Schema normalisation (one-time)

If raw scrape parquets have inconsistent schemas, run the normalisation script once before the first pipeline run:

```bash
python fix_parquet_schemas.py
```

Uncomment the relevant folders in `SCRAPE_FOLDERS` before running.

## Architecture

```
Excel mapping files
        ↓  common/mapping.py
Mapping parquets  ←→  BFP parquets (common/bfp.py)
        ↓
Raw scrape parquets (per retailer)
        ↓  common/retailer.py
  load_retailer_df        → stamps account / date / week / year from filename
  build_bfp_joined_df     → LEFT JOINs Q1 / Q2 / current BFP; applies multiplier
  check_missing_mappings  → alerts on unmapped Top 58 SKUs
  build_final_view        → filters to Top 58 + Competitor rows, deduplicates
        ↓
Per-retailer parquets (historical)
        ↓  common/price_track.py
Combined Price Track parquet + Hive table
```

### Key modules

| Module | Purpose |
|--------|---------|
| `common/config.py` | Single source of truth for all paths, BFP periods, and date formats |
| `common/retailer.py` | Core PySpark logic: loading scrapes, BFP joins, multiplier logic, alerts |
| `common/bfp.py` | Registers BFP temp views; refreshes/rebuilds BFP parquets from Excel |
| `common/mapping.py` | Extracts product-name → SKU mappings from Excel into parquet |
| `common/price_track.py` | Unions all retailer DataFrames into a single channel-level output |
| `common/spark.py` | Centralized Spark session accessor |
| `channels/dcomm.py` | DCOMM channel entry point |
| `channels/hsm.py` | HSM channel entry point |
| `channels/minis.py` | MINIS channel entry point |
| `channels/haba.py` | HABA channel entry point |
| `fix_parquet_schemas.py` | One-time schema normalisation utility |

### BFP period logic

Three BFP periods are applied in `retailer.py`:

| Period | Filter |
|--------|--------|
| Q1 2025 | `year < 2025 OR (year = 2025 AND week < 21)` |
| Q2 2025 | `(year = 2025 AND week >= 21) OR (year = 2026 AND week < 6)` |
| Current | joined on `week + year` (week ≥ 6 of current year) |

Each row lands in exactly one period based on its scrape date.

### Multiplier logic (DCOMM only)

DCOMM products may be sold in multipack variants. `build_bfp_joined_df` derives a per-unit price by dividing `basePrice` / `finalPrice` by a multiplier sourced from:
1. The mapping Excel (`*_MULTIPLIER` column), falling back to
2. Regex-parsed pack hints in the product name (`"isi 3"`, `"x2"`, `"twinpack"`, etc.), falling back to
3. Manual override rules from the `Manual Multiplier Mapping` sheet.

## License

MIT
