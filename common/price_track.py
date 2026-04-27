# =============================================================================
# common/price_track.py
# Loads each channel's processed retailer parquet files, selects a consistent
# set of columns, unions them, and writes the combined Price Track output.
# =============================================================================

from datetime import date

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from common.config import PRICE_TRACK_OUTPUT
from common.config import PRICE_TRACK_OUTPUT_TABLE
from common.logger import get_logger

log = get_logger(__name__)


def get_last_processed_dates_by_account(spark: SparkSession, channel: str) -> dict:
    """
    Return {lower(account): max_date} from the combined price track for the
    given channel. Tries the Hive table first, then the parquet output.
    Accounts with no prior history (new retailer) will be absent from the dict.
    """
    table = PRICE_TRACK_OUTPUT_TABLE[channel.upper()]
    path = PRICE_TRACK_OUTPUT[channel.upper()]

    try:
        df = spark.sql(
            f"SELECT lower(account) AS account, MAX(`date`) AS max_date "
            f"FROM {table} GROUP BY lower(account)"
        )
        rows = df.collect()
        if rows:
            out = {r["account"]: r["max_date"] for r in rows if r["max_date"] is not None}
            log.info("  Last processed dates (hive %s): %s", table, out)
            return out
    except Exception as e:
        log.info("  Hive per-account lookup failed for %s (%s) — falling back to parquet", table, e)

    try:
        existing = spark.read.parquet(path)
        df = (existing
              .groupBy(F.lower(F.col("account")).alias("account"))
              .agg(F.max("date").alias("max_date")))
        rows = df.collect()
        out = {r["account"]: r["max_date"] for r in rows if r["max_date"] is not None}
        log.info("  Last processed dates (parquet %s): %s", path, out)
        return out
    except Exception as e:
        log.info("  Parquet per-account lookup failed for %s (%s) — no existing history", path, e)

    return {}


def resolve_per_account_windows(
    spark: SparkSession,
    channel: str,
    account_names: list,
    from_date,
    to_date,
) -> dict:
    """
    Resolve per-account (start_date, end_date) windows.

    For each account:
      - If `from_date` is given, start = from_date (reprocess mode).
      - Else if account has prior history, start = its last processed date
        (inclusive — save_price_track append mode clears the window before
        union, so re-processing the last day safely refreshes partial data).
      - Else start = None (no history → process all files for this account).

    Returns {lower(account_name): (start, end)}.
    """
    end = to_date or date.today()
    last_by_account = {} if from_date is not None else get_last_processed_dates_by_account(spark, channel)

    windows = {}
    for name in account_names:
        key = name.lower()
        if from_date is not None:
            start = from_date
        else:
            start = last_by_account.get(key)  # may be None
        if start is not None and start > end:
            log.warning("  %s: start %s is after end %s — window is empty", name, start, end)
        windows[key] = (start, end)

    log.info("  Per-account windows for %s: %s", channel, windows)
    return windows

# Shared output column schema for all channels
PRICE_TRACK_COLUMNS = [
    "skuName",
    "productName",
    "url",
    "basePrice",
    "finalPrice",
    "bfp",
    "location",
    "status",
    "account",
    "date",
    "week",
    "year",
    "packHintText",
    "derivedMultiplier",
    "belowBfp",
    "source_file",
]


def select_price_track_cols(
    df: DataFrame,
    sku_col: str,
    bfp_col: str,
    has_location: bool = True,
) -> DataFrame:
    """
    Standardize a retailer DataFrame to the shared price track schema.

    Args:
        df:           Input retailer DataFrame.
        sku_col:      Column to alias as "skuName".
        bfp_col:      Column to alias as "bfp".
        has_location: Whether the DataFrame has a location column (Minis/HSM).
                      DCOMM (eComm) does not have store locations.
    """
    select_exprs = [
        F.col(sku_col).alias("skuName"),
        F.col("productName"),
        F.col("url") if "url" in df.columns else F.lit(None).cast("string").alias("url"),
        F.col("basePrice"),
        F.col("finalPrice"),
        F.col(bfp_col).alias("bfp"),
        F.col("status"),
        F.col("account"),
        F.col("scrape_date").alias("date"),
        F.col("week"),
        F.col("year"),
        F.col("packHintText"),
        F.col("derivedMultiplier"),
        F.col("belowBfp"),
        F.col("source_file") if "source_file" in df.columns else F.lit(None).cast("string").alias("source_file"),
    ]

    if has_location and "location" in df.columns:
        select_exprs.insert(5, F.col("location"))
    else:
        select_exprs.insert(5, F.lit(None).cast("string").alias("location"))

    return df.select(select_exprs)


def build_combined_price_track(retailer_dfs: list[DataFrame], has_location: bool = True) -> DataFrame:
    """
    Union a list of standardized retailer DataFrames, parse the date column,
    deduplicate, and sort.

    Args:
        retailer_dfs: List of DataFrames already passed through select_price_track_cols.
        has_location: Whether to include the location column in the output.
                      Channels without store locations (e.g. DCOMM, HABA) can set
                      this to False to drop the null location column entirely.
    """
    if not retailer_dfs:
        raise ValueError("No retailer DataFrames provided to union.")

    combined = retailer_dfs[0]
    for df in retailer_dfs[1:]:
        combined = combined.unionAll(df)

    combined = combined.withColumn("date", F.to_date(F.col("date"), "M/d/yyyy")).dropDuplicates()
    if has_location:
        combined = combined.orderBy(
            F.col("date").desc(),
            F.col("account").asc(),
            F.col("location").asc(),
            F.col("skuName").asc(),
        )
    else:
        combined = combined.drop("location").orderBy(
            F.col("date").desc(),
            F.col("account").asc(),
            F.col("skuName").asc(),
        )

    return combined


def save_price_track(
    spark: SparkSession,
    df: DataFrame,
    channel: str,
    mode: str = "overwrite",
    windows: dict = None,
) -> None:
    """
    Write the combined price track DataFrame to the configured output path.

    Args:
        df:          Combined price track DataFrame for the processed window.
        channel:     One of "DCOMM", "DCOMM_RAW", "HSM", "HABA", "MINIS".
        mode:
            - "overwrite": replace the entire parquet/table with `df`.
            - "append":    merge `df` into the existing output per-account.
                           For each entry in `windows` ({lower(account): (start, end)}),
                           rows in [start, end] for that account are cleared
                           from the existing output before unioning `df`.
        windows:
            Required when mode="append". Dict mapping lowercased account name
            to (start_date, end_date). Accounts with start=None are skipped
            (no history → nothing to clear; the new rows in `df` are simply added).
    """
    output_path = PRICE_TRACK_OUTPUT[channel.upper()]
    output_path_table = PRICE_TRACK_OUTPUT_TABLE[channel.upper()]

    final_df = df
    if mode == "append":
        if not windows:
            raise ValueError("append mode requires a non-empty `windows` dict")
        log.info("Appending [%s] per-account windows: %s", channel, windows)
        try:
            existing = spark.table(output_path_table)
        except Exception as e:
            log.info("  Hive table unavailable (%s) — trying parquet", e)
            try:
                existing = spark.read.parquet(output_path)
            except Exception as e2:
                log.warning("  No existing output found (%s) — writing fresh", e2)
                existing = None

        if existing is not None:
            # Build a single OR-combined predicate covering every
            # (account, [start, end]) pair we need to clear.
            clear_cond = None
            for acct, (start, end) in windows.items():
                if start is None:
                    continue  # no prior history for this account → nothing to clear
                c = (
                    (F.lower(F.col("account")) == F.lit(acct))
                    & (F.col("date") >= F.lit(start))
                    & (F.col("date") <= F.lit(end))
                )
                clear_cond = c if clear_cond is None else (clear_cond | c)

            kept = existing if clear_cond is None else existing.filter(~clear_cond)
            final_df = kept.unionByName(df, allowMissingColumns=True).dropDuplicates()
        else:
            final_df = df
    elif mode != "overwrite":
        raise ValueError(f"Unknown save mode: {mode}")

    log.info("Saving combined Price Track [%s] → %s", channel, output_path)
    final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

    row_count = final_df.count()
    log.info("  Saved %d total rows", row_count)

    log.info("Saving combined Price Track [%s] → %s", channel, output_path_table)
    final_df.createOrReplaceTempView("temp_price_track")
    spark.sql(f"CREATE OR REPLACE TABLE {output_path_table} AS (SELECT * FROM temp_price_track)")

    try:
        final_df.groupBy("account").count().show()
    except Exception:
        pass
