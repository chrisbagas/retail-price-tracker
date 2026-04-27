# =============================================================================
# main.py
# Entry point for the Price Tracking pipeline.
#
# Run via Databricks Job with a widget parameter:
#   dbutils.widgets.text("channel", "DCOMM")
#
# Or from the command line (local/CI):
#   python main.py --channel DCOMM
#
# Valid channels: DCOMM, HSM, MINIS, HABA
# To refresh BFP before running: set refresh_bfp widget/arg to "true"
# =============================================================================

import sys
import argparse
from datetime import datetime, date

from common.logger import get_logger
from common.spark import get_spark

log = get_logger("main")

VALID_CHANNELS = {"DCOMM", "HSM", "MINIS", "HABA"}


def _parse_date(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_args():
    """
    Read channel and options.
    For Python script tasks, Databricks passes job parameters as CLI args (sys.argv),
    so argparse works for both Databricks jobs and local runs.
    """
    parser = argparse.ArgumentParser(description="Price Tracking Pipeline")
    parser.add_argument(
        "--channel",
        required=True,
        choices=list(VALID_CHANNELS),
        help="Channel to process: DCOMM, HSM, MINIS, or HABA",
    )
    parser.add_argument(
        "--refresh-bfp",
        default="false",
        help="Refresh the current-period BFP parquet before processing",
    )
    parser.add_argument(
        "--refresh-mapping",
        default="true",
        help="Re-extract mapping parquets from Excel before processing",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        help="Start date (YYYY-MM-DD) of the window to process. "
             "If omitted, defaults to the day after the last date already in the table.",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        help="End date (YYYY-MM-DD) of the window to process. Defaults to today.",
    )
    parser.add_argument(
        "--full-refresh",
        default="false",
        help="If true, ignore prior history and reprocess all scrape files "
             "(replaces the entire price_track output).",
    )
    args = parser.parse_args()
    channel         = args.channel.upper()
    refresh_bfp     = args.refresh_bfp.lower() == "true"
    refresh_mapping = args.refresh_mapping.lower() == "true"
    full_refresh    = args.full_refresh.lower() == "true"
    from_date       = _parse_date(args.from_date)
    to_date         = _parse_date(args.to_date)
    log.info(
        "Args — channel=%s, refresh_bfp=%s, refresh_mapping=%s, from_date=%s, to_date=%s, full_refresh=%s",
        channel, refresh_bfp, refresh_mapping, from_date, to_date, full_refresh,
    )
    return channel, refresh_bfp, refresh_mapping, from_date, to_date, full_refresh


def main():
    channel, refresh_bfp, refresh_mapping, from_date, to_date, full_refresh = get_args()


    log.info("=" * 60)
    log.info("Price Tracking Pipeline")
    log.info("Channel:         %s", channel)
    log.info("Refresh BFP:     %s", refresh_bfp)
    log.info("Refresh Mapping: %s", refresh_mapping)
    log.info("=" * 60)

    spark = get_spark()

    # Optional: rebuild current BFP parquet from Excel before processing
    if refresh_bfp:
        from common.bfp import refresh_current_bfp
        log.info("Refreshing current BFP...")
        refresh_current_bfp()

    channel_kwargs = {
        "from_date":    from_date,
        "to_date":      to_date,
        "full_refresh": full_refresh,
    }

    # Dynamically load and run the channel module
    if channel == "DCOMM":
        from channels import dcomm
        dcomm.run(spark, refresh_mapping=refresh_mapping, **channel_kwargs)

    elif channel == "HSM":
        from channels import hsm
        hsm.run(spark, **channel_kwargs)

    elif channel == "MINIS":
        from channels import minis
        minis.run(spark, **channel_kwargs)

    elif channel == "HABA":
        from channels import haba
        haba.run(spark, **channel_kwargs)

    log.info("Pipeline finished successfully for channel: %s", channel)


if __name__ == "__main__":
    main()
