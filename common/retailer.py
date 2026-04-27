# =============================================================================
# common/retailer.py
# Core Spark processing logic shared across all channels and retailers.
# Handles:
#   - Loading raw scrape data and enriching with date/week/year
#   - Joining to BFP periods (Q1, Q2, current) and unioning results
#   - Applying price multipliers (DCOMM channel)
#   - Missing-mapping alert checks
#   - Filtering to final view and saving to parquet
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from common.config import DISPLAY_DATE_FORMAT, FILENAME_DATE_FORMAT
from common.logger import get_logger

log = get_logger(__name__)

# Canonical types for columns that scrapers may write inconsistently.
# When Spark reads with an explicit schema, it can cast compatible types
# (e.g. INT64 → DOUBLE) but NOT incompatible ones (e.g. INT64 → STRING).
# By forcing everything to STRING for text cols and DOUBLE for price cols,
# Spark will read all files consistently.
CANONICAL_TYPES = {
    "productName":         StringType(),
    "combinedProductName": StringType(),
    "location":            StringType(),
    "shop":                StringType(),
    "image":               StringType(),
    "url":                 StringType(),
    "plu":                 LongType(),
    "sold":                LongType(),
    "basePrice":           DoubleType(),
    "finalPrice":          DoubleType(),
    "discountPercent":     DoubleType(),
}



def load_retailer_df(
    spark: SparkSession,
    source_path: str,
    account_name: str,
    filename_regex: str,
    drop_cols: list = None,
    start_date=None,
    end_date=None,
) -> "DataFrame | None":
    """
    Load raw scrape parquet data, extract scrape_date/week/year from filename,
    and tag with account name.

    Assumes parquet files have been pre-normalized (via fix_parquet_schemas.py)
    so all price columns are float64 and schemas are consistent.

    Args:
        spark:              Active Spark session.
        source_path:        Path to the folder of scrape parquet files.
        account_name:       Literal account label to stamp on each row.
        filename_regex:     Regex with one capture group extracting YYMMDD from filename.
        drop_cols:          Extra columns to drop beyond the temp working columns.
        start_date / end_date:
            Optional datetime.date bounds (inclusive). When provided, only files
            whose filename-date falls in [start_date, end_date] are loaded,
            avoiding a full-folder reprocess.

    Returns None if no files fall in the requested window.
    """
    log.info("Loading retailer data: %s from %s", account_name, source_path)
    if start_date or end_date:
        log.info("  Date window: %s → %s", start_date, end_date)

    import os
    import re
    from datetime import datetime

    all_files = sorted([f for f in os.listdir(source_path) if f.endswith(".parquet")])

    # Filter by filename-embedded scrape date when a window is requested.
    if start_date or end_date:
        pat = re.compile(filename_regex)
        filtered = []
        for f in all_files:
            m = pat.search(f)
            if not m:
                continue
            try:
                d = datetime.strptime(m.group(1), "%y%m%d").date()
            except ValueError:
                continue
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue
            filtered.append(f)
        parquet_files = filtered
    else:
        parquet_files = all_files

    if not parquet_files:
        log.warning("  No parquet files matched for %s in window %s → %s", account_name, start_date, end_date)
        return None

    # Use the newest file's schema as a base, then override known problematic
    # columns with canonical types so Spark can read all files without type errors.
    # Prefer the newest file across the full folder so schema is stable even when
    # the requested window is historical.
    newest_reference = all_files[-1] if all_files else parquet_files[-1]
    raw_schema = spark.read.parquet(f"{source_path}/{newest_reference}").schema

    safe_fields = []
    for field in raw_schema:
        if field.name in CANONICAL_TYPES:
            safe_fields.append(StructField(field.name, CANONICAL_TYPES[field.name], True))
        else:
            safe_fields.append(field)
    safe_schema = StructType(safe_fields)

    log.info("  Schema from newest file (%s), with overrides: %s", newest_reference, safe_schema.simpleString())
    log.info("  Reading %d parquet file(s)", len(parquet_files))
    file_paths = [f"{source_path}/{f}" for f in parquet_files]
    df = spark.read.schema(safe_schema).parquet(*file_paths)

    # Cast plu/sold from string to long (read as string to avoid parquet type conflicts)
    for col_name in ["plu", "sold"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))

    df = (
        df
        .withColumn("filename",         F.col("_metadata.file_path"))
        .withColumn("date_str",         F.regexp_extract("filename", filename_regex, 1))
        .withColumn("scrape_date_temp", F.expr(f"try_to_date(date_str, '{FILENAME_DATE_FORMAT}')"))
        .withColumn("scrape_date",      F.date_format("scrape_date_temp", DISPLAY_DATE_FORMAT))
        .withColumn("week",             F.weekofyear("scrape_date_temp"))
        .withColumn("year",             F.year("scrape_date_temp"))
        .withColumn("account",          F.lit(account_name))
        .withColumn("source_file",      F.regexp_replace(
                                            F.regexp_extract("filename", r"([^/\\]+)\.parquet$", 1),
                                            r"_\d{6}$", ""
                                        ))
    )

    cols_to_drop = ["filename", "date_str", "scrape_date_temp"] + (drop_cols or [])
    df = df.drop(*cols_to_drop)

    log.info("  Loaded %s — schema columns: %s", account_name, df.columns)
    return df


def build_bfp_joined_df(
    spark: SparkSession,
    view_name: str,
    mapping_view: str,
    product_col: str,
    sku_col: str,
    bfp_col: str,
    has_multiplier: bool = False,
    contains_mapping_match: bool = False,
    manual_multiplier_df: "DataFrame | None" = None,
    return_raw: bool = False,
) -> "DataFrame | tuple[DataFrame, DataFrame]":
    """
    Join a retailer temp view to all three BFP periods (Q1, Q2, current)
    and union them into a single DataFrame.

    Args:
        spark:                Active Spark session.
        view_name:            Spark temp view name for the retailer data.
        mapping_view:         Spark temp view name for the retailer mapping.
        product_col:          Column in the retailer data used for the product name join.
        sku_col:              Column in the mapping view holding the SKU name.
        bfp_col:              BFP price column name in the BFP temp views.
        has_multiplier:       If True, include Multiplier in select and apply it to prices.
        contains_mapping_match:
            If True, map products using case-insensitive contains fallback
            (after exact match), useful for multipack variants where the
            scraped product name includes the mapped base name.
        manual_multiplier_df:
            Optional DataFrame (from the 'Manual Multiplier Mapping' sheet) with columns
            [SKU NAME, Account, ProductPattern, Multiplier]. When provided, matching rows
            in the combined result will have their Multiplier overridden before price
            division. DCOMM only.
    """
    mapping_cols = set(spark.table(mapping_view).columns)

    # All multiplier-related SQL fragments are built only when has_multiplier=True.
    # When the flag is off (every channel except DCOMM) we skip pack-hint parsing,
    # the derived multiplier CASE, the Multiplier SELECT column, and the price
    # division entirely — belowBFP is computed directly from raw finalPrice.
    if has_multiplier:
        use_mapping_multiplier = "Multiplier" in mapping_cols
        if not use_mapping_multiplier:
            log.info("  Mapping view %s has no Multiplier column — deriving multiplier from product name", mapping_view)

        pack_hint_text_sql = f"""
            trim(regexp_replace(
                CASE
                    WHEN mapping_match.`ProductName` IS NOT NULL
                     AND length(trim(mapping_match.`ProductName`)) > 0
                        THEN replace(
                            lower({view_name}.`{product_col}`),
                            lower(mapping_match.`ProductName`),
                            ' '
                        )
                    ELSE lower({view_name}.`{product_col}`)
                END,
                '\\\\s+',
                ' '
            ))
        """

        derived_multiplier_sql = f"""
            CASE
                WHEN lower({pack_hint_text_sql}) RLIKE 'buy\\\\s+([0-9]+)\\\\s+get\\\\s+([0-9]+)'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), 'buy\\\\s+([0-9]+)\\\\s+get\\\\s+([0-9]+)', 1) AS DOUBLE)
                    + CAST(regexp_extract(lower({pack_hint_text_sql}), 'buy\\\\s+([0-9]+)\\\\s+get\\\\s+([0-9]+)', 2) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE 'beli\\\\s+([0-9]+)\\\\s+gratis\\\\s+([0-9]+)'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), 'beli\\\\s+([0-9]+)\\\\s+gratis\\\\s+([0-9]+)', 1) AS DOUBLE)
                    + CAST(regexp_extract(lower({pack_hint_text_sql}), 'beli\\\\s+([0-9]+)\\\\s+gratis\\\\s+([0-9]+)', 2) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE '\\\\[free\\\\s+[^\\\\]]+\\\\]\\\\s*buy\\\\s+([0-9]+)'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), '\\\\[free\\\\s+[^\\\\]]+\\\\]\\\\s*buy\\\\s+([0-9]+)', 1) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE '\\\\[\\\\s*([0-9]+)\\\\s*pcs?\\\\s*\\\\]'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), '\\\\[\\\\s*([0-9]+)\\\\s*pcs?\\\\s*\\\\]', 1) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE '([0-9]+)\\\\s*pcs?\\\\b'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), '([0-9]+)\\\\s*pcs?\\\\b', 1) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE 'isi\\\\s*[0-9]+'
                    AND NOT lower({view_name}.`{product_col}`) RLIKE 'sariwangi.*isi'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), 'isi\\\\s*([0-9]+)', 1) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) RLIKE 'x\\\\s*[0-9]+'
                    THEN CAST(regexp_extract(lower({pack_hint_text_sql}), 'x\\\\s*([0-9]+)', 1) AS DOUBLE)
                WHEN lower({pack_hint_text_sql}) LIKE '%twinpack%' OR lower({pack_hint_text_sql}) LIKE '%twin pack%'
                    THEN 2.0
                WHEN lower({pack_hint_text_sql}) LIKE '%triplepack%' OR lower({pack_hint_text_sql}) LIKE '%triple pack%'
                    THEN 3.0
                ELSE 1.0
            END
        """

        if use_mapping_multiplier:
            log.info("  Using mapping Multiplier with product-name fallback for null/invalid values")
            effective_multiplier_sql = f"""
                CASE
                    WHEN mapping_match.`Multiplier` IS NOT NULL AND mapping_match.`Multiplier` > 0
                        THEN mapping_match.`Multiplier`
                    ELSE ({derived_multiplier_sql})
                END
            """
        else:
            effective_multiplier_sql = f"({derived_multiplier_sql})"

        multiplier_select = f"        ({effective_multiplier_sql}) AS `Multiplier`,"
        multiplier_below = f"(CAST({view_name}.finalPrice AS DOUBLE) / COALESCE(({effective_multiplier_sql}), 1))"
    else:
        log.info("  has_multiplier=False — skipping pack-hint parsing and price division for %s", view_name)
        use_mapping_multiplier = False
        pack_hint_text_sql = None
        derived_multiplier_sql = None
        multiplier_select = ""
        multiplier_below = f"CAST({view_name}.finalPrice AS DOUBLE)"

    if contains_mapping_match:
        mapping_join_clause = f"""
            LEFT JOIN (
                SELECT
                    retailer_product_name,
                    mapped_product_name AS `ProductName`,
                    mapped_sku AS `{sku_col}`,
                    mapped_status AS Status
                    {', mapped_multiplier AS `Multiplier`' if use_mapping_multiplier else ''}
                FROM (
                    SELECT
                        retailer_names.retailer_product_name,
                        {mapping_view}.`ProductName`   AS mapped_product_name,
                        {mapping_view}.`{sku_col}`       AS mapped_sku,
                        {mapping_view}.Status            AS mapped_status
                        {', ' + mapping_view + '.`Multiplier` AS mapped_multiplier' if use_mapping_multiplier else ''},
                        ROW_NUMBER() OVER (
                            PARTITION BY retailer_names.retailer_product_name
                            ORDER BY
                                CASE
                                    WHEN lower(trim(retailer_names.retailer_product_name)) = lower(trim({mapping_view}.`ProductName`)) THEN 0
                                    ELSE 1
                                END,
                                length(trim({mapping_view}.`ProductName`)) DESC
                        ) AS rn
                    FROM (
                        SELECT DISTINCT `{product_col}` AS retailer_product_name
                        FROM {view_name}
                        WHERE `{product_col}` IS NOT NULL
                    ) retailer_names
                    LEFT JOIN {mapping_view}
                        ON {mapping_view}.`ProductName` IS NOT NULL
                       AND length(trim({mapping_view}.`ProductName`)) > 0
                       AND (
                            lower(trim(retailer_names.retailer_product_name)) = lower(trim({mapping_view}.`ProductName`))
                            OR lower(trim(retailer_names.retailer_product_name)) LIKE concat('%', lower(trim({mapping_view}.`ProductName`)), '%')
                       )
                ) ranked_mapping
                WHERE rn = 1
            ) mapping_match
                ON {view_name}.`{product_col}` = mapping_match.retailer_product_name
        """
    else:
        mapping_join_clause = f"""
            LEFT JOIN {mapping_view} mapping_match
                ON {view_name}.`{product_col}` = mapping_match.`ProductName`
        """

    def _build_query(bfp_view: str, period_filter: str = None, join_on_week: bool = False) -> str:
        where_clause = f"WHERE {period_filter}" if period_filter else ""

        bfp_join = f"""LEFT JOIN {bfp_view}
                    ON mapping_match.`{sku_col}` = {bfp_view}.`SKU NAME`
                    {"AND " + view_name + ".week = " + bfp_view + ".week AND " + view_name + ".year = " + bfp_view + ".year" if join_on_week else ""}"""

        if has_multiplier:
            pack_hint_col    = f"({pack_hint_text_sql}) AS packHintText,"
            derived_mult_col = f"({derived_multiplier_sql}) AS derivedMultiplier,"
        else:
            pack_hint_col    = "CAST(NULL AS STRING) AS packHintText,"
            derived_mult_col = "CAST(NULL AS DOUBLE) AS derivedMultiplier,"

        return f"""
            SELECT {view_name}.*,
                mapping_match.`{sku_col}`,
                {bfp_view}.`{bfp_col}`,
                mapping_match.Status,
                {multiplier_select}
                {pack_hint_col}
                {derived_mult_col}
                IF(
                    {multiplier_below} < {bfp_view}.`{bfp_col}`,
                    True, False
                ) AS belowBFP
            FROM {view_name}
            {mapping_join_clause}
            {bfp_join}
            {where_clause}
        """

    q1      = spark.sql(_build_query("bfp_q1_2025", f"{view_name}.year < 2025 OR ({view_name}.year = 2025 AND {view_name}.week < 21)", join_on_week=False))
    q2      = spark.sql(_build_query("bfp_q2_2025", f"({view_name}.year = 2025 AND {view_name}.week >= 21) OR ({view_name}.year = 2026 AND {view_name}.week < 6)", join_on_week=False))
    current = spark.sql(_build_query("bfp_top58", join_on_week=True))

    combined = q1.unionByName(q2).unionByName(current)
    if has_multiplier:
        # --- Pre-division filters ---
        # Rule 1: global raw price cap — drop anything above 600k before division.
        combined = combined.filter(F.col("finalPrice") <= 600000)

        # Rule 2: Pepsodent-specific — drop twinpack/x2 variants entirely,
        # regardless of price, as they distort unit-price comparisons.
        combined = combined.filter(
            ~(
                (F.col(sku_col) == "PEPSODENT WHITE RL 12X3X225G")
                & (F.col("derivedMultiplier") == 2.0)
            )
        )

        # --- Manual multiplier overrides ---
        # Apply rules from the 'Manual Multiplier Mapping' sheet before dividing
        # prices, so the override is used consistently throughout.
        if manual_multiplier_df is None:
            log.info("  No manual multiplier overrides provided (manual_multiplier_df=None)")
        elif manual_multiplier_df.rdd.isEmpty():
            log.info("  Manual multiplier overrides sheet is empty — nothing to apply")
        else:
            rule_count = manual_multiplier_df.count()
            log.info("  Applying manual multiplier overrides (%d rules loaded)", rule_count)
            manual_multiplier_df.createOrReplaceTempView("_manual_mult_override")
            combined.createOrReplaceTempView("_combined_pre_manual")
            # Join keys (all exact, case-insensitive):
            #   account       = Account
            #   SKUName       = SKU NAME
            #   packHintText  = ProductPattern
            overrides = spark.sql(f"""
                SELECT DISTINCT
                    c.account,
                    c.`{sku_col}`,
                    c.productName,
                    FIRST(CAST(mm.Multiplier AS DOUBLE)) OVER (
                        PARTITION BY c.account, c.`{sku_col}`, c.productName
                        ORDER BY length(mm.ProductPattern) DESC
                    ) AS manualMultiplier
                FROM _combined_pre_manual c
                INNER JOIN _manual_mult_override mm
                    ON  lower(trim(c.account))       = lower(trim(mm.Account))
                    AND lower(trim(c.`{sku_col}`))   = lower(trim(mm.`SKU NAME`))
                    AND lower(trim(c.packHintText))  = lower(trim(mm.ProductPattern))
            """)
            overrides.cache()
            matched_rows = overrides.count()
            distinct_products = overrides.select("account", sku_col, "productName").distinct().count()
            if matched_rows == 0:
                log.warning(
                    "  Manual multiplier: 0 rows in %s matched any rule — overrides had no effect",
                    view_name,
                )
            else:
                log.info(
                    "  Manual multiplier: %d combined rows matched (%d distinct account/SKU/product) in %s",
                    matched_rows, distinct_products, view_name,
                )
                try:
                    overrides.select("account", sku_col, "productName", "manualMultiplier") \
                        .distinct().show(20, truncate=False)
                except Exception:
                    pass
            combined = (
                combined
                .join(F.broadcast(overrides), on=["account", sku_col, "productName"], how="left")
                .withColumn("Multiplier", F.coalesce(F.col("manualMultiplier"), F.col("Multiplier")))
                .withColumn("derivedMultiplier", F.coalesce(F.col("manualMultiplier"), F.col("derivedMultiplier")))
                .drop("manualMultiplier")
            )

        # Snapshot before division — returned as raw output when return_raw=True.
        raw_combined = combined if return_raw else None

        log.info("  Applying multiplier to basePrice and finalPrice")
        combined = (
            combined
            .withColumn(
                "basePrice",
                F.when(F.col("Multiplier").isNotNull(), F.col("basePrice") / F.col("Multiplier"))
                 .otherwise(F.col("basePrice")),
            )
            .withColumn(
                "finalPrice",
                F.when(F.col("Multiplier").isNotNull(), F.col("finalPrice") / F.col("Multiplier"))
                 .otherwise(F.col("finalPrice")),
            )
            .withColumn(
                "belowBfp",
                F.when(
                    F.col(f"`{bfp_col}`").isNotNull(),
                    F.col("finalPrice") < F.col(f"`{bfp_col}`"),
                ).otherwise(F.lit(False)),
            )
        )

        # Drop promotional bundle listings (buy-X-get-free, gratis) from the
        # processed output only — these skew unit-price comparisons.
        combined = combined.filter(
            ~(
                F.lower(F.col("packHintText")).like("%free%")
                | F.lower(F.col("packHintText")).like("%gratis%")
                | F.lower(F.col("packHintText")).like("%bundle%")
                | F.lower(F.col("packHintText")).like("%brand box%")
                | F.lower(F.col("packHintText")).like("%mystery box%")
            )
        )

    display(combined)
    if return_raw and has_multiplier:
        return combined, raw_combined
    return combined


def check_missing_mappings(
    spark: SparkSession,
    view_name: str,
    mapping_view: str,
    product_col: str,       # retailer's product name column  (e.g. "productName")
    sku_col: str,            # mapping table's SKU column      (e.g. "SKUName")
    bfp_col: str,            # BFP summary column              (e.g. "BFP Summary Minis")
    product_name_col: str = "ProductName",  # mapping table's product name column
    has_competitor: bool = True,
    contains_mapping_match: bool = False,
) -> None:
    """
    Alert on any Top 58 SKUs that have no mapping or were not found in the
    retailer scrape data for the current week.

    Logs a WARNING if gaps are found; logs INFO if everything is mapped.

    Args:
        spark:            Active SparkSession.
        view_name:        Retailer scrape view  (e.g. "lazada").
        mapping_view:     SKU→Product mapping view (e.g. "mapping_lazada").
        product_col:      Retailer product-name column in `view_name`
                          (e.g. "productName").
        sku_col:          SKU column in `mapping_view` that joins to
                          bfp_top58.`SKU NAME`  (e.g. "SKUName").
        bfp_col:          BFP summary column in bfp_top58
                          (e.g. "BFP Summary Minis").
        product_name_col: Product-name column in `mapping_view` that joins
                          to `view_name`.`product_col`
                          (e.g. "ProductName").  Defaults to "ProductName".
    """
    log.info("Checking missing mappings for: %s", view_name)

    mapping_product_join = (
        f"""
            lower(trim({view_name}.`{product_col}`)) = lower(trim({mapping_view}.`{product_name_col}`))
            OR lower(trim({view_name}.`{product_col}`)) LIKE concat('%', lower(trim({mapping_view}.`{product_name_col}`)), '%')
        """
        if contains_mapping_match
        else f"{view_name}.`{product_col}` = {mapping_view}.`{product_name_col}`"
    )

    missing_df = spark.sql(f"""
        WITH sku_check AS (
            SELECT
                   bfp_top58.`SKU NAME`,
                   bfp_top58.week,
                   bfp_top58.year,
                   bfp_top58.`{bfp_col}`,
                   COUNT(DISTINCT {mapping_view}.`{sku_col}`)  AS mapping_count,
                   COUNT(DISTINCT CASE
                       WHEN {view_name}.`{product_col}` IS NOT NULL
                       THEN {view_name}.`{product_col}`
                   END)                                        AS found_count
            FROM bfp_top58
            LEFT JOIN {mapping_view}
                   ON {mapping_view}.`{sku_col}` = bfp_top58.`SKU NAME`
            LEFT JOIN {view_name}
                     ON ({mapping_product_join})
                  AND {view_name}.week             = bfp_top58.week
                  AND {view_name}.year             = bfp_top58.year
            WHERE bfp_top58.week = weekofyear(current_date())
              AND bfp_top58.year = year(current_date())
            GROUP BY
                   bfp_top58.`SKU NAME`,
                   bfp_top58.week,
                   bfp_top58.year,
                   bfp_top58.`{bfp_col}`
        )
        SELECT
               `SKU NAME`,
               week,
               year,
               `{bfp_col}`,
               mapping_count,
               found_count,
               CASE
                   WHEN mapping_count = 0 THEN 'NO MAPPING IN {view_name}'
                   WHEN found_count   = 0 THEN 'NOT FOUND IN {view_name} STORE (None of the variants)'
                   ELSE 'FOUND'
               END AS issue_type
        FROM sku_check
        WHERE mapping_count = 0
           OR found_count   = 0
        ORDER BY issue_type, `SKU NAME`
    """)

    missing_count = missing_df.count()
    if missing_count > 0:
        label = "Top 58/Competitor" if has_competitor else "Top 58"
        log.warning(
            "ALERT: %d %s products have MISSING MAPPINGS in %s",
            missing_count,
            label,
            view_name,
        )
        try:
            display(missing_df)  # noqa: F821 — Databricks built-in
        except NameError:
            missing_df.show(truncate=False)
    else:
        log.info("All Top 58 products mapped correctly for %s", view_name)


def build_final_view(
    df: DataFrame,
    sku_col: str,
    bfp_col: str,
    has_competitor: bool = True,
) -> DataFrame:
    """
    Filter to only Top 58 (and optionally Competitor) rows that have a valid
    mapping, and deduplicate.

    Args:
        df:             Input DataFrame after BFP join.
        sku_col:        SKU column name.
        bfp_col:        BFP price column name.
        has_competitor:  If True, also keep rows with Status='Competitor'.
    """
    if has_competitor:
        filter_expr = (
            f"(Status = 'Competitor' OR `{bfp_col}` IS NOT NULL) "
            f"AND `{sku_col}` IS NOT NULL"
        )
    else:
        filter_expr = f"`{bfp_col}` IS NOT NULL AND `{sku_col}` IS NOT NULL"

    return (
        df
        .filter(filter_expr)
        .dropDuplicates()
    )


def save_retailer(df: DataFrame, output_path: str, label: str) -> None:
    """Write a retailer's final view to parquet (single file)."""
    log.info("Saving %s → %s", label, output_path)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    log.info("  Saved successfully")
