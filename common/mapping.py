# =============================================================================
# common/mapping.py
# Extracts product mapping definitions from Excel and writes them to parquet.
# Handles two mapping formats:
#   - "column-pair" format (DCOMM / HSM): TOP 58 + retailer column pairs
#   - "sheet-per-retailer" format (Minis): one sheet per retailer definition
# =============================================================================

import os
import pandas as pd

from common.config import MAPPING_OUTPUT_DIR
from common.logger import get_logger

log = get_logger(__name__)


def extract_column_pair_mapping(
    excel_path: str,
    retailers: dict,
    mapping_sheet: str,
    mapping_sku_col: str,
    competitor_sheet: str = None,
    competitor_sku_col: str = None,
    has_multiplier: bool = False,
) -> dict:
    """
    Extract mapping definitions where each retailer is a column in a shared sheet.
    Used by DCOMM and HSM channels.

    Returns:
        Dict mapping retailer_code → pandas DataFrame (the combined mapping).
        Also writes each DataFrame to parquet for persistence.

    Args:
        excel_path:          Path to the source Excel file.
        retailers:           Dict of retailer configs — see channels/dcomm.py for shape.
        mapping_sheet:       Sheet name for Top 58 mapping data.
        mapping_sku_col:     Column name for the SKU identifier in the mapping sheet.
        competitor_sheet:    Sheet name for competitor mapping data (None = no competitors).
        competitor_sku_col:  Column name for the SKU identifier in the competitor sheet.
        has_multiplier:      Whether retailer configs include a multiplier column (DCOMM only).
    """
    log.info("Reading mapping Excel: %s", excel_path)
    df_mapping = pd.read_excel(excel_path, sheet_name=mapping_sheet)
    log.info("  Mapping sheet shape:    %s", df_mapping.shape)

    df_competitor = None
    if competitor_sheet:
        df_competitor = pd.read_excel(excel_path, sheet_name=competitor_sheet)
        log.info("  Competitor sheet shape: %s", df_competitor.shape)
    else:
        log.info("  No competitor sheet provided — skipping competitor extraction")

    os.makedirs(MAPPING_OUTPUT_DIR, exist_ok=True)

    result = {}
    for retailer_code, cols in retailers.items():
        log.info("  Processing retailer: %s", retailer_code)

        # --- Top 58 rows ---
        top58_cols = [mapping_sku_col, cols["mapping_col"]]
        if has_multiplier:
            top58_cols.append(cols["multiplier_col"])

        top58 = df_mapping[top58_cols].dropna(subset=[cols["mapping_col"]]).copy()
        rename = {"mapping_col": "ProductName", mapping_sku_col: "SKUName"}
        top58.rename(columns={cols["mapping_col"]: "ProductName", mapping_sku_col: "SKUName"}, inplace=True)
        if has_multiplier:
            top58.rename(columns={cols["multiplier_col"]: "Multiplier"}, inplace=True)
        top58["Status"] = "Top 58"

        # --- Competitor rows (only if competitor sheet and column are available) ---
        competitor_col = cols.get("competitor_col")
        if df_competitor is not None and competitor_col:
            comp_cols = [competitor_sku_col, competitor_col]
            if has_multiplier:
                comp_cols.append(cols["multiplier_col"])

            comp = df_competitor[comp_cols].dropna(subset=[competitor_col]).copy()
            comp.rename(columns={competitor_col: "ProductName", competitor_sku_col: "SKUName"}, inplace=True)
            if has_multiplier:
                comp.rename(columns={cols["multiplier_col"]: "Multiplier"}, inplace=True)
            comp["Status"] = "Competitor"

            combined = pd.concat([top58, comp], ignore_index=True)
        else:
            combined = top58
            log.info("  No competitor data for %s — Top 58 only", retailer_code)

        result[retailer_code] = combined

        output_path = os.path.join(MAPPING_OUTPUT_DIR, f"{retailer_code}_Definition.parquet")
        combined.to_parquet(output_path, index=False)
        n_comp = len(combined) - len(top58)
        log.info(
            "  Saved %d rows (%d Top 58 + %d Competitor) → %s",
            len(combined), len(top58), n_comp, output_path,
        )

    return result


def extract_manual_multiplier_mapping(excel_path: str) -> pd.DataFrame:
    """
    Read the 'Manual Multiplier Mapping' sheet from the DCOMM Excel file.
    Used for edge-case product listings (e.g. "[sale !] multipack" bundles)
    that need a specific multiplier not derivable from the product name alone.

    Expected sheet columns:
        SKU NAME       — BFP SKU name to match (exact, case-insensitive)
        Account        — Retailer account name (e.g. "tokopedia")
        ProductPattern — Substring to match against the scraped productName
                         (case-insensitive LIKE match)
        Multiplier     — The override multiplier to apply

    Returns:
        pandas DataFrame of valid (non-null) override rules.
    """
    log.info("Reading manual multiplier mapping from: %s", excel_path)
    df = pd.read_excel(excel_path, sheet_name="Manual Multiplier Mapping")
    df = df.dropna(subset=["ProductPattern", "Multiplier"])
    df["Multiplier"] = pd.to_numeric(df["Multiplier"], errors="coerce")
    df = df.dropna(subset=["Multiplier"])
    log.info("  Loaded %d manual multiplier rules", len(df))
    return df


def extract_sheet_mapping(excel_path: str, sheet_retailer_map: dict) -> dict:
    """
    Extract mapping definitions where each retailer has its own sheet.
    Used by the Minis channel.

    Returns:
        Dict mapping output_name → pandas DataFrame.
        Also writes each DataFrame to parquet for persistence.

    Args:
        excel_path:         Path to the source Excel file.
        sheet_retailer_map: Dict mapping sheet names to output parquet filenames,
                            e.g. {"ALF-Definition": "ALF_Definition"}.
    """
    log.info("Reading sheet-based mapping Excel: %s", excel_path)
    os.makedirs(MAPPING_OUTPUT_DIR, exist_ok=True)

    result = {}
    for sheet_name, output_name in sheet_retailer_map.items():
        log.info("  Processing sheet: %s", sheet_name)
        df = pd.read_excel(excel_path, sheet_name=sheet_name)

        # Standardize column names to match downstream expectations
        df = df.rename(columns={
            "Product Name": "ProductName",
            "SKU name":     "SKUName",
        })

        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str)
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        result[output_name] = df

        output_path = os.path.join(MAPPING_OUTPUT_DIR, f"{output_name}.parquet")
        df.to_parquet(output_path, index=False)
        log.info("  Saved %d rows → %s", len(df), output_path)

    return result
