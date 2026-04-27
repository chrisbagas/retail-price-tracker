# =============================================================================
# fix_parquet_schemas.py
# ONE-TIME script to normalize all scrape parquet files across all retailers.
#
# What it does:
#   1. Casts basePrice, finalPrice, discountPercent to float64
#   2. Removes junk columns (Unnamed: X, productName.1, etc.)
#   3. Renames "concat"/"CONCAT" → "combinedProductName" (Superindo)
#   4. Migrates old Guardian schema → new unified schema
#   5. Parses Lazada "sold" strings e.g. "140.8K Terjual" → 140800 (Int64)
#
# Run this ONCE on Databricks, then never again (unless scrapers regress).
# =============================================================================

import os
import re
import pandas as pd

from common.config import BASE_PATH

SCRAPE_FOLDERS = {
    # DCOMM
    # "LAZADA":    f"{BASE_PATH}/Price Scraping/Ecomm/Lazada",
    # "BLIBLI":    f"{BASE_PATH}/Price Scraping/Ecomm/Blibli",
    # "TOKOPEDIA": f"{BASE_PATH}/Price Scraping/Ecomm/Tokopedia",
    # # HSM
    # "HYPERMART": f"{BASE_PATH}/Price Scraping/Hsm/Hypermart",
    # "TIPTOP":    f"{BASE_PATH}/Price Scraping/Hsm/Tiptop",
    # "SUPERINDO": f"{BASE_PATH}/Price Scraping/Hsm/Superindo",
    # # MINIS
    "ALFAGIFT":  f"{BASE_PATH}/Price Scraping/Minis/Alfagift",
    # "INDOMARET": f"{BASE_PATH}/Price Scraping/Minis/Indomaret",
    # "ALFAMIDI":  f"{BASE_PATH}/Price Scraping/Minis/Midi",
    # # HABA
    # "WATSON":   f"{BASE_PATH}/Price Scraping/Haba/Watsons",
    # "GUARDIAN": f"{BASE_PATH}/Price Scraping/Haba/Guardian",
}

PRICE_COLS = ["basePrice", "finalPrice", "discountPercent"]

RENAME_MAP = {
    "concat": "combinedProductName",
    "CONCAT": "combinedProductName",
}

# Old Guardian column → new unified column
GUARDIAN_RENAME_MAP = {
    "Name":          "productName",
    "Price":         "finalPrice",
    "Regular Price": "basePrice",
    "Discount (%)":  "discountPercent",
    "Image URL":     "image",
    "Product URL":   "url",
    "Keyword":       "keyword",
}

GUARDIAN_CANONICAL  = ["keyword", "productName", "basePrice", "finalPrice",
                        "discountPercent", "image", "url", "shop", "sold"]


# ---------------------------------------------------------------------------
# Guardian helpers
# ---------------------------------------------------------------------------
def is_old_guardian_schema(pdf: pd.DataFrame) -> bool:
    old_cols = {"Name", "Price", "Regular Price", "Discount (%)", "Image URL", "Product URL"}
    return bool(old_cols & set(pdf.columns))


def fix_guardian_file(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.rename(columns=GUARDIAN_RENAME_MAP, inplace=True)
    if "shop" not in pdf.columns:
        pdf["shop"] = "Guardian"
    if "sold" not in pdf.columns:
        pdf["sold"] = pd.NA
    final_cols = [c for c in GUARDIAN_CANONICAL if c in pdf.columns]
    return pdf[final_cols]


# ---------------------------------------------------------------------------
# Lazada helpers
# ---------------------------------------------------------------------------
def parse_sold_str(val):
    """'140.8K Terjual' → 140800 | '235 Terjual' → 235 | else None"""
    if pd.isna(val) or val is None:
        return None
    match = re.search(r"([\d.]+)\s*([KkMm]?)\s*Terjual", str(val))
    if not match:
        return None
    number = float(match.group(1))
    suffix = match.group(2).upper()
    if suffix == "K":
        number *= 1_000
    elif suffix == "M":
        number *= 1_000_000
    return int(number)


def needs_sold_fix(pdf: pd.DataFrame) -> bool:
    if "sold" not in pdf.columns:
        return False
    sample = pdf["sold"].dropna().astype(str)
    return sample.str.contains("Terjual", case=False).any()


# ---------------------------------------------------------------------------
# Core fixer
# ---------------------------------------------------------------------------
def fix_folder(retailer: str, folder_path: str) -> None:
    if not os.path.exists(folder_path):
        print(f"  SKIP {retailer}: folder not found ({folder_path})")
        return

    files = sorted([f for f in os.listdir(folder_path) if f.endswith(".parquet")])
    if not files:
        print(f"  SKIP {retailer}: no parquet files")
        return

    canonical_cols = (
        GUARDIAN_CANONICAL
        if retailer == "GUARDIAN"
        else list(pd.read_parquet(f"{folder_path}/{files[-1]}").columns)
    )

    fixed_count = 0
    skipped_count = 0

    for filename in files:
        file_path = f"{folder_path}/{filename}"
        try:
            pdf = pd.read_parquet(file_path)
            needs_fix = False

            # ── Guardian: migrate old schema ──────────────────────────────
            if retailer == "GUARDIAN" and is_old_guardian_schema(pdf):
                print(f"  MIGRATING (old schema): {filename}")
                pdf = fix_guardian_file(pdf)
                needs_fix = True

            # ── Lazada: parse "140.8K Terjual" → int ─────────────────────
            elif retailer == "LAZADA" and needs_sold_fix(pdf):
                print(f"  FIXING sold (Terjual strings): {filename}")
                pdf["sold"] = pdf["sold"].apply(parse_sold_str)
                needs_fix = True

            # ── Generic retailers ─────────────────────────────────────────
            else:
                canonical_plus = set(canonical_cols) | set(RENAME_MAP.values())

                for old_name in RENAME_MAP:
                    if old_name in pdf.columns:
                        needs_fix = True
                        break

                if not needs_fix:
                    for col in pdf.columns:
                        if col not in canonical_plus and col not in RENAME_MAP:
                            needs_fix = True
                            break

                if not needs_fix:
                    for col in PRICE_COLS:
                        if col in pdf.columns and pdf[col].dtype != "float64":
                            needs_fix = True
                            break

                if not needs_fix:
                    for scol in ["location", "productName"]:
                        if scol in pdf.columns and pdf[scol].dtype != "object":
                            needs_fix = True
                            break

                if not needs_fix:
                    for icol in ["plu", "sold","stock"]:
                        if icol in pdf.columns and str(pdf[icol].dtype) not in ("Int64", "int64"):
                            needs_fix = True
                            break

                if not needs_fix:
                    skipped_count += 1
                    continue

                # Apply generic fixes
                pdf.rename(columns=RENAME_MAP, inplace=True)
                keep = [c for c in pdf.columns if c in canonical_plus]
                pdf = pdf[keep]

                for scol in ["location", "productName"]:
                    if scol in pdf.columns and pdf[scol].dtype != "object":
                        pdf[scol] = pdf[scol].astype("object").where(pdf[scol].notna(), None)

            # ── Shared: cast price + integer columns (all retailers) ───────
            for col in PRICE_COLS:
                if col in pdf.columns:
                    pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("float64")

            for icol in ["plu", "sold","stock"]:
                if icol in pdf.columns:
                    pdf[icol] = pd.to_numeric(pdf[icol], errors="coerce").astype("Int64")

            pdf.to_parquet(file_path, index=False)
            fixed_count += 1
            print(f"  FIXED: {filename}")

        except Exception as e:
            print(f"  ERROR: {filename} — {e}")

    print(f"  {retailer}: {fixed_count} fixed, {skipped_count} already OK, {len(files)} total")


def main():
    print("=" * 60)
    print("Parquet Schema Normalization — One-Time Fix")
    print("=" * 60)

    for retailer, folder in SCRAPE_FOLDERS.items():
        print(f"\n--- {retailer} ---")
        fix_folder(retailer, folder)

    print("\n" + "=" * 60)
    print("DONE. All parquet files normalized.")
    print("spark.read.parquet() should now work without type errors.")
    print("=" * 60)


if __name__ == "__main__":
    main()