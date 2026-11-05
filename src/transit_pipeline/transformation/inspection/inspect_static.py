"""
GTFS Static source inspection.

This module inspects extracted GTFS files without modifying them.

For each borough and each TXT file, it collects:
- Row and column counts
- Column names
- Source data types
- Empty values
- Duplicate rows
- Unique values per column
- Sample records

The result is saved as a JSON report.
"""

import json
import logging

from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# 1. Project paths
# ---------------------------------------------------------

# File location:
# src/transit_pipeline/transformation/inspection/inspect_static.py

PROJECT_ROOT = Path(__file__).resolve().parents[4]

STAGING_DIR = (
    PROJECT_ROOT
    / "data"
    / "staging"
    / "gtfs_static"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "inspection"
)

REPORT_PATH = REPORT_DIR / "gtfs_static_inspection.json"


# ---------------------------------------------------------
# 2. Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# 3. Inspect one GTFS file
# ---------------------------------------------------------

def inspect_file(file_path: Path, borough: str) -> dict:
    """
    Inspect one GTFS TXT file.

    Values are read as strings to preserve identifiers
    and avoid changing the original source representation.
    """

    logger.info(
        "Inspecting %s | Borough: %s",
        file_path.name,
        borough,
    )

    # Read the GTFS file.
    # keep_default_na=False prevents strings such as "NA"
    # from being interpreted automatically as missing values.
    df = pd.read_csv(
        file_path,
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
        on_bad_lines="error",
    )

    # Count empty or whitespace-only cells.
    empty_values = {}

    for column in df.columns:
        empty_count = (
            df[column]
            .astype(str)
            .str.strip()
            .eq("")
            .sum()
        )

        if empty_count > 0:
            empty_values[column] = int(empty_count)

    # Count fully duplicated rows.
    duplicate_rows = int(df.duplicated().sum())

    # Count distinct values for each column.
    unique_values = {
        column: int(df[column].nunique(dropna=False))
        for column in df.columns
    }

    # Get a small sample of the source data.
    sample_records = (
        df.head(5)
        .to_dict(orient="records")
    )

    # Describe the source structure.
    result = {
        "borough": borough,
        "file_name": file_path.name,
        "file_path": str(
            file_path.relative_to(PROJECT_ROOT)
        ),
        "file_size_bytes": file_path.stat().st_size,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "source_data_types": {
            column: str(dtype)
            for column, dtype in df.dtypes.items()
        },
        "empty_values": empty_values,
        "duplicate_rows": duplicate_rows,
        "unique_values": unique_values,
        "sample_records": sample_records,
    }

    return result


# ---------------------------------------------------------
# 4. Inspect all files for one borough
# ---------------------------------------------------------

def inspect_borough(borough_dir: Path) -> list[dict]:
    """
    Inspect every TXT file belonging to one borough.
    """

    borough = borough_dir.name

    txt_files = sorted(
        borough_dir.rglob("*.txt")
    )

    if not txt_files:
        logger.warning(
            "No TXT files found for borough: %s",
            borough,
        )

        return []

    results = []

    for file_path in txt_files:

        try:
            result = inspect_file(
                file_path=file_path,
                borough=borough,
            )

            results.append(result)

        except Exception as error:
            logger.exception(
                "Inspection failed for %s",
                file_path,
            )

            results.append({
                "borough": borough,
                "file_name": file_path.name,
                "file_path": str(file_path),
                "status": "failed",
                "error": str(error),
            })

    return results


# ---------------------------------------------------------
# 5. Inspect all boroughs
# ---------------------------------------------------------

def inspect_all() -> list[dict]:
    """
    Discover borough directories and inspect their GTFS files.
    """

    if not STAGING_DIR.exists():
        raise FileNotFoundError(
            f"Staging directory does not exist: {STAGING_DIR}"
        )

    borough_dirs = sorted(
        path
        for path in STAGING_DIR.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )

    if not borough_dirs:
        raise FileNotFoundError(
            f"No borough directories found in: {STAGING_DIR}"
        )

    all_results = []

    for borough_dir in borough_dirs:

        logger.info(
            "Starting inspection for: %s",
            borough_dir.name,
        )

        borough_results = inspect_borough(borough_dir)

        all_results.extend(borough_results)

    return all_results


# ---------------------------------------------------------
# 6. Save inspection report
# ---------------------------------------------------------

def save_report(results: list[dict]) -> None:
    """
    Save inspection results as a JSON report.
    """

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    report = {
        "source": "GTFS Static",
        "total_files_inspected": len(results),
        "successful_files": sum(
            "error" not in result
            for result in results
        ),
        "failed_files": sum(
            "error" in result
            for result in results
        ),
        "files": results,
    }

    REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=4,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Inspection report saved to: %s",
        REPORT_PATH,
    )


# ---------------------------------------------------------
# 7. Display summary
# ---------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    """
    Print a readable summary of inspected GTFS files.
    """

    print("\n" + "=" * 75)
    print("GTFS STATIC SOURCE INSPECTION")
    print("=" * 75)

    for result in results:

        print(
            f"\nBorough: {result['borough']}"
        )

        print(
            f"File: {result['file_name']}"
        )

        if "error" in result:
            print(
                f"Status: FAILED | {result['error']}"
            )
            continue

        print(
            f"Rows: {result['row_count']:,}"
        )

        print(
            f"Columns: {result['column_count']}"
        )

        print(
            f"Duplicates: {result['duplicate_rows']:,}"
        )

        print(
            f"Columns names: {result['columns']}"
        )

        print(
            f"Columns with empty values: "
            f"{len(result['empty_values'])}"
        )

    successful = sum(
        "error" not in result
        for result in results
    )

    failed = len(results) - successful

    print("\n" + "-" * 75)
    print(f"Files inspected successfully: {successful}")
    print(f"Files failed: {failed}")
    print("=" * 75)


# ---------------------------------------------------------
# 8. Main entry point
# ---------------------------------------------------------

if __name__ == "__main__":

    inspection_results = inspect_all()

    save_report(inspection_results)

    print_summary(inspection_results)

    if any("error" in result for result in inspection_results):
        raise SystemExit(1)