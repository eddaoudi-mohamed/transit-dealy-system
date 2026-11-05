"""
GTFS-Realtime source inspection.

Inspects the Parquet dataset without modifying it.

Collects:
- Parquet metadata
- Schema and column types
- Row and column counts
- Null values
- Duplicate rows
- Unique values
- Timestamp information
- Sample records
"""

import json
import logging

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# ---------------------------------------------------------
# 1. Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[4]

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "gtfs_realtime"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "inspection"
)

REPORT_PATH = REPORT_DIR / "gtfs_realtime_inspection.json"


# ---------------------------------------------------------
# 2. Configuration
# ---------------------------------------------------------

PARQUET_FILE = RAW_DIR / "nyct_bus_2025-01-24.parquet"

# Update this list after examining the actual schema.
TIMESTAMP_CANDIDATES = [
    "timestamp",
    "ts",
    "event_timestamp",
    "record_timestamp",
]

IDENTIFIER_CANDIDATES = [
    "vehicle_id",
    "trip_id",
    "route_id",
    "stop_id",
]


# ---------------------------------------------------------
# 3. Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# 4. Inspect Parquet metadata
# ---------------------------------------------------------

def inspect_metadata(file_path: Path) -> dict:
    """
    Inspect Parquet metadata without loading all rows.
    """

    parquet_file = pq.ParquetFile(file_path)

    metadata = parquet_file.metadata

    return {
        "file_name": file_path.name,
        "file_size_bytes": file_path.stat().st_size,
        "row_count": metadata.num_rows,
        "column_count": len(parquet_file.schema.names),
        "row_group_count": metadata.num_row_groups,
        "columns": parquet_file.schema.names,
        "schema": str(parquet_file.schema),
    }


# ---------------------------------------------------------
# 5. Inspect one Realtime dataset
# ---------------------------------------------------------

def inspect_realtime(file_path: Path) -> dict:
    """
    Inspect the complete Parquet dataset.

    This function loads the dataset into Pandas.
    For very large datasets, we will optimize this
    to process data in batches.
    """

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    if not file_path.is_file():
        raise ValueError(f"Not a file: {file_path}")

    logger.info("Inspecting GTFS-Realtime: %s", file_path.name)

    # Read metadata first.
    metadata = inspect_metadata(file_path)

    # Load the Parquet data.
    table = pq.read_table(file_path)

    df = table.to_pandas()

    # -----------------------------------------------------
    # Null values
    # -----------------------------------------------------

    null_values = {
        column: int(count)
        for column, count in df.isnull().sum().items()
        if count > 0
    }

    null_percentages = {
        column: round(
            count / len(df) * 100, 2
        ) if len(df) > 0 else 0
        for column, count in null_values.items()
    }

    # -----------------------------------------------------
    # Duplicate rows
    # -----------------------------------------------------

    duplicate_rows = int(df.duplicated().sum())

    # -----------------------------------------------------
    # Unique values
    # -----------------------------------------------------

    unique_values = {
        column: int(df[column].nunique(dropna=True))
        for column in df.columns
    }

    # -----------------------------------------------------
    # Timestamp inspection
    # -----------------------------------------------------

    timestamp_info = {}

    for column in TIMESTAMP_CANDIDATES:

        if column not in df.columns:
            continue

        timestamps = pd.to_datetime(
            df[column],
            errors="coerce",
            utc=True,
        )

        valid_timestamps = timestamps.dropna()

        timestamp_info[column] = {
            "valid_count": int(len(valid_timestamps)),
            "invalid_count": int(timestamps.isna().sum()),
            "minimum": (
                str(valid_timestamps.min())
                if not valid_timestamps.empty
                else None
            ),
            "maximum": (
                str(valid_timestamps.max())
                if not valid_timestamps.empty
                else None
            ),
        }

    # -----------------------------------------------------
    # Identifier inspection
    # -----------------------------------------------------

    identifier_info = {}

    for column in IDENTIFIER_CANDIDATES:

        if column not in df.columns:
            continue

        identifier_info[column] = {
            "unique_count": int(df[column].nunique(dropna=True)),
            "null_count": int(df[column].isnull().sum()),
        }

    # -----------------------------------------------------
    # Sample records
    # -----------------------------------------------------

    sample_records = (
        df.head(5)
        .astype(object)
        .where(pd.notnull(df.head(5)), None)
        .to_dict(orient="records")
    )

    # -----------------------------------------------------
    # Final inspection result
    # -----------------------------------------------------

    result = {
        "source": "GTFS-Realtime",
        "metadata": metadata,
        "columns": list(df.columns),
        "data_types": {
            column: str(dtype)
            for column, dtype in df.dtypes.items()
        },
        "null_values": null_values,
        "null_percentages": null_percentages,
        "duplicate_rows": duplicate_rows,
        "unique_values": unique_values,
        "timestamp_analysis": timestamp_info,
        "identifier_analysis": identifier_info,
        "sample_records": sample_records,
    }

    return result


# ---------------------------------------------------------
# 6. Save report
# ---------------------------------------------------------

def save_report(result: dict) -> None:

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        json.dumps(
            result,
            indent=4,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Inspection report saved: %s",
        REPORT_PATH,
    )


# ---------------------------------------------------------
# 7. Display summary
# ---------------------------------------------------------

def print_summary(result: dict) -> None:

    metadata = result["metadata"]

    print("\n" + "=" * 65)
    print("GTFS REALTIME INSPECTION REPORT")
    print("=" * 65)

    print("File:", metadata["file_name"])

    print("Size (MB):", round(
        metadata["file_size_bytes"] / (1024 ** 2), 2
    ))

    print("Rows:", f"{metadata['row_count']:,}")
    print("Columns:", metadata["column_count"])
    print("Row groups:", metadata["row_group_count"])

    print("\nColumns and data types:")

    for column, dtype in result["data_types"].items():
        print(f"  {column}: {dtype}")

    print("\nNull values:")

    for column, count in result["null_values"].items():
        percentage = result["null_percentages"][column]

        print(f"  {column}: {count:,} ({percentage}%)")

    print("\nDuplicate rows:", result["duplicate_rows"])

    print("\nIdentifier analysis:")

    for column, info in result["identifier_analysis"].items():
        print(
            f"  {column}: "
            f"{info['unique_count']:,} unique, "
            f"{info['null_count']:,} null"
        )

    print("\nTimestamp analysis:")

    for column, info in result["timestamp_analysis"].items():
        print(f"  {column}:")
        print(f"    Minimum: {info['minimum']}")
        print(f"    Maximum: {info['maximum']}")
        print(f"    Invalid: {info['invalid_count']}")

    print("=" * 65)


# ---------------------------------------------------------
# 8. Main entry point
# ---------------------------------------------------------

if __name__ == "__main__":

    inspection_result = inspect_realtime(PARQUET_FILE)

    save_report(inspection_result)

    print_summary(inspection_result)