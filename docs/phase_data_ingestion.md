# Phase 2 — Data Ingestion

## 1. Overview

The objective of this phase is to build a reliable data ingestion pipeline that collects raw public transportation data from external sources and stores it locally for further processing.

This phase focuses on applying data engineering practices rather than simply downloading datasets.

The ingestion pipeline is responsible for:

- Collecting GTFS Static datasets.
- Collecting GTFS-Realtime historical data.
- Downloading files safely.
- Handling network failures and retries.
- Validating downloaded files.
- Tracking ingestion operations through a manifest.
- Avoiding unnecessary repeated downloads.
- Organizing raw data for downstream processing.

The output of this phase is a collection of raw datasets ready for the transformation and processing stages.

---

## 2. Data Sources

The project uses two complementary datasets.

### 2.1 GTFS Static

GTFS Static provides the scheduled transportation information of the New York City bus network.

It describes the planned organization of the transportation system.

The project collects five datasets corresponding to the five NYC boroughs:

| Borough | File |
|---|---|
| Bronx | `google_transit_bronx.zip` |
| Brooklyn | `google_transit_brooklyn.zip` |
| Manhattan | `google_transit_manhattan.zip` |
| Queens | `google_transit_queens.zip` |
| Staten Island | `google_transit_staten_island.zip` |

Each ZIP archive contains files describing the transportation network, including:

- `agency.txt`: Transportation agencies.
- `routes.txt`: Available bus routes.
- `trips.txt`: Individual scheduled trips.
- `stops.txt`: Bus stops and their geographic coordinates.
- `stop_times.txt`: Scheduled arrival and departure times.
- `calendar.txt`: Service operating days.

These files will later be extracted, inspected, cleaned, and transformed.

### 2.2 GTFS-Realtime — Bus Observatory

GTFS-Realtime provides information about the actual operation of public transportation.

Unlike static GTFS, which describes scheduled services, real-time data contains observations about actual vehicle operations.

For this project, we use the Bus Observatory historical dataset.

Source:

https://busobservatory-lake.s3.amazonaws.com/feeds/nyct_mta_bus_gtfsrt/

Selected feed:

`nyct_mta_bus_gtfsrt`

Format:

`Parquet`

Example dataset:

`COMPACTED_nyct_mta_bus_gtfsrt_2025-01-24_18:03:14.parquet`

The Parquet files will be used to investigate bus movements and compare observed transportation behavior with scheduled services.

**Important:** The selected historical dates must be checked against the validity period of the corresponding GTFS Static feed before performing joins.

---

## 3. Ingestion Architecture

The ingestion architecture follows this workflow:

```text
             EXTERNAL DATA SOURCES
                       |
              +--------+--------+
              |                 |
              v                 v
          GTFS Static       GTFS-Realtime
              |                 |
              v                 v
         ZIP Archives      Parquet Files
              |                 |
              +--------+--------+
                       |
                       v
               INGESTION PIPELINE
                       |
              +--------+--------+
              |                 |
              v                 v
        Configuration      Downloader
          settings.yaml     download_files.py
                                |
                       +--------+--------+
                       |                 |
                       v                 v
                 File Validation    Retry Mechanism
                       |                 |
                       +--------+--------+
                                |
                                v
                          RAW STORAGE
                                |
                                v
                         manifest.jsonl
```

---

## 4. Project Structure

The ingestion-related project structure is:

```text
transit-delay-system/
│
├── config/
│   └── settings.yaml
│
├── data/
│   └── raw/
│       ├── gtfs_static/
│       │   ├── google_transit_bronx.zip
│       │   ├── google_transit_brooklyn.zip
│       │   ├── google_transit_manhattan.zip
│       │   ├── google_transit_queens.zip
│       │   └── google_transit_staten_island.zip
│       │
│       ├── gtfs_realtime/
│       │   └── nyct_bus_2025-01-24.parquet
│       │
│       └── manifest.jsonl
│
├── logs/
│
├── src/
│   └── transit_pipeline/
│       ├── __init__.py
│       ├── config.py
│       │
│       └── ingestion/
│           ├── __init__.py
│           ├── download_files.py
│           └── manifest.py
│
└── tests/
    └── test_ingestion.py
```

---

## 5. Configuration Management

The ingestion pipeline uses a YAML configuration file:

`config/settings.yaml`

This file centralizes ingestion parameters, including:

- Raw storage directory.
- Manifest location.
- Log directory.
- Download timeout.
- Maximum retry attempts.
- Retry waiting time.
- Dataset URLs.
- Destination paths.

Example:

```yaml
paths:
  raw_dir: /app/data/raw
  manifest: /app/data/raw/manifest.jsonl
  logs_dir: /app/logs

download:
  timeout_seconds: 120
  max_retries: 3
  backoff_seconds: 2

downloads:
  - name: gtfs_static_manhattan
    kind: gtfs_static
    url: "https://example.com/manhattan.zip"
    dest: gtfs_static/google_transit_manhattan.zip
```

The configuration is loaded through:

`src/transit_pipeline/config.py`

Centralizing configuration avoids hardcoding download parameters throughout the application.

---

## 6. Download Management

The main downloader is implemented in:

`src/transit_pipeline/ingestion/download_files.py`

Its responsibilities include:

### 6.1 Safe downloading

Files are downloaded progressively using HTTP streaming instead of loading the entire file into memory.

This is particularly useful for large Parquet files.

### 6.2 Timeout management

Each request has a maximum execution time.

If the server does not respond within the configured timeout, the download operation fails.

### 6.3 Retry mechanism

Temporary network failures should not immediately interrupt the ingestion process.

The downloader retries failed requests according to the configuration.

Example:

```yaml
download:
  timeout_seconds: 120
  max_retries: 3
  backoff_seconds: 2
```

### 6.4 Temporary files

Incomplete downloads use a temporary `.part` file.

After successful validation, the temporary file is renamed to its final destination.

This prevents incomplete files from being mistaken for valid datasets.

### 6.5 File validation

The downloader checks that downloaded files are usable.

For example:

- Empty files are rejected.
- Invalid ZIP archives are rejected.
- Failed downloads are not retained as completed files.

Parquet files should also be checked for readability using PyArrow.

---

## 7. Manifest Management

The manifest is implemented in:

`src/transit_pipeline/ingestion/manifest.py`

Its purpose is to maintain an ingestion history.

The manifest uses JSON Lines format:

`data/raw/manifest.jsonl`

Each line represents one ingestion record.

Example:

```json
{
  "url": "https://example.com/manhattan.zip",
  "path": "data/raw/gtfs_static/google_transit_manhattan.zip",
  "status": "downloaded"
}
```

The manifest helps the pipeline:

- Track downloaded datasets.
- Identify previously ingested URLs.
- Avoid unnecessary downloads.
- Detect missing local files.
- Maintain ingestion traceability.

### Idempotency

The ingestion pipeline should be idempotent.

This means that running the downloader multiple times should not unnecessarily download files that have already been successfully ingested.

However, if a previously downloaded file is deleted, the pipeline should download it again.

This behavior is tested through the `already_ingested()` function.

---

## 8. Automated Testing

The ingestion tests are implemented in:

`tests/test_ingestion.py`

The project uses Pytest to validate the downloader and manifest behavior.

The tests cover:

| Test | Objective |
|---|---|
| `test_download_success` | Verify successful downloads. |
| `test_retry_then_success` | Verify recovery after temporary HTTP failures. |
| `test_fails_after_retries` | Verify that the downloader stops after maximum retries. |
| `test_empty_file_rejected` | Reject empty downloads. |
| `test_invalid_zip_rejected` | Reject invalid ZIP archives. |
| `test_already_ingested` | Verify ingestion tracking and redownload behavior. |

The tests use mocked HTTP responses to avoid depending on external servers.

To execute the tests inside Docker:

```bash
docker compose exec python pytest -v tests/test_ingestion.py
```

If the package is not installed and the source directory is not configured:

```bash
docker compose exec python sh -c \
"PYTHONPATH=/app/src pytest -v tests/test_ingestion.py"
```

---

## 9. Raw Data Storage

All original datasets are stored in:

`data/raw/`

The raw layer preserves the original downloaded files.

The original data should remain unchanged during subsequent transformations.

This separation allows us to:

- Reprocess datasets when transformation logic changes.
- Compare raw and cleaned records.
- Investigate data quality problems.
- Maintain data lineage.

The raw layer is the starting point for the next processing stages.

---

## 10. Phase 2 Validation Checklist

Before considering ingestion complete, verify the following:

### Data collection

- [x] Download the five GTFS Static archives.
- [ ] Download the selected Bus Observatory Parquet files.
- [ ] Verify that all expected files exist.
- [ ] Verify file sizes.

### File integrity

- [ ] Test ZIP archive integrity.
- [ ] Verify GTFS archive contents.
- [ ] Read Parquet files successfully using PyArrow.
- [ ] Inspect Parquet schemas.

### Ingestion management

- [x] Implement YAML configuration loading.
- [x] Implement the downloader.
- [x] Implement manifest management.
- [ ] Verify manifest records for all datasets.
- [ ] Verify idempotent behavior with actual files.

### Testing

- [x] Implement ingestion unit tests.
- [ ] Execute all ingestion tests successfully.
- [ ] Verify the real download workflow.

---

## 11. Current Phase Status

The five GTFS Static ZIP archives have been downloaded successfully.

The downloader, configuration loader, manifest manager, and ingestion tests have been implemented.

The remaining work is to complete the real-time Parquet ingestion and verify all downloaded datasets.

Therefore, Phase 2 is currently in the validation and completion stage.

---

## 12. Next Phase — Data Processing (Silver Layer)

After completing ingestion, the project moves to Phase 3.

The objective is to transform raw transportation datasets into clean, structured, and analysis-ready datasets.

The planned operations include:

1. Extract GTFS Static ZIP archives.
2. Inspect the schemas of all GTFS tables.
3. Standardize column names and data types.
4. Handle missing and invalid values.
5. Parse GTFS-Realtime Parquet observations.
6. Normalize timestamps.
7. Remove duplicate observations.
8. Prepare relationships between static and real-time datasets.
9. Store cleaned datasets in Parquet format.

The expected output is a Silver Layer containing cleaned and standardized transportation data.

```text
              RAW LAYER
                  |
                  v
          DATA PROCESSING
                  |
          +-------+-------+
          |               |
          v               v
       GTFS Static    GTFS-Realtime
          |               |
          v               v
       Cleaning        Cleaning
          |               |
          +-------+-------+
                  |
                  v
             SILVER LAYER
```

**End of Phase 2 — Data Ingestion.**