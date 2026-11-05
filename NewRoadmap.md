| Phase | Description                                | Status           |
| ----- | ------------------------------------------ | ---------------- |
| 1     | Environment setup (Docker, Docker Compose) | Completed        |
| 2     | Data ingestion (GTFS Static + Realtime)    | Almost completed |
| 3     | Data processing (Silver Layer)             | Next             |
| 4     | Data quality and validation                | Upcoming         |
| 5     | PySpark processing                         | Upcoming         |
| 6     | PostgreSQL data warehouse                  | Upcoming         |
| 7     | Analytics and KPI generation               | Upcoming         |
| 8     | Airflow orchestration                      | Upcoming         |
| 9     | Kafka and Spark Streaming                  | Upcoming         |
| 10    | Delay prediction model                     | Upcoming         |
| 11    | Grafana dashboard and alerts               | Upcoming         |


2. What should you do immediately after downloading the data?

You should follow the Medallion Architecture.

This architecture organizes data into three layers:

Bronze: Raw data.
Silver: Cleaned and transformed data.
Gold: Business-ready analytical data.

Your architecture:


                 DATA SOURCES
                      |
             +--------+--------+
             |                 |
         GTFS Static       GTFS Realtime
             |                 |
             +--------+--------+
                      |
                      v
              PHASE 2: INGESTION
                      |
                      v
                 BRONZE LAYER
                (data/raw/)
                      |
                      v
             PHASE 3: PROCESSING
                      |
              +-------+-------+
              |               |
              v               v
         Static Data      Realtime Data
         Processing       Processing
              |               |
              +-------+-------+
                      |
                      v
                 SILVER LAYER
                (data/silver/)
                      |
                      v
             PHASE 4: DATA QUALITY
                      |
                      v
                PHASE 5: PYSPARK
                      |
                      v
                  GOLD LAYER
                (data/gold/)
                      |
                      v
              POSTGRESQL DATABASE
                      |
                      v
                GRAFANA DASHBOARD





## phase of transformation


| Milestone | Description | Status |
|---|---|---|
| M1 | Bronze data verification | 🟡 completed |
| M2 | GTFS Static extraction | completed |
| M3 | Source data inspection | starting |
| M4 | Data dictionary | Pending |
| M5 | Define transformation rules | Pending |
| M6 | GTFS Static transformation | Pending |
| M7 | GTFS-Realtime transformation | Pending |
| M8 | Data quality validation | Pending |
| M9 | Silver Parquet storage | Pending |
| M10 | Pipeline integration and testing | Pending |



cd ~/Documents/projects/transit-delay-system

mkdir -p \
    data/staging/gtfs_static \
    data/processed/silver/gtfs_static \
    data/processed/silver/gtfs_realtime \
    data/processed/quarantine/invalid_stops \
    data/processed/quarantine/invalid_trips \
    data/processed/quarantine/invalid_stop_times \
    data/processed/quarantine/invalid_realtime \
    src/transit_pipeline/transformation/extract \
    src/transit_pipeline/transformation/inspection \
    src/transit_pipeline/transformation/cleaning \
    src/transit_pipeline/transformation/validation \
    src/transit_pipeline/transformation/storage \
    tests/transformation \


file created in this phase 

touch \
    src/transit_pipeline/transformation/__init__.py \
    src/transit_pipeline/transformation/pipeline.py \
    \
    src/transit_pipeline/transformation/extract/__init__.py \
    src/transit_pipeline/transformation/extract/extract_gtfs.py \
    \
    src/transit_pipeline/transformation/inspection/__init__.py \
    src/transit_pipeline/transformation/inspection/inspect_static.py \
    src/transit_pipeline/transformation/inspection/inspect_realtime.py \
    \
    src/transit_pipeline/transformation/cleaning/__init__.py \
    src/transit_pipeline/transformation/cleaning/clean_static.py \
    src/transit_pipeline/transformation/cleaning/clean_realtime.py \
    \
    src/transit_pipeline/transformation/validation/__init__.py \
    src/transit_pipeline/transformation/validation/validate_static.py \
    src/transit_pipeline/transformation/validation/validate_realtime.py \
    \
    src/transit_pipeline/transformation/storage/__init__.py \
    src/transit_pipeline/transformation/storage/parquet_writer.py \
    \
    tests/transformation/__init__.py \
    tests/transformation/test_extract.py \
    tests/transformation/test_inspection.py \
    tests/transformation/test_clean_static.py \
    tests/transformation/test_clean_realtime.py \
    tests/transformation/test_validation.py \
    tests/transformation/test_pipeline.py

these are distination dicrectories , we will only populate these corresponding to dataset actualy present in source 

mkdir -p \
    data/processed/silver/gtfs_static/agency \
    data/processed/silver/gtfs_static/routes \
    data/processed/silver/gtfs_static/stops \
    data/processed/silver/gtfs_static/trips \
    data/processed/silver/gtfs_static/stop_times \
    data/processed/silver/gtfs_static/calendar \
    data/processed/silver/gtfs_static/calendar_dates \
    data/processed/silver/gtfs_realtime/vehicle_positions \
    data/processed/silver/gtfs_realtime/trip_updates







    # Data Directory Structure — Phase 3: Transformation

## 1. Overview

The `data/` directory contains all datasets used and generated by the Bus Observatory project.

During Phase 3, we transform the original data collected during ingestion into clean, validated, and structured datasets.

We organize data into three main layers:

1. **Raw (Bronze):** Original downloaded data, preserved without modification.
2. **Staging:** Intermediate files extracted from the original archives.
3. **Processed (Silver):** Cleaned and validated datasets ready for analysis and future data processing.

We also use a **Quarantine** area to store invalid records that cannot be included in the Silver datasets.

---

## 2. Complete Directory Structure

```text
data/
│
├── raw/
│   ├── gtfs_static/
│   │   ├── google_transit_bronx.zip
│   │   ├── google_transit_brooklyn.zip
│   │   ├── google_transit_manhattan.zip
│   │   ├── google_transit_queens.zip
│   │   └── google_transit_staten_island.zip
│   │
│   └── manifest.jsonl
│
├── staging/
│   └── gtfs_static/
│
└── processed/
    │
    ├── silver/
    │   │
    │   ├── gtfs_static/
    │   │   ├── agency/
    │   │   ├── routes/
    │   │   ├── stops/
    │   │   ├── trips/
    │   │   ├── stop_times/
    │   │   ├── calendar/
    │   │   └── calendar_dates/
    │   │
    │   └── gtfs_realtime/
    │       ├── vehicle_positions/
    │       └── trip_updates/
    │
    └── quarantine/
        ├── invalid_stops/
        ├── invalid_trips/
        ├── invalid_stop_times/
        └── invalid_realtime/
```

---

# 3. Raw Directory — Bronze Layer

### Location

```text
data/raw/
```

### Role

The `raw/` directory stores the original data downloaded from external sources.

This is our **Bronze layer**.

The main principle is to preserve the original data without modifying it.

If we make a mistake during transformation, we can always return to the original files and process them again.

### 3.1 `raw/gtfs_static/`

```text
data/raw/gtfs_static/
```

Contains the original GTFS Static ZIP archives downloaded from the MTA.

Each ZIP archive corresponds to a geographical area:

| File | Description |
|---|---|
| `google_transit_bronx.zip` | Static transit data for the Bronx |
| `google_transit_brooklyn.zip` | Static transit data for Brooklyn |
| `google_transit_manhattan.zip` | Static transit data for Manhattan |
| `google_transit_queens.zip` | Static transit data for Queens |
| `google_transit_staten_island.zip` | Static transit data for Staten Island |

These archives contain GTFS tables such as:

- `agency.txt`
- `routes.txt`
- `stops.txt`
- `trips.txt`
- `stop_times.txt`
- `calendar.txt`
- `calendar_dates.txt`
- `shapes.txt`

We will extract and inspect these files during the transformation phase.

**Important:** We must not directly modify or delete the original ZIP archives.

### 3.2 `raw/manifest.jsonl`

```text
data/raw/manifest.jsonl
```

This file maintains a record of downloaded files.

It helps us track the ingestion process and avoid downloading the same data unnecessarily.

Each line represents one download record.

A record may contain information such as:

- File name.
- Source URL.
- Destination path.
- File size.
- Download timestamp.
- Download status.
- File checksum, if implemented.

Example (illustrative):

```json
{
  "name": "gtfs_static_manhattan",
  "status": "success",
  "path": "gtfs_static/google_transit_manhattan.zip"
}
```

**Role:** Maintain traceability of the original datasets and their ingestion history.

---

# 4. Staging Directory — Intermediate Layer

### Location

```text
data/staging/
```

### Role

The staging directory stores intermediate data extracted from the original archives.

It acts as a temporary working area between Bronze and Silver.

We do not directly transform the original ZIP files. Instead, we extract their contents into staging.

### 4.1 `staging/gtfs_static/`

```text
data/staging/gtfs_static/
```

Contains the extracted GTFS Static files.

The extraction process will create a separate directory for each borough.

Expected structure:

```text
data/staging/gtfs_static/
│
├── bronx/
│   ├── agency.txt
│   ├── routes.txt
│   ├── stops.txt
│   ├── trips.txt
│   └── stop_times.txt
│
├── brooklyn/
│   ├── agency.txt
│   ├── routes.txt
│   ├── stops.txt
│   └── ...
│
├── manhattan/
│   ├── agency.txt
│   ├── routes.txt
│   ├── stops.txt
│   └── ...
│
├── queens/
│   └── ...
│
└── staten_island/
    └── ...
```

The actual files depend on the contents of each downloaded ZIP archive.

### Why do we need staging?

Staging allows us to:

1. Extract ZIP archives without modifying Bronze data.
2. Inspect the original GTFS tables.
3. Understand the schema and data types.
4. Apply cleaning and transformation rules.
5. Prepare datasets for validation.

**Important:** Staging is an intermediate layer. Its contents can be regenerated from the original ZIP archives.

---

# 5. Processed Directory — Silver Layer

### Location

```text
data/processed/
```

### Role

The `processed/` directory contains the outputs generated by our transformation pipeline.

Our main objective is to convert raw and intermediate data into clean, consistent, and validated datasets.

The Silver layer will contain data stored primarily in **Parquet format**.

Parquet is a columnar storage format that provides efficient compression and analytical queries.

---

## 5.1 `processed/silver/`

```text
data/processed/silver/
```

This directory contains datasets that have passed our transformation and validation rules.

Before publishing data into Silver, we should:

- Standardize column names.
- Apply appropriate data types.
- Handle missing values according to defined rules.
- Remove duplicates using appropriate business keys.
- Validate identifiers and relationships.
- Preserve useful source and processing metadata.

We divide Silver into two main categories.

---

## 5.2 `silver/gtfs_static/`

```text
data/processed/silver/gtfs_static/
```

Contains cleaned and validated GTFS Static datasets.

Each GTFS table has its own directory.

### `agency/`

```text
data/processed/silver/gtfs_static/agency/
```

Contains information about transit agencies.

The `agency.txt` table describes the organization operating the transit service.

Typical columns:

- `agency_id`
- `agency_name`
- `agency_url`
- `agency_timezone`

**Purpose:** Identify and describe the transit agencies.

### `routes/`

```text
data/processed/silver/gtfs_static/routes/
```

Contains information about transit routes.

The `routes.txt` table describes the different bus routes.

Typical columns:

- `route_id`
- `agency_id`
- `route_short_name`
- `route_long_name`
- `route_type`

**Purpose:** Identify bus routes and their associated agencies.

### `stops/`

```text
data/processed/silver/gtfs_static/stops/
```

Contains information about bus stops.

The `stops.txt` table describes the geographical locations where passengers can board or leave buses.

Typical columns:

- `stop_id`
- `stop_name`
- `stop_lat`
- `stop_lon`

**Purpose:** Provide geographical information about bus stops.

### `trips/`

```text
data/processed/silver/gtfs_static/trips/
```

Contains information about individual scheduled trips.

The `trips.txt` table connects routes with service schedules.

Typical columns:

- `route_id`
- `service_id`
- `trip_id`
- `trip_headsign`
- `direction_id`

**Purpose:** Identify individual trips operated on specific routes.

### `stop_times/`

```text
data/processed/silver/gtfs_static/stop_times/
```

Contains scheduled arrival and departure times for each stop on a trip.

The `stop_times.txt` table describes the sequence of stops followed by a bus.

Typical columns:

- `trip_id`
- `arrival_time`
- `departure_time`
- `stop_id`
- `stop_sequence`

**Purpose:** Understand the planned movement of buses between stops.

This table will be particularly important when we compare scheduled times with real-time observations.

### `calendar/`

```text
data/processed/silver/gtfs_static/calendar/
```

Contains regular service schedules.

The `calendar.txt` table describes the days on which a particular service operates.

Typical columns:

- `service_id`
- `monday`
- `tuesday`
- `wednesday`
- `thursday`
- `friday`
- `saturday`
- `sunday`
- `start_date`
- `end_date`

**Purpose:** Determine which services are normally active on a particular date.

### `calendar_dates/`

```text
data/processed/silver/gtfs_static/calendar_dates/
```

Contains exceptions to regular service schedules.

The `calendar_dates.txt` table describes additional or removed service dates.

Typical columns:

- `service_id`
- `date`
- `exception_type`

**Purpose:** Handle exceptional service days, such as holidays or special operating schedules.

---

## 5.3 `silver/gtfs_realtime/`

```text
data/processed/silver/gtfs_realtime/
```

Contains cleaned and validated real-time transit information.

Unlike GTFS Static, which describes planned services, GTFS-Realtime provides information about actual or predicted transit operations.

The exact datasets we create here depend on the entities present in our downloaded Parquet files.

### `vehicle_positions/`

```text
data/processed/silver/gtfs_realtime/vehicle_positions/
```

Will contain cleaned vehicle-position records if the source includes vehicle position information.

Potential information:

- Vehicle identifier.
- Trip identifier.
- Current latitude.
- Current longitude.
- Current stop information.
- Observation timestamp.

**Purpose:** Track the geographical position of buses over time.

This information can help us study vehicle movement and identify potential delays.

### `trip_updates/`

```text
data/processed/silver/gtfs_realtime/trip_updates/
```

Will contain cleaned trip-update records if the source includes trip updates.

Potential information:

- Trip identifier.
- Stop identifier.
- Predicted arrival time.
- Predicted departure time.
- Delay information.
- Observation timestamp.

**Purpose:** Track changes between scheduled and predicted transit operations.

This information will be useful for analyzing bus delays.

**Important:** These directories represent planned output categories. We must inspect the actual Parquet schema before deciding which datasets to create. We should not assume that every source file contains both entity types.

---

# 6. Quarantine Directory — Invalid Data

### Location

```text
data/processed/quarantine/
```

### Role

The quarantine directory stores records that fail our data quality or validation rules.

We should not silently delete invalid records.

Instead, we preserve them separately and record why they were rejected.

This improves data traceability and makes debugging easier.

### 6.1 `invalid_stops/`

```text
data/processed/quarantine/invalid_stops/
```

Stores invalid stop records.

Possible problems:

- Missing `stop_id`.
- Invalid latitude.
- Invalid longitude.
- Missing required information.

### 6.2 `invalid_trips/`

```text
data/processed/quarantine/invalid_trips/
```

Stores invalid trip records.

Possible problems:

- Missing `trip_id`.
- Missing `route_id`.
- Unknown `route_id`.
- Invalid service reference.

### 6.3 `invalid_stop_times/`

```text
data/processed/quarantine/invalid_stop_times/
```

Stores invalid stop-time records.

Possible problems:

- Missing `trip_id`.
- Unknown `stop_id`.
- Invalid `stop_sequence`.
- Invalid time format.

Note: GTFS scheduled times can exceed 24 hours, such as `25:15:00`. This is valid GTFS data and must not automatically be rejected.

### 6.4 `invalid_realtime/`

```text
data/processed/quarantine/invalid_realtime/
```

Stores invalid real-time records.

Possible problems:

- Missing required identifiers.
- Invalid coordinates.
- Invalid timestamps.
- Invalid or inconsistent trip references.

Each quarantined dataset should ideally include a rejection reason, such as:

```text
record_id
source_file
rejection_reason
processing_timestamp
```

The exact schema will depend on the type of invalid record.

---

# 7. How Data Moves Through the Pipeline

The following diagram summarizes the role of each directory.

```text
          EXTERNAL DATA SOURCES
                    |
                    v
          +-------------------+
          |    data/raw/      |
          |                   |
          | Original ZIP      |
          | Parquet files     |
          | manifest.jsonl    |
          +-------------------+
                    |
                    | Extraction
                    v
          +-------------------+
          |  data/staging/    |
          |                   |
          | Extracted GTFS    |
          | Static files      |
          +-------------------+
                    |
                    | Inspection
                    | Cleaning
                    | Transformation
                    | Validation
                    |
             +------+------+
             |             |
             v             v
    +----------------+  +----------------+
    | Silver Layer   |  |   Quarantine   |
    |                |  |                |
    | Valid records  |  | Invalid records|
    +----------------+  +----------------+
             |
             v
    +----------------+
    | Future Phases  |
    |                |
    | PostgreSQL     |
    | Kafka          |
    | Spark          |
    | Airflow        |
    | Grafana        |
    +----------------+
```

---

# 8. Important Data Engineering Principles

During implementation, we should follow these principles:

| Principle | Explanation |
|---|---|
| Data preservation | Never modify original Bronze files. |
| Traceability | Keep information about the source of each dataset. |
| Data quality | Validate data before publishing it into Silver. |
| Quarantine | Preserve invalid records with rejection reasons. |
| Idempotency | Running the transformation pipeline multiple times should not create duplicated outputs. |
| Consistency | Use consistent column names, data types, and formats. |
| Reproducibility | Silver datasets should be regenerable from Bronze data. |
| Validation | Verify outputs before considering a transformation successful. |

## 9. Expected Output Formats

We will primarily use the following formats:

| Layer | Data format | Example |
|---|---|---|
| Bronze | ZIP | `google_transit_manhattan.zip` |
| Bronze | Parquet | Original GTFS-Realtime Parquet |
| Staging | TXT / CSV | `stops.txt`, `routes.txt` |
| Silver | Parquet | `stops.parquet`, `trips.parquet` |
| Quarantine | Parquet or CSV | Invalid records with rejection reasons |

The exact output filenames and partitioning strategy will be defined during implementation.

## 10. Final Objective

At the end of Phase 3, we should have:

1. Original data safely preserved in Bronze.
2. GTFS Static archives extracted into Staging.
3. A clear understanding of the source schemas.
4. Cleaned and standardized GTFS Static datasets.
5. Cleaned GTFS-Realtime datasets, according to the actual source schema.
6. Invalid records stored separately in Quarantine.
7. Validated Silver datasets stored in Parquet format.
8. Tests demonstrating that extraction, transformation, and validation work correctly.

**The main objective is to establish a reliable Bronze → Staging → Silver data pipeline that can be reused in the next phases of Bus Observatory.**




## inspection module 

Our objective is to answer questions such as:

    What columns does each dataset contain?
    What does each column represent?
    What are the data types?
    How many records does each table contain?
    Are there missing values?
    Are there duplicate records?
    Are there invalid coordinates?
    Are the relationships between tables valid?
    Are there inconsistencies between the five boroughs?

We will not modify the data during this phase. Inspection is read-only