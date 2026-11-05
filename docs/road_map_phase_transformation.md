# Phase 3 — Data Transformation: Bronze → Silver

## 1. Overview

### 1.1 Objective

The objective of this phase is to transform the raw transportation data collected during Phase 2 (Data Ingestion) into clean, standardized, validated, and reusable datasets.

This phase follows the **Medallion Architecture**, focusing on the transition from the Bronze layer to the Silver layer.

The transformation pipeline must be:

- Reproducible.
- Testable.
- Idempotent.
- Traceable.
- Maintainable.
- Configurable.

### 1.2 Project context

The Bus Observatory project uses two main data sources:

| Source | Format | Description |
|---|---|---|
| GTFS Static | ZIP / TXT | Routes, stops, trips, schedules, and geographic information. |
| GTFS-Realtime | Parquet | Historical real-time transportation observations. |

The exact contents and schemas of the GTFS-Realtime files must be inspected before defining transformations.

### 1.3 Expected outcome

At the end of this phase, we should have:

- Extracted GTFS Static archives.
- Inspected and documented source schemas.
- Defined data cleaning and standardization rules.
- Transformed GTFS Static datasets.
- Transformed GTFS-Realtime datasets.
- Validated data quality.
- Quarantined invalid records.
- Stored cleaned datasets in Parquet format.
- Created automated transformation tests.

---

# 2. Medallion Architecture

## 2.1 Bronze Layer — Raw Data

The Bronze layer contains the original data downloaded from external sources.

```text
data/
└── raw/
    ├── gtfs_static/
    │   ├── google_transit_bronx.zip
    │   ├── google_transit_brooklyn.zip
    │   ├── google_transit_manhattan.zip
    │   ├── google_transit_queens.zip
    │   └── google_transit_staten_island.zip
    │
    ├── gtfs_realtime/
    │   ├── realtime_2025-01-23.parquet
    │   └── realtime_2025-01-24.parquet
    │
    └── manifest.jsonl
```

### Bronze principles

- Preserve original downloaded files.
- Never modify raw data.
- Never overwrite source files with transformed data.
- Maintain ingestion metadata.
- Preserve source URLs and download information.
- Ensure that every dataset can be traced back to its source.

The Bronze layer is considered immutable.

## 2.2 Staging Layer — Intermediate Data

The staging layer contains extracted and intermediate data used during transformation.

```text
data/
└── staging/
    └── gtfs_static/
        ├── bronx/
        ├── brooklyn/
        ├── manhattan/
        ├── queens/
        └── staten_island/
```

The staging layer is not the final destination for cleaned data.

Its purpose is to provide a workspace for extraction and inspection.

## 2.3 Silver Layer — Cleaned Data

The Silver layer contains standardized and validated datasets.

```text
data/
└── processed/
    └── silver/
        ├── gtfs_static/
        │   ├── agency/
        │   ├── routes/
        │   ├── stops/
        │   ├── trips/
        │   ├── stop_times/
        │   ├── calendar/
        │   └── calendar_dates/
        │
        └── gtfs_realtime/
            ├── vehicle_positions/
            └── trip_updates/
```

Only create datasets that actually exist in the source.

### Silver principles

- Standardized column names.
- Consistent data types.
- Explicit missing-value handling.
- Duplicate detection.
- Valid timestamps.
- Valid geographic coordinates.
- Referential integrity.
- Documented schemas.
- Preserved data lineage.

---

# 3. Transformation Architecture

The transformation process follows this workflow:

```text
                 EXTERNAL DATA SOURCES
                           |
              +------------+------------+
              |                         |
          GTFS Static              GTFS-Realtime
            ZIP/TXT                  Parquet
              |                         |
              +------------+------------+
                           |
                           v
                     BRONZE LAYER
                       Raw Data
                           |
                           v
                    EXTRACTION / STAGING
                           |
                           v
                    SOURCE INSPECTION
                           |
                           v
                   DATA TRANSFORMATION
                           |
              +------------+------------+
              |                         |
         Data Cleaning            Standardization
              |                         |
              +------------+------------+
                           |
                           v
                    DATA VALIDATION
                           |
              +------------+------------+
              |                         |
          Valid Records           Invalid Records
              |                         |
              v                         v
         SILVER LAYER                QUARANTINE
              |
              v
        Cleaned Parquet
```

---

# 4. Complete Phase 3 Roadmap

The phase is divided into ten sub-phases.

## Sub-phase 3.1 — Bronze Data Verification

### Objective

Verify that all raw data required for transformation exists and is accessible.

### Tasks

1. List all files inside `data/raw/`.
2. Verify that all GTFS Static ZIP archives exist.
3. Verify ZIP file integrity.
4. Verify the availability of GTFS-Realtime Parquet files.
5. Check Parquet file readability.
6. Inspect the ingestion manifest.
7. Identify missing or corrupted files.
8. Record the source inventory.

### Important considerations

- Do not assume that a successful download guarantees a valid file.
- Do not start transformation if required files are missing.
- Distinguish between missing data and corrupted data.
- Keep the original source files unchanged.

### Deliverables

- Raw data inventory.
- ZIP integrity verification.
- Parquet readability verification.
- List of missing or corrupted files.

---

## Sub-phase 3.2 — GTFS Static Extraction

### Objective

Extract GTFS Static ZIP archives safely into the staging directory.

### Tasks

1. Implement a ZIP extraction module.
2. Create a separate directory for each borough.
3. Verify ZIP archive integrity before extraction.
4. Prevent unsafe archive paths (Zip Slip).
5. Extract available `.txt` files.
6. Verify the presence of expected GTFS tables.
7. Record extraction results.

### Expected files

| File | Description |
|---|---|
| `agency.txt` | Transport agency information |
| `routes.txt` | Bus route information |
| `stops.txt` | Bus stop information |
| `trips.txt` | Scheduled trips |
| `stop_times.txt` | Scheduled stop arrival and departure times |
| `calendar.txt` | Regular service schedules |
| `calendar_dates.txt` | Service exceptions |
| `shapes.txt` | Geographic route shapes |

The actual files depend on the source feed.

### Expected output

```text
data/
└── staging/
    └── gtfs_static/
        ├── bronx/
        ├── brooklyn/
        ├── manhattan/
        ├── queens/
        └── staten_island/
```

### Tests

- Test successful extraction.
- Test invalid ZIP archives.
- Test empty archives.
- Test missing expected files.
- Test unsafe archive paths.
- Test repeated extraction.

### Deliverables

- `extract_gtfs.py`
- Extraction tests.
- Extracted GTFS Static files.

---

## Sub-phase 3.3 — Source Data Inspection

### Objective

Understand the structure and characteristics of the source datasets before modifying them.

### A. GTFS Static inspection

For every available table, inspect:

- Number of rows.
- Number of columns.
- Column names.
- Data types.
- Missing values.
- Duplicate records.
- Unique identifiers.
- Minimum and maximum values.
- Relationships with other tables.

Important GTFS tables:

| Table | Primary identifier | Important relationships |
|---|---|---|
| agency | `agency_id` | Agency information |
| routes | `route_id` | `agency_id` |
| stops | `stop_id` | Stop information |
| trips | `trip_id` | `route_id`, `service_id` |
| stop_times | `trip_id`, `stop_sequence` | `trip_id`, `stop_id` |
| calendar | `service_id` | Service schedules |
| calendar_dates | `service_id`, `date` | Service exceptions |
| shapes | `shape_id`, `shape_pt_sequence` | Geographic shapes |

The actual primary keys and relationships must be verified against the applicable GTFS specification and source feed.

### B. GTFS-Realtime inspection

For each Parquet file, inspect:

- Schema.
- Column names.
- Data types.
- Number of rows.
- Number of columns.
- Missing values.
- Duplicate records.
- Timestamp ranges.
- Available transportation entities.

Determine whether the source contains:

- Vehicle positions.
- Trip updates.
- Service alerts.
- Other transportation observations.

Do not assume that every entity is present.

### Deliverables

- Static source inspection report.
- Real-time source inspection report.
- Initial schema documentation.

---

## Sub-phase 3.4 — Data Dictionary and Schema Definition

### Objective

Document the source schemas and define the expected Silver schemas.

### Tasks

1. Document each source table.
2. Describe every column.
3. Identify identifiers and relationships.
4. Document source data types.
5. Define target Silver data types.
6. Record column mappings.
7. Document transformation decisions.

### Example

| Source column | Silver column | Target type | Description |
|---|---|---|---|
| `stop_id` | `stop_id` | string | Unique stop identifier |
| `stop_name` | `stop_name` | string | Stop name |
| `stop_lat` | `stop_lat` | float64 | Stop latitude |
| `stop_lon` | `stop_lon` | float64 | Stop longitude |

### Deliverable

```text
docs/
└── data_dictionary.md
```

The data dictionary must describe the actual source data, not an assumed schema.

---

## Sub-phase 3.5 — Define Data Cleaning Rules

### Objective

Define explicit rules for handling missing, invalid, and duplicated records.

Cleaning decisions must be documented before implementation.

### A. Missing values

Investigate missing values by column.

Examples:

| Column | Treatment |
|---|---|
| `stop_id` | Invalid if missing |
| `stop_name` | Flag missing value |
| `stop_lat` | Validate and quarantine if required |
| `stop_lon` | Validate and quarantine if required |
| `vehicle_id` | May be retained with a missing-value flag |

Do not automatically delete every row containing a null value.

### B. Duplicate records

Define a business key for each dataset.

Possible examples:

- Static stops: `stop_id`, subject to feed scope.
- Static trips: `trip_id`, subject to feed scope.
- Stop times: `trip_id + stop_sequence`.
- Real-time observations: candidate key based on available vehicle and timestamp fields.

Do not assume that `(vehicle_id, timestamp)` is always unique.

### C. Geographic validation

Global coordinate rules:

```python
-90 <= latitude <= 90
-180 <= longitude <= 180
```

Additional geographic checks can be implemented for NYC after defining the intended service area.

### D. Timestamp validation

Check:

- Missing timestamps.
- Invalid timestamp formats.
- Unexpected time ranges.
- Timezone interpretation.
- Event time versus ingestion time.

Special consideration:

GTFS scheduled times may exceed `24:00:00`.

For example:

```text
25:15:00
```

This represents 1:15 AM on the following service day.

Do not parse GTFS scheduled times as ordinary clock times without handling this rule.

### E. Referential integrity

Check relationships between:

- `routes` and `trips`.
- `trips` and `stop_times`.
- `stops` and `stop_times`.
- `calendar` and `trips`.

Do not invent identifiers to resolve missing relationships.

### Deliverable

```text
docs/
└── transformation_rules.md
```

---

## Sub-phase 3.6 — GTFS Static Transformation

### Objective

Transform extracted GTFS Static tables into standardized Silver datasets.

### Tasks

1. Read each `.txt` file.
2. Preserve identifiers as strings.
3. Standardize column names.
4. Normalize missing values.
5. Convert numeric columns to appropriate types.
6. Validate geographic coordinates.
7. Validate scheduled times.
8. Detect duplicate records.
9. Validate table relationships.
10. Add source metadata.
11. Save validated datasets in Parquet format.

### Important considerations

- Do not merge all tables into one DataFrame.
- Do not remove identifiers with leading zeros.
- Preserve the source feed or borough information.
- Handle GTFS time values above 24 hours.
- Preserve the meaning of the original fields.

### Expected output

```text
data/
└── processed/
    └── silver/
        └── gtfs_static/
            ├── agency/
            ├── routes/
            ├── stops/
            ├── trips/
            ├── stop_times/
            ├── calendar/
            └── calendar_dates/
```

Only include tables available in the source.

### Deliverables

- Static transformation modules.
- Cleaned GTFS Static Parquet datasets.
- Transformation tests.

---

## Sub-phase 3.7 — GTFS-Realtime Transformation

### Objective

Transform historical GTFS-Realtime observations into clean and standardized datasets.

### Tasks

1. Read Parquet files using PyArrow or Pandas.
2. Inspect the actual source schema.
3. Identify the available transportation entities.
4. Standardize column names.
5. Normalize data types.
6. Interpret timestamps correctly.
7. Detect missing identifiers.
8. Validate geographic coordinates.
9. Identify duplicate observations.
10. Preserve event and ingestion timestamps where available.
11. Add source metadata.
12. Write validated observations to Parquet.

### Important considerations

- Do not assume the source contains vehicle positions only.
- Do not invent columns that are absent from the source.
- Do not assume all missing vehicle IDs make records unusable.
- Do not confuse event timestamps with download timestamps.
- Preserve the original source file reference.
- Document any schema differences between files.

### Expected output

```text
data/
└── processed/
    └── silver/
        └── gtfs_realtime/
            ├── vehicle_positions/
            └── trip_updates/
```

Only create the entity datasets that are actually available.

### Deliverables

- Real-time transformation modules.
- Cleaned real-time Parquet datasets.
- Transformation tests.

---

## Sub-phase 3.8 — Data Quality Validation

### Objective

Verify that transformed datasets satisfy the defined quality rules.

### A. Completeness

Examples:

- Required columns exist.
- Required identifiers are not null.
- Important timestamps are available.

### B. Validity

Examples:

- Coordinates are within valid ranges.
- Numeric fields have valid values.
- Timestamps can be interpreted correctly.

### C. Uniqueness

Examples:

- Static identifiers follow the expected uniqueness rules.
- Stop-time business keys are unique.
- Real-time duplicate rules are respected.

### D. Consistency

Examples:

- Every referenced trip exists in the corresponding static feed.
- Every referenced route exists.
- Stop-time references resolve to known stops where expected.

### E. Quarantine invalid records

Invalid records should not silently disappear.

Expected structure:

```text
data/
└── processed/
    └── quarantine/
        ├── invalid_stops/
        ├── invalid_trips/
        ├── invalid_stop_times/
        └── invalid_realtime/
```

Each rejected record should have a rejection reason.

### Deliverables

- Validation modules.
- Validation reports.
- Quarantined records.
- Automated validation tests.

---

## Sub-phase 3.9 — Silver Storage

### Objective

Store validated datasets in Parquet format.

### Tasks

1. Define consistent Silver schemas.
2. Write validated DataFrames to Parquet.
3. Use PyArrow as the Parquet engine.
4. Validate the written files.
5. Record row counts.
6. Record processing timestamps.
7. Preserve source lineage.
8. Avoid overwriting valid outputs with incomplete results.

### Example

```python
df.to_parquet(
    output_path,
    engine="pyarrow",
    index=False
)
```

### Storage considerations

- Use consistent data types.
- Avoid unnecessary small Parquet files.
- Partition by date only when justified by data volume and query patterns.
- Write to temporary locations before publishing.
- Make reruns idempotent.
- Keep Silver separate from Bronze.

### Expected output

```text
data/
└── processed/
    └── silver/
        ├── gtfs_static/
        └── gtfs_realtime/
```

### Deliverables

- Parquet writer module.
- Silver datasets.
- Storage verification tests.

---

## Sub-phase 3.10 — Integration, Testing and Documentation

### Objective

Connect the independent transformation modules and verify the complete Bronze-to-Silver workflow.

### Tasks

1. Implement the transformation pipeline entry point.
2. Connect extraction, inspection, cleaning, and validation.
3. Execute transformations in the correct order.
4. Verify input and output paths.
5. Test error handling.
6. Test repeated executions.
7. Verify data lineage.
8. Generate execution logs.
9. Document the architecture.
10. Record completed and pending tasks.

### Expected pipeline

```text
Bronze
   |
   v
Extraction
   |
   v
Inspection
   |
   v
Cleaning
   |
   v
Standardization
   |
   v
Validation
   |
   +-------> Invalid Records
   |              |
   |              v
   |          Quarantine
   |
   v
Silver Parquet
   |
   v
Verification
```

### Deliverables

- Transformation pipeline.
- Automated tests.
- Execution logs.
- Updated documentation.

---

# 5. Recommended Project Structure

The following is the proposed structure after implementing Phase 3.

```text
transit-delay-system/
│
├── config/
│   └── settings.yaml
│
├── data/
│   ├── raw/
│   │   ├── gtfs_static/
│   │   ├── gtfs_realtime/
│   │   └── manifest.jsonl
│   │
│   ├── staging/
│   │   └── gtfs_static/
│   │
│   └── processed/
│       ├── silver/
│       │   ├── gtfs_static/
│       │   └── gtfs_realtime/
│       │
│       └── quarantine/
│
├── src/
│   └── transit_pipeline/
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── download_files.py
│       │   └── manifest.py
│       │
│       └── transformation/
│           ├── __init__.py
│           ├── extract/
│           │   ├── __init__.py
│           │   └── extract_gtfs.py
│           │
│           ├── inspection/
│           │   ├── __init__.py
│           │   ├── inspect_static.py
│           │   └── inspect_realtime.py
│           │
│           ├── cleaning/
│           │   ├── __init__.py
│           │   ├── clean_static.py
│           │   └── clean_realtime.py
│           │
│           ├── validation/
│           │   ├── __init__.py
│           │   ├── validate_static.py
│           │   └── validate_realtime.py
│           │
│           ├── storage/
│           │   ├── __init__.py
│           │   └── parquet_writer.py
│           │
│           └── pipeline.py
│
├── tests/
│   ├── test_ingestion.py
│   └── transformation/
│       ├── test_extract.py
│       ├── test_inspection.py
│       ├── test_clean_static.py
│       ├── test_clean_realtime.py
│       ├── test_validation.py
│       └── test_pipeline.py
│
├── docs/
│   ├── phase_02_ingestion.md
│   ├── phase_03_transformation.md
│   ├── data_dictionary.md
│   └── transformation_rules.md
│
├── Dockerfile
├── compose.yaml
├── requirements.txt
└── README.md
```

This structure is a target, not a requirement to create every file immediately. Introduce modules as their corresponding sub-phase begins.

---

# 6. Technologies

| Technology | Responsibility | When to use |
|---|---|---|
| Python | Transformation logic | Throughout Phase 3 |
| Pandas | Tabular data manipulation | Static and manageable datasets |
| PyArrow | Parquet reading and writing | Throughout Phase 3 |
| Pytest | Unit and integration tests | Throughout Phase 3 |
| Great Expectations / Soda | Advanced data quality checks | Optional extension |
| PySpark | Distributed processing | Later project phase |

### Important decision

We will initially use Python, Pandas, and PyArrow.

We will not introduce Spark just to make the project look more complex. First, we will understand the data, define the rules, and establish a correct transformation pipeline. Later, we can migrate suitable transformations to PySpark.

---

# 7. What We Must NOT Do

| Mistake | Why it is problematic | Recommended practice |
|---|---|---|
| Modify raw files | Destroys source traceability | Preserve Bronze |
| Delete all null records | Can remove useful observations | Apply column-specific rules |
| Convert every ID to integer | Can remove leading zeros | Preserve identifiers as strings |
| Assume timestamps are UTC | Can create incorrect event times | Document timezone semantics |
| Merge every table immediately | Can create incorrect relationships | Transform tables independently |
| Assume identical schemas | Feeds and files may differ | Inspect each source |
| Drop duplicates blindly | May remove legitimate observations | Define business keys |
| Ignore invalid records | Hides data-quality issues | Use quarantine |
| Overwrite Silver directly | Failed jobs can corrupt output | Validate before publishing |
| Hardcode file paths | Makes code difficult to maintain | Use configuration |
| Introduce Spark too early | Adds unnecessary complexity | Start with Pandas and PyArrow |

---

# 8. Important GTFS-Specific Considerations

## 8.1 Static and real-time compatibility

The historical real-time data and static schedules must be compatible.

For example:

- GTFS-Realtime: January 24, 2025.
- GTFS Static: A feed downloaded in November 2026.

These datasets may describe different service periods.

Before joining them, investigate:

- `calendar.txt`.
- `calendar_dates.txt`.
- Feed validity periods.
- Trip identifiers.
- Route identifiers.
- Static feed versions.

A missing trip match does not automatically mean that the real-time record is invalid.

## 8.2 Combining five borough feeds

Before concatenating the five borough datasets, investigate:

- Whether identifiers are globally unique.
- Whether stops are shared between feeds.
- Whether routes overlap.
- Whether records are duplicated across feeds.

Preserve a `source_feed` column to identify each record's origin.

## 8.3 Event time versus ingestion time

For real-time data, distinguish between:

| Timestamp | Meaning |
|---|---|
| `event_timestamp` | When the transportation observation happened |
| `ingestion_timestamp` | When the pipeline downloaded or processed it |

Preserve both where available.

## 8.4 Data lineage

Each Silver dataset should retain useful metadata, such as:

- `source_file`
- `source_feed`
- `processing_timestamp`
- `transformation_version`

These fields help reproduce transformations and investigate errors.

---

# 9. Definition of Done — Phase 3

Phase 3 is complete only when the following conditions are satisfied.

## Bronze verification

- [ ] Original source files are preserved.
- [ ] Raw files are inventoried.
- [ ] ZIP files pass integrity checks.
- [ ] Parquet files are readable.
- [ ] Source metadata is available.

## Extraction and inspection

- [ ] GTFS Static archives are safely extracted.
- [ ] Available GTFS tables are identified.
- [ ] GTFS-Realtime schemas are inspected.
- [ ] Source data dictionary is documented.
- [ ] Static and real-time compatibility is investigated.

## Transformation

- [ ] Cleaning rules are documented.
- [ ] Data types are standardized.
- [ ] Identifiers are preserved correctly.
- [ ] Missing values are handled according to explicit rules.
- [ ] Duplicate detection uses defined keys.
- [ ] Invalid timestamps and coordinates are handled.
- [ ] Referential integrity is checked.
- [ ] Invalid records are quarantined where appropriate.

## Silver storage

- [ ] Cleaned GTFS Static datasets are stored in Parquet.
- [ ] Cleaned GTFS-Realtime datasets are stored in Parquet.
- [ ] Silver schemas are documented.
- [ ] Data lineage is preserved.
- [ ] Outputs are validated before publication.
- [ ] Repeated executions are idempotent.

## Testing and documentation

- [ ] Unit tests pass.
- [ ] Integration tests pass.
- [ ] Error handling is tested.
- [ ] Execution logs are generated.
- [ ] Phase 3 documentation is updated.

---

# 10. Execution Plan — Recommended Order

We will follow this sequence without skipping steps.

| Milestone | Task | Deliverable |
|---|---|---|
| M1 | Bronze verification | Raw data inventory |
| M2 | GTFS Static extraction | Staging datasets |
| M3 | Source inspection | Schema reports |
| M4 | Data dictionary | `data_dictionary.md` |
| M5 | Transformation rules | `transformation_rules.md` |
| M6 | Static transformation | Clean static Parquet |
| M7 | Real-time transformation | Clean real-time Parquet |
| M8 | Data quality validation | Validation reports and quarantine |
| M9 | Silver storage | Complete Silver layer |
| M10 | Pipeline integration | Reproducible Bronze-to-Silver pipeline |

---

# 11. Project Management and Progress Tracking

Use the following table to track implementation.

| Sub-phase | Status | Notes |
|---|---|---|
| 3.1 Bronze verification | Not started | Verify raw files and manifest |
| 3.2 GTFS Static extraction | Not started | Safe ZIP extraction |
| 3.3 Source inspection | Not started | Inspect static and real-time schemas |
| 3.4 Data dictionary | Not started | Document source and target schemas |
| 3.5 Cleaning rules | Not started | Define data-quality policies |
| 3.6 Static transformation | Not started | Clean GTFS Static |
| 3.7 Real-time transformation | Not started | Clean historical observations |
| 3.8 Data quality validation | Not started | Validate and quarantine |
| 3.9 Silver storage | Not started | Write Parquet datasets |
| 3.10 Integration and testing | Not started | Build complete pipeline |

Update the status only after verifying the corresponding deliverable.

---

# 12. Final Recommendation — How We Will Work

We will implement Phase 3 incrementally.

For each milestone:

1. Understand the objective.
2. Inspect the existing project structure.
3. Identify the required files and dependencies.
4. Implement the code.
5. Explain the