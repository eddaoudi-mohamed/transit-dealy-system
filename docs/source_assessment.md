# Source Assessment

## Source
- Archive: Bus Observatory (public archive of GTFS-Realtime vehicle data)
- Feed: nyct_mta_bus_gtfsrt (New York City Transit buses, VehiclePositions)
- Sample file: COMPACTED_nyct_mta_bus_gtfsrt_2026-10-16_18_03_57.parquet
- Size: 134.06 MB | Rows: 3,054,703 | Row groups: 1
- Retrieved on: <date> (how: <URL / bucket>)
- License / attribution: <check the Bus Observatory docs>

## What the data contains
One row per bus observation: trip, route, position, heading, timestamp, a
related stop, and an occupancy code. The file covers 24 hours:
2026-10-15 18:00:04 UTC to 2026-10-16 18:01:00 UTC
(14:00 to 14:01 New York time). 5,137 buses, 53,906 trips, 347 routes.

## Quality observations
- Null rates: all columns 0% except occupancy_status (57.64%)
- Exact duplicate rows: 1,267 (0.04%)
- Duplicate (bus, timestamp) pairs: 8,014 (0.26%), more than exact duplicates,
  so some pairs have conflicting values (to investigate)
- Coordinates: lat 40.50-40.93, lon -74.25 to -73.70, all inside NYC area
- `id` equals vehicle id in 100% of rows
- Occupancy (non-null rows): 1=79.9%, 2=17.4%, 3=2.3%, 5=0.4%

## Limitations
- No delay, arrival time, stop status or stop sequence in this feed
- vehicle.stop_id is ambiguous (at the stop vs heading to it)
- Timestamps are UTC; local time is America/New_York
- Static GTFS (stop_times, stops, shapes) must be downloaded separately
- Static GTFS trip_id match: not yet tested
- Occupancy is 58% missing and strongly imbalanced

## Feasibility: delay prediction
Verdict: PARTIAL (not directly supported)
Delay would have to be estimated by comparing position pings with the
static schedule (approximate label, about ±30s). It depends on the static
GTFS trip_id match and on having several days of data.
Alternative ML target: occupancy (crowded vs. not crowded).