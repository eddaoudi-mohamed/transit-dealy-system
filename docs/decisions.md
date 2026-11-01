# Decisions

- First feed: NYCT bus VehiclePositions (nyct_mta_bus_gtfsrt)
- First dataset: the one-day file (15-16 Oct 2026); extend to 7 days after Phase 2
- Timezone: stored as UTC; convert to America/New_York for analysis
- Data zones: data/raw = raw, data/processed = silver, data/features = gold
- Duplicates: handled in silver (drop exact duplicates; resolve repeated bus+timestamp)
- Delay prediction: deferred to Phase 12; occupancy is the backup target
- Static GTFS: Manhattan first for the join test, other boroughs later