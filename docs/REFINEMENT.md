# Earthquake Analytics refinement

## Run the working demo

Python 3 is sufficient. From the repository root:

```sh
python -m http.server 8000 --directory demo
```

Open http://localhost:8000. No keys, database, Node installation, or student credits are required for this static demo. Upload `demo/` to any static host. The portfolio version is mounted at `/demos/earthquake/`; the `ak.` link returns to the host root.

The demo includes a Leaflet map, time and magnitude filters, location search, event inspection, filtered statistics, CSV download, and a dated USGS snapshot. A seven-day snapshot supports all displayed time windows. Snapshot filtering uses its generation timestamp, never the current date. Feed failures and delayed feeds are labeled. Unknown magnitudes are never converted to zero. The map may require an external tile connection, while the event table continues to work without tiles. Vendored Leaflet license is in `demo/vendor/LEAFLET_LICENSE`.

## What was corrected

- Historical loader now pulls the historical fetch task's XCom, rather than silently finding no realtime-task records.
- Backfill paginates beyond the USGS 20,000-record response limit using one-based offsets. This is not a consistent snapshot: catalog revisions during pagination can still change page boundaries.
- Snowflake staging inserts use `executemany`; merge input deduplicates by ID and most recent revision, and handles null target revision timestamps.
- The dbt incremental predicate compares revisions per event, avoiding the old global timestamp watermark that missed delayed revisions. All fact columns update together, including magnitude categories and location.
- Removed custom `analytics` schema overrides, which dbt would otherwise append to the target schema as `ANALYTICS_ANALYTICS`.
- Snowflake result columns are normalized to lowercase for the existing Streamlit code. Marker sizing supports negative magnitudes.
- Renamed the USGS tsunami flag to avoid calling it a verified warning. Magnitude labels no longer assume every measurement is on the Richter scale.
- Account-specific connection defaults are now configurable examples. The regional activity heuristic is explicitly unvalidated, and aggregate-panel filter scope is disclosed.

## Warehouse migration and validation still needed

The static demo is runnable independently. Full Airflow / Snowflake / dbt / Streamlit integration has **not** been executed against your account. The automated tests isolate real ingestion functions using fake connections and exercise the incremental SQL predicate with SQLite; they do not certify Snowflake execution or Airflow scheduler integration.

Configure `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_DATABASE` (default `EARTHQUAKE_DB`). Set Airflow connection `snowflake_conn`. Use an appropriately scoped role. The source schema is `RAW`; target schema is `ANALYTICS`.

After reviewing the model changes in a development schema, run `dbt deps`, then `dbt build --full-refresh --profiles-dir ..` from `dbt/earthquake_analytics`. A full refresh is required to rebuild derived values and the renamed tsunami column. This rebuild has not been run here. Preserve existing production tables until migration is validated. The old `ANALYTICS_ANALYTICS` schema, if present, is not automatically deleted.

Remaining architecture limits: batches still pass through XCom (large backfills should move to staged files); ingestion and transformations retain independent schedules; a 24-hour ingestion feed can miss revisions to older events; merge storage retains only the latest version; requirements use broad version ranges and are not a verified environment lock. The regional score and text-derived regions are exploratory analytics, not hazard assessment.

## Regression checks

```sh
python -m unittest discover -s tests -v
node --test tests/demo.test.mjs
```

See `docs/VALIDATION.txt` for saved results. Python checks cover historical task routing, empty batches, pagination, and delayed per-event revisions. JavaScript checks cover malformed records, deduplication, negative/unknown magnitudes, snapshot windows, empty statistics, and CSV escaping. No performance benchmark is claimed.

## 60-second demonstration

1. Open the demo and point out the feed generation timestamp.
2. Select a seven-day window and search California.
3. Raise the magnitude filter and inspect an event's original USGS source.
4. Download the filtered observations.
5. Switch to Saved snapshot: the badge and time-window anchor explicitly change.
6. Explain the difference between the browser demo and the warehouse architecture below it.
