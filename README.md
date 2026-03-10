# Measurement Database Toolkit

A compact workflow for turning raw optical/electrical measurement folders into an indexed SQLite database with reproducible analytics. It contains:

- **`schema.sql`** – normalized tables for DUTs, measurements, sessions, raw files, instrument settings, analyses, and metrics.
- **`database_api.py`** – a high-level API that handles imports, queries, feature extraction, and lifecycle utilities (backup, vacuum, schema evolution).
- **`analysis.py`** – instrument-specific parsers plus core spectrum/OMA/tuning routines.
- **`main.py`** – an end-to-end example that backs up the DB, ingests a folder, runs every analysis type per session, and emits optional exports.

> The codebase targets Windows paths but works cross-platform as long as Python can reach your data directories.

---

## Highlights

- **Idempotent ingestion**: filename parsing enforces unique DUT+Measurement+Session keys; rerunning an import only updates changed metadata.
- **Rich metadata**: optical channels, launch power, sweep ranges, SMU setpoints, and arbitrary key/value pairs stay attached to each `RawDataFile`.
- **Pluggable analytics**: `MRM_SPCM`, `MRM_OMA`, `MRM_tuning`, and `MRM_SSRF` helper methods automatically create `Analyses → Features → FeatureMetrics` rows and wire source files through `AnalysisSources`.
- **Modern query helpers**: filter DUTs, measurements, sessions, or raw files by wafer/DOE/die, time ranges, session indices, data types, or instrument settings.
- **Safety utilities**: one-line database backups, WAL cleanup via `vacuum()`, schema migrations with `add_column()`, plus Excel exports for quick sharing.

---

## Requirements & Setup

| Requirement | Notes |
|-------------|-------|
| Python 3.9+ | tested on Windows 11 / CPython 3.11 |
| Packages | `numpy`, `scipy`, `matplotlib`, `pandas`, `openpyxl`, `tqdm` |

```bash
# optional virtual environment
python -m venv .venv
.venv\Scripts\activate

pip install numpy scipy matplotlib pandas openpyxl tqdm
```

---

## Quick Start

1. **Create / reset the database**
   ```python
   from database_api import DatabaseAPI

   with DatabaseAPI("measurement_data.db") as db:
       db.reset_db("schema.sql")  # or db.create_db(...)
   ```

2. **Import a measurement folder**
   ```python
   source = r"D:\processing\測量資料\260310_RF_test1"

   with DatabaseAPI("measurement_data.db") as db:
       db.backup_database()
       db.import_from_measurement_folder(source, schema_file="schema.sql")
   ```

3. **Run bundled analyses per session** (see `main.py` for a full batch loop)
   ```python
   cage = "Cage31"
   measure_name = "260310_RF_test1"

   with DatabaseAPI("measurement_data.db") as db:
       sessions = db.select_session(cage=cage, measure_name=measure_name)
       for session in sessions:
           session_id = session["session_id"]
           db.MRM_SPCM_analysis_by_session(session_id, input_channel="1", output_channel="2", commit=False)
           db.MRM_OMA_analysis_by_session(session_id, start=1305, end=1315, commit=False)
           db.MRM_tuning_analysis_by_session(session_id, start=1305, end=1315, commit=False)
           db.MRM_SSRF_analysis_by_session(session_id, commit=False)
       db.conn.commit()
   ```

4. **Export results**
   ```python
   with DatabaseAPI("measurement_data.db") as db:
       db.export_all_tables_to_xlsx("database_export.xlsx")
   ```

---

## Database Cheat Sheet

| Table | Purpose |
|-------|---------|
| `DUT` | Unique wafer/DOE/die/cage/device combinations. |
| `Measurement` | Logical measurement runs tied to a DUT; holds `measure_name`, operator, timestamp. |
| `MeasureSession` | Repeat counter (`session_idx`) under a measurement; referenced by raw data & analyses. |
| `RawDataFiles` | Every imported file with type, relative path, record time. |
| `OpticalInfo` / `ElectricInfo` / `AnotherInfo` | Per-file instrument configuration and free-form metadata. |
| `Analyses` | One row per algorithm run (`analysis_type`, `instance_no`, algorithm, version). |
| `AnalysisSources` | Many-to-many bridge between analyses and the raw files used. |
| `Features` / `FeatureMetrics` | Hierarchical storage for extracted observables and unit-tagged values. |

All foreign keys cascade, so deleting a DUT cleans downstream measurements, sessions, data, and analyses.

---

## Query Recipes

```python
from database_api import DatabaseAPI
from datetime import datetime, timedelta

with DatabaseAPI("measurement_data.db") as db:
    # 1. Filter DUTs
    duts = db.select_duts(wafer="W12", cage="Cage31")

    # 2. Measurements by DUT and time window
    week_ago = datetime.now() - timedelta(days=7)
    measurements = db.select_measurements(dut_id=duts[0]["DUT_id"], measured_at_start=week_ago)

    # 3. Session ids with fine-grained filters
    sessions = db.select_session(wafer="W12", measure_name="260310_RF_test1", session_idx=0)

    # 4. Raw files matching instrument settings
    raw = db.select_rawdata_files(session_id=sessions[0]["session_id"],
                                  data_type="SPCM",
                                  optical_input_channel="1",
                                  electric_element="pn")
```

Each helper only applies filters you pass, keeping the queries composable and efficient.

---

## Workflow in `main.py`

1. Configure `db_path`, `folder`, and `folder_path`.
2. Run `python main.py`.
3. The script will:
   - Backup the database (optional but recommended).
   - Import the pointed folder (comment the import block if you just want analytics).
   - Iterate `select_session(...)` for the requested cage/measure pair.
   - Execute all four bundled analyses per session, wiring results into the database.
   - (Optional) export the final state or perform ad-hoc queries at the bottom of the file.

Feel free to adapt the loop (e.g., only run a subset of analyses, tweak channels, or add new algorithms).

---

## Operational Tips

- **Backups before imports**: `db.backup_database()` writes timestamped copies under `backup/` so you can roll back quickly.
- **Long-running imports**: the importer uses `ThreadPoolExecutor` only for file copies; DB writes stay transactional to avoid corruption.
- **Schema changes**: use `add_column()` inside a `with DatabaseAPI(...)` block; it no-ops if the column is already present.
- **Cleanup**: `vacuum()` supports `VACUUM INTO`, WAL checkpoints, and incremental vacuuming for compact SQLite files.
- **Reproducibility**: all insert helpers rely on `ON CONFLICT` clauses, so rerunning scripts simply updates data instead of duplicating it.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `sqlite3.OperationalError: no such table` | Run `db.create_db("schema.sql")` or ensure you are pointing at the latest database file. |
| Foreign-key errors on delete | Confirm `PRAGMA foreign_keys = ON` (set automatically in `DatabaseAPI.connect()`). |
| Import skips files | Check `database_api.MAIN_PATTERN` and adjust the regex if your filenames changed. |
| Missing pandas/openpyxl | Install the packages before calling `export_all_tables_to_xlsx()`. |

Have fun automating your measurement pipeline! If you extend the schema or add new analysis modules, document them here to keep the toolkit discoverable.
