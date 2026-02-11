# Measurement Database Toolkit

This repository contains a lightweight SQLite schema plus helper scripts for ingesting optical measurement files, running spectrum analyses, and exporting the results for downstream work.

## Project Layout

| Path | Description |
|------|-------------|
| `schema.sql` | DDL for DUT / measurement / analysis tables including the `AnalysisInputs` bridge. |
| `database_api.py` | High-level API for creating the database, importing folders, querying metadata, and writing analytics. |
| `analysis.py` | CSV reader utilities (currently tailored for SPCM exports). |
| `main.py` | Example workflow that imports folders, runs a baseline spectral analysis, and exports to Excel. |
| `example_usage.py` | Standalone demos showing how to call each API method. |

## Requirements

- Python 3.9+
- Packages: `numpy`, `scipy`, `matplotlib`, `tqdm`, `pandas`, `openpyxl`

Install dependencies with:

```bash
pip install -r requirements.txt  # or install the packages listed above
```

## Quick Start

1. **Create the database**
   ```python
   from database_api import DatabaseAPI

   with DatabaseAPI("Measurement.db") as db:
       db.create_database("schema.sql")
   ```

2. **Import measurement folders** (see `main.py` for a full loop):
   ```python
   with DatabaseAPI("Measurement.db") as db:
       db.import_measurement_folder(r"C:\data\PIC9_batch", schema_file="schema.sql")
   ```

3. **Run analyses**
   `main.py` demonstrates how to iterate through unprocessed sessions, level spectra, locate valleys, compute FSR/FWHM/Q, and store results in `AnalysisRuns`, `AnalysisFeatures`, and `FeatureValues` while linking the raw data via `AnalysisInputs`.

4. **Export**
   ```python
   with DatabaseAPI("Measurement.db") as db:
       db.export_all_tables_to_xlsx("database_export.xlsx")
   ```

## Key Database Concepts

- **AnalysisRuns**: each run is scoped by `(session_id, analysis_type, analysis_index)` for multi-pass workflows.
- **AnalysisInputs**: many-to-many bridge recording which `MeasurementData` entries fed a given analysis run.
- **AnalysisFeatures / FeatureValues**: hierarchical storage for peaks, valleys, or other observables with arbitrary key/value units.

Refer to `schema.sql` for all table definitions, indexes, and foreign-key cascades.

## Using `database_api.DatabaseAPI`

The API wraps common insert/query/delete patterns.

```python
with DatabaseAPI("Measurement.db") as db:
    dut_id = db.insert_dut("W001", "DOE_A", 5, "C1", "DEV01")
    session_id = db.insert_measurement_session(dut_id, session_name="SPCM_20260205")
    data_id = db.insert_measurement_data(session_id, "SPCM", ".../file.csv")
    db.insert_experimental_conditions(session_id, {"temperature": (25, "°C")})
   db.insert_analysis_run(session_id,
                     "basic_spectrum_analysis",
                     analysis_index=0,
                     algorithm="valley_scan",
                     version="1.0.0")
```

Query helpers include `get_session_full_info()`, `get_analysis_input_data()`, and `search_features_by_value()`.

To evolve the schema safely, leverage `add_column()` which skips work if the column already exists:

```python
with DatabaseAPI("Measurement.db") as db:
   db.add_column("AnalysisRuns", "version", "TEXT NOT NULL DEFAULT '1.0.0'")
```

## Running the Example Workflow

1. Update the `folder_path` and `db_path` constants at the top of `main.py`.
2. Run the script:
   ```bash
   python main.py
   ```
3. The script will:
   - Import new measurement folders (comment/uncomment as needed).
   - Process sessions lacking `AnalysisRuns` and persist computed metrics.
   - Optionally export the current SQLite contents to Excel.

## Notes

- All inserts use `ON CONFLICT` clauses to keep reruns idempotent.
- Foreign keys are enforced via `PRAGMA foreign_keys = ON`, so deleting a parent row automatically cascades.
- The spectral reader in `analysis.py` is tailored to a specific CSV format; adapt as necessary when onboarding new instruments.

For further usage patterns explore `example_usage.py`, which includes CRUD, analytics, and export demonstrations.
