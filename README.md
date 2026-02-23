# Measurement Database Toolkit

This repository contains a lightweight SQLite schema plus helper scripts for ingesting optical measurement files, running spectrum analyses, and exporting the results for downstream work.

## Project Layout

| Path | Description |
|------|-------------|
| `schema.sql` | DDL for DUT / measurement / analysis tables including the `AnalysisSources` bridge. |
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
         db.create_db("schema.sql")
   ```

2. **Import measurement folders** (see `main.py` for a full loop):
   ```python
      with DatabaseAPI("Measurement.db") as db:
         db.import_session_folder(r"C:\data\PIC9_batch", schema_file="schema.sql")
   ```

3. **Run analyses**
   `main.py` demonstrates how to iterate through unprocessed measurements, level spectra, locate valleys, compute FSR/FWHM/Q, and store results in `Analyses`, `Features`, and `FeatureMetrics` while linking the raw data via `AnalysisSources`.

4. **Export**
   ```python
   with DatabaseAPI("Measurement.db") as db:
       db.export_all_tables_to_xlsx("database_export.xlsx")
   ```

## Key Database Concepts

- **Measurement vs. MeasureSession**: `Measurement` captures DUT-level metadata, while `MeasureSession` indexes each repeat (`session_idx`) under a measurement and is referenced by data files and analyses.
- **Analyses**: each run is scoped by `(session_id, analysis_type, instance_no)` where `session_id` points to a `MeasureSession` row.
- **AnalysisSources**: many-to-many bridge recording which `RawDataFiles` entries fed a given analysis run.
- **Features / FeatureMetrics**: hierarchical storage for peaks, valleys, or other observables with arbitrary key/value units.

Refer to `schema.sql` for all table definitions, indexes, and foreign-key cascades.

## Using `database_api.DatabaseAPI`

The API wraps common insert/query/delete patterns.

```python
with DatabaseAPI("Measurement.db") as db:
   dut_id = db.insert_dut(wafer="W001", doe="DOE_A", die=5, cage="C1", device="DEV01", client="Demo")
   measure_id = db.insert_session(dut_id, session_name="SPCM_20260205")
   data_id = db.insert_rawdata_file(measure_id, session_idx=0, data_type="SPCM", file_path=".../file.csv")
   db.insert_conditions(measure_id, {"temperature": (25, "°C")})
   db.insert_analysis(measure_id,
                      session_idx=0,
                      analysis_type="basic_spectrum_analysis",
                      instance_no=0,
                      algorithm="valley_scan",
                      version="1.0.0")
```

Query helpers include `get_session_details()`, `list_analysis_inputs()`, and `search_metrics()`.

To evolve the schema safely, leverage `add_column()` which skips work if the column already exists:

```python
with DatabaseAPI("Measurement.db") as db:
   db.add_column("Analyses", "version", "TEXT NOT NULL DEFAULT '1.0.0'")
```

## Running the Example Workflow

1. Update the `folder_path` and `db_path` constants at the top of `main.py`.
2. Run the script:
   ```bash
   python main.py
   ```
3. The script will:
   - Import new measurement folders (comment/uncomment as needed).
   - Process measurements lacking `Analyses` and persist computed metrics.
   - Optionally export the current SQLite contents to Excel.

## Notes

- All inserts use `ON CONFLICT` clauses to keep reruns idempotent.
- Foreign keys are enforced via `PRAGMA foreign_keys = ON`, so deleting a parent row automatically cascades.
- The spectral reader in `analysis.py` is tailored to a specific CSV format; adapt as necessary when onboarding new instruments.

For further usage patterns explore `example_usage.py`, which includes CRUD, analytics, and export demonstrations.
