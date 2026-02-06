#%%
from pathlib import Path
from database_api import DatabaseAPI

folder_path = Path(r"C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202")
db_path = folder_path.parent / "Measurement.db"

with DatabaseAPI(db_path) as db:
    db.import_measurement_folder(folder_path,schema_file="schema.sql")

# %%
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(folder_path.parent / "database_export.xlsx")

# %%
