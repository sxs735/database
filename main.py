#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

folder_path = Path(r"C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202")
db_path = Path(r"C:\Users\mg942\Desktop\元澄\資料庫") / "DataBase.db"
#%%
with DatabaseAPI(db_path) as db:
    db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
    #a,b = db.parse_folder(folder_path)
    #c = db.parse_filename(b[0])
#%%
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")

#%%
measure_name="20260202"
cage = "cage1"
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_cage(measure_name = measure_name,cage = cage)
    processing_plan = []
    total_spcm = 0
    for session_id in session_ids:
        spcm_data = db.select_data_ids_paths_by_session(session_id, data_type='SPCM')
        processing_plan.append((session_id, spcm_data))
        total_spcm += len(spcm_data)

    with tqdm(total=total_spcm, desc="Analyzing MRM SPCM", unit="file") as pbar:
        for session_id, spcm_data in processing_plan:
            for instance_no, info in enumerate(spcm_data):
                filepath = info['file_path']
                head, data = read_spectrum(filepath)
                x = data[:, 0]
                y = data[:, 3] - data[:, 1]
                result, algorithm_name, version = MRM_SPCM_analysis(x, y)
                analysis_id = db.insert_analysis(session_id = session_id,
                                                 analysis_type = 'MRM_SPCM_analysis',
                                                 instance_no = instance_no,
                                                 algorithm = algorithm_name,
                                                 version = version,
                                                 commit=False)
                db.insert_sources(analysis_id, info["data_id"], commit=False)
                for i in range(len(result['valley_wavelength'][0])):
                    feature_id = db.insert_feature(analysis_id=analysis_id, feature_type='basic parameters', feature_idx=i)
                    result_idx = {key:(result[key][0][i],result[key][1]) for key in result}
                    db.insert_metrics(feature_id, result_idx, commit=False)
                pbar.update(1)
    db.conn.commit()
    
#%%  
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT measure_name
             FROM Measurement
             WHERE measure_name IS NOT NULL
             ORDER BY measure_name;'''
    a = db.query(cmd)

    # for d in a:
    #     data_id = d['data_id']
    #     info = db.select_electric_info_by_data_id(data_id)
    #     print(info)

    
# %%
with DatabaseAPI(db_path) as db:
    db.conn.rollback()
# %%
import sqlite3

conn = sqlite3.connect(db_path)
print(conn.execute("PRAGMA integrity_check;").fetchone())
# %%
