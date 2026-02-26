#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm

folder_list = ['260122_3',
               '260122_4',
               '260122_manual',
               '260123',
               '260127',
               '260127_swr0.5nm',
               '260127_swr1nm',
               '260127_swr10nm',
               '260129',
               '260204_cage158_D4_repeat20',
               '260205_cage158_D4_5die',
               '260205_cage158_D4_die51',
               '260205_mapping',
               '260206_mapping',
               '260212_mapping']
db_path = Path(r"Y:\量測資料\資料庫") / "DataBase.db"
#%%
for folder_i in folder_list[:]:
    print(folder_i)
    folder_path = Path(f"D:\\processing\\資料庫整理\\{folder_i}")
    with DatabaseAPI(db_path) as db:
        db.import_from_measurement_folder(folder_path,schema_file="schema.sql")
        #a,b = db.parse_folder(folder_path)
        #c = db.parse_filename(b[0])
#%% Processing cage: cage158, measure_name: 260127_swr0.5nm
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT d.cage,m.measure_name
             FROM DUT d
             JOIN Measurement m ON d.DUT_id = m.DUT_id
             WHERE d.device <> 'WDM'
             ORDER BY d.cage, m.measure_name;'''
    res = [(res['cage'], res['measure_name']) for res in db.query(cmd)]
#%%
for cage, measure_name in res[5:]:
    print(f"Processing cage: {cage}, measure_name: {measure_name}")
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
                    col = 3 if data.shape[1] == 5 else 2
                    y = data[:, col] - data[:, 1]
                    result, algorithm_name, version = MRM_SPCM_analysis(x, y)
                    analysis_id = db.insert_analysis(session_id = session_id,
                                                    analysis_type = 'MRM_SPCM_analysis',
                                                    instance_no = instance_no,
                                                    algorithm = algorithm_name,
                                                    version = version,
                                                    commit=False)
                    db.insert_sources(analysis_id, info["data_id"], commit=False)
                    for i in range(len(result['Valley_Wavelength'][0])):
                        feature_id = db.insert_feature(analysis_id=analysis_id, feature_type='basic parameters', feature_idx=i)
                        result_idx = {key:(result[key][0][i],result[key][1]) for key in result}
                        db.insert_metrics(feature_id, result_idx, commit=False)
                    pbar.update(1)
        db.conn.commit()
    
#%%  
with DatabaseAPI(db_path) as db:  
    cmd = '''SELECT DISTINCT d.cage,m.measure_name
             FROM DUT d
             JOIN Measurement m ON d.DUT_id = m.DUT_id
             WHERE d.device <> 'WDM'
             ORDER BY d.cage, m.measure_name;'''
    a = db.query(cmd)
    # cursor = db.conn.execute("""
    # SELECT sql FROM sqlite_master
    # WHERE type='table' AND name='FeatureMetrics'
    # """)
    # print(cursor.fetchone()[0])
    

#%%
with DatabaseAPI(db_path) as db:
    output_path = db.export_all_tables_to_xlsx(db_path.parent / "database_export.xlsx")   
# %%
with DatabaseAPI(db_path) as db:
    db.conn.rollback()
# %%
import sqlite3

conn = sqlite3.connect(db_path)
print(conn.execute("PRAGMA integrity_check;").fetchone())
# %%
import matplotlib.pyplot as plt
cage='cage158'
measure_name='260127_swr0.5nm'
with DatabaseAPI(db_path) as db:
    session_ids = db.select_session_ids_by_measure_name_and_cage(measure_name = measure_name,cage = cage)
    spcm_data = db.select_data_ids_paths_by_session(session_ids[0], data_type='SPCM')
    filepath = spcm_data[0]['file_path']
    head, data = read_spectrum(filepath)
    x = data[:, 0]
    col = 3 if data.shape[1] == 5 else 2
    y = data[:, col] - data[:, 1]
    res = MRM_SPCM_analysis(x, y)
    plt.plot(x,y)
    plt.show()
# %%
