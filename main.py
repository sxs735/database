#%%
from pathlib import Path
from database_api import DatabaseAPI
import numpy as np
from scipy.signal import find_peaks,peak_widths,peak_prominences,savgol_filter
from analysis import *
from tqdm import tqdm

folder_path = Path(r"C:\Users\mg942\Desktop\元澄\PIC9-FPN3_DOE1_MRM033_DC&RF_3dB\20260202")
db_path = Path(r"C:\Users\mg942\Desktop\元澄\Data") / "DataBase.db"
#%%
with DatabaseAPI(db_path) as db:
    db.import_measurement_folder(folder_path,schema_file="schema.sql")
    #output_path = db.export_all_tables_to_xlsx(folder_path.parent / "database_export.xlsx")
#%%
with DatabaseAPI(db_path) as db:
    sql = '''
    SELECT s.session_id,s.DUT_id,s.session_name,s.measurement_datetime
    FROM MeasurementSessions s
    WHERE NOT EXISTS (
    SELECT 1
    FROM AnalysisRuns a
    WHERE a.session_id = s.session_id)
    ORDER BY s.measurement_datetime'''
    result = db.query(sql)
    session_id_list = [d['session_id'] for d in result]
    for session_id in tqdm(session_id_list, desc="Importing", unit="file"):
        infos = db.get_measurement_data_by_session(session_id = session_id, data_type="SPCM")
        for idx, info in enumerate(infos):
            path = info['file_path']
            analysis_idx = idx
            head,data = read_spectrum(path)
            x = data[:, 0]
            y = data[:, 3] - data[:, 1]
            baseline = np.polynomial.Polynomial.fit(x, y, 6)
            loss_level = y - baseline(x)
            prominence = -(loss_level.min() + loss_level.max())/3 
            valley_idx, props = find_peaks(-loss_level,prominence=prominence,distance=5)
            valley_x = x[valley_idx]
            valley_y = loss_level[valley_idx]
            FSRnm = valley_x[1:]-valley_x[:-1]
            FSRnm = np.vstack((np.r_[FSRnm,np.nan],np.r_[np.nan,FSRnm]))
            FSRnm = np.nanmax(FSRnm,axis = 0)
            Ty = 10**(loss_level/10)
            FWHMnm = peak_widths(-Ty, valley_idx, rel_height=0.5)[0]*np.median(np.diff(x))
            delta_T = peak_prominences(-Ty, valley_idx)[0]
            Q = delta_T/FWHMnm*1000

            if len(valley_idx) <= 6:
                analysis_id = db.insert_analysis_run(session_id = session_id,
                                                     analysis_type = 'basic_spectrum_analysis',
                                                     analysis_index = analysis_idx,
                                                     algorithm = "valley_scan",
                                                     version = "1.0.0",
                                                     commit=False)
                
                db.insert_analysis_input(analysis_id, info["data_id"], commit=False)
                for i in range(len(valley_idx)):
                    feature_id = db.insert_analysis_feature(analysis_id=analysis_id, feature_type='basic parameters', feature_index=i)
                    db.insert_feature_values(feature_id, {'valley wavelength': (valley_x[i],'nm'),
                                                          'FSR':(FSRnm[i],'nm'),
                                                          'FWHM':(FWHMnm[i],'nm'),
                                                          'Q factor':(Q[i],'')})
    db.conn.commit()

# %%
import matplotlib.pyplot as plt
import time
%matplotlib qt
with DatabaseAPI(db_path) as db:
    spcm = db.get_measurement_data_by_session(session_id = 4, data_type="SPCM")
    for spcm_info in spcm:
        data_id = spcm_info['data_id']
        data_info = db.get_data_info_by_data(data_id=data_id)
        spcm_dict = {info['key']: info['value'] for info in data_info}
    dcvi = db.get_measurement_data_by_session(session_id = 4, data_type="DCVI")
    for dcvi_info in dcvi:
        data_id = dcvi_info['data_id']
        data_info = db.get_data_info_by_data(data_id=data_id)
        dcvi_dict = {info['key']: info['value'] for info in data_info}

    #dcvi = db.get_measurement_data_by_session(session_id = 4, data_type="DCVI")
    #dcvi_info = db.get_data_info_by_data(data_id=spcm[0]['data_id'])
    
# %%
