#%%
from pathlib import Path
from database_api import DatabaseAPI
import numpy as np
from scipy.signal import find_peaks,peak_widths,peak_prominences
from analysis import read_spectrum
from tqdm import tqdm

folder_path = Path(r"D:\processing\260205_mapping")
db_path = Path(r"Y:\量測資料\資料庫") / "DataBase.db"
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
        for info in infos:
            path = info['file_path']
            head,data = read_spectrum(path)
            x = data[:, 0]
            y = data[:, 3] - data[:, 1]
            baseline = np.polynomial.Polynomial.fit(x, y, 6)
            loss_level = y - baseline(x)
            prominence = -(loss_level.min() + loss_level.max())/3 
            valley_idx, props = find_peaks(-loss_level,prominence=prominence,distance=5)
            valley_x = x[valley_idx]
            valley_y = loss_level[valley_idx]
            FSR = valley_x[1:]-valley_x[:-1]
            FSR = np.vstack((np.r_[FSR,np.nan],np.r_[np.nan,FSR]))
            FSR = np.nanmax(FSR,axis = 0)
            Ty = 10**(loss_level/10)
            FWHM = peak_widths(-Ty, valley_idx, rel_height=0.5)[0]*np.median(np.diff(x))
            delta_T = peak_prominences(-Ty, valley_idx)[0]
            Q = delta_T/FWHM*1000

            if len(valley_idx) <= 6:
                analysis_id = db.insert_analysis_run(session_id = session_id,
                                                    analysis_type = f'basic_spectrum_analysis_{info["data_id"]}',
                                                    commit=False)
                for i in range(len(valley_idx)):
                    feature_id = db.insert_analysis_feature(analysis_id=analysis_id, feature_type='basic parameters', feature_index=i)
                    db.insert_feature_values(feature_id, {'valley wavelength': (valley_x[i],'nm'),
                                                        'FSR':(FSR[i],'nm'),
                                                        'FWHM':(FWHM[i],'nm'),
                                                        'Q factor':(Q[i],'')})
    db.conn.commit()

# %%
with DatabaseAPI(db_path) as db:
    sql = '''
    SELECT a.analysis_id
    FROM AnalysisRuns a;'''
    result = db.query(sql)
    #output_path = db.export_all_tables_to_xlsx(folder_path.parent / "database_export.xlsx")
    for analysis_id in [res['analysis_id'] for res in result]:
        db.delete_analysis_run(analysis_id)

# %%
import matplotlib.pyplot as plt
%matplotlib qt
with DatabaseAPI(db_path) as db:
    info = db.get_measurement_data_by_session(session_id = 1, data_type="SPCM")
    path = info[0]['file_path']

    head,data = read_spectrum(path)
    wavelength = data[:, 0]
    loss = data[:, 3] - data[:, 1]
    baseline = np.polynomial.Polynomial.fit(wavelength, loss, 6)
    loss_level = loss - baseline(wavelength)
    prominence = -(loss_level.min() + loss_level.max())/3 
    valley_idx, props = find_peaks(-loss_level,prominence=prominence,distance=5)
    valley_x = wavelength[valley_idx]
    valley_y = loss_level[valley_idx]
    FSR = valley_x[1:]-valley_x[:-1]
    FSR = np.vstack((np.r_[FSR,np.nan],np.r_[np.nan,FSR]))
    FSR = np.nanmax(FSR,axis = 0)
    ER = -peak_prominences(-loss_level, valley_idx)[0]
    Ty = 10**(loss_level/10)
    FWHM = peak_widths(-Ty, valley_idx, rel_height=0.5)[0]*np.median(np.diff(wavelength))
    
    Q = valley_x/FWHM

    plt.plot(wavelength,loss_level)
    plt.plot(valley_x,valley_y,'x')
    plt.show()
# %%
