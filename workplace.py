#%%
from pathlib import Path
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm
import matplotlib.pyplot as plt
from scipy import signal

db_path = Path(r"X:\1_Database") / "DataBase.db"
local = Path(r"X:\1_Database\processing")
rawdata = Path(r"X:\1_Database\RawDataFiles")


#%% mCoupe need to revise
import matplotlib.pyplot as plt
save_folder = Path(r"Y:\量測資料\1_DataBase\Results\260702_mCoupe_plug_PM_mueller")
#measure_name = '260629_mCoupe_repeat_unplug_1'
measure_name = '260702_mCoupe_plug_PM'
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    group = {}
    ref = {}
    for session in sessions:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')[0]
        opt_ch = (spcm_info['optical_input_channel'],spcm_info['optical_output_channel'])
        if opt_ch not in group:
            group[opt_ch] = [spcm_info]
        else:
            group[opt_ch].append(spcm_info)

    group_tf = {('1', '1'): ['Group1', 'Group6'],#s15
                ('2', '2'): ['Group2', 'Group7'],#s15
                ('3', '3'): ['Group3', 'Group8'],#s15
                ('4', '4'): ['Group4', 'Group9'],#s15
                ('5', '5'): ['Group5', 'Group10'],#s15
                ('6', '6'): ['Group11', 'Group16'],#s16
                ('7', '7'): ['Group12', 'Group17'],#s16
                ('8', '8'): ['Group13', 'Group18'],#s16
                ('9', '9'): ['Group14', 'Group19'],#s16
                ('10', '10'): ['Group15', 'Group20'],#s16
                ('10', '11'): ['Group15', 'Group21'],#s16
                ('11', '11'): ['RL15', 'Group21'],#s16
                ('12', '12'): ['R15', 'NA'],#s15
                ('12', '13'): ['NA', 'R16'],#s16
                }
    for opt_ch in list(group.keys())[:]:
        spcm_info = group[opt_ch][0]
        path = Path(db.db_path).parent / spcm_info['file_path']
        _,data = read_spectrum_lite(path)

        normalized_path = path.with_name(path.name.replace("SPCMs", "SPCM"))
        read_path = normalized_path if normalized_path.exists() else path
        data_all = read_spectrum_all(read_path)
        mueller_data = data_all['mueller']
        mueller_data[:, 0] *= 1E9
        
        m = re.search(r'#(\d+)', spcm_info['file_path'])
        wavelenght = mueller_data[:,0]

        
        
        if group_tf[opt_ch][0] not in ['NA','R15','R16']:
            fig1, ax11 = plt.subplots(1, 1, figsize=(7, 5), sharex=True)
            ax12 = ax11.twinx()
            ax11.plot(wavelenght, -data[:,1],c = 'm', label=f"Min Loss")
            ax12.plot(wavelenght, mueller_data[:,1], label=f"M11")
            ax12.plot(wavelenght, mueller_data[:,2], label=f"M12")
            ax12.plot(wavelenght, mueller_data[:,3], label=f"M13")
            ax12.plot(wavelenght, mueller_data[:,4], label=f"M14")
            
            ax11.set_title(f"{group_tf[opt_ch][0]}")
            ax11.set_xlabel("Wavelength (nm)")
            ax11.set_ylabel("Loss (dB)")
            ax12.set_ylabel("M coefficients")
            ax11.set_ylim([-15, 0])
            ax12.set_ylim([-0.5, 0.5])
            h1, l1 = ax11.get_legend_handles_labels()
            h2, l2 = ax12.get_legend_handles_labels()
            ax11.legend(h1 + h2, l1 + l2, loc='best')
            fig1.tight_layout()
        
        if group_tf[opt_ch][1] not in ['NA','R15','R16']:# and group_tf[opt_ch][0] != 'RL15':
            fig2, ax21 = plt.subplots(1, 1, figsize=(7, 5), sharex=True)
            ax22 = ax21.twinx()
            ax21.plot(wavelenght, -data[:,3],c = 'm', label=f"Min Loss")
            ax22.plot(wavelenght, mueller_data[:,5], label=f"M11")
            ax22.plot(wavelenght, mueller_data[:,6], label=f"M12")
            ax22.plot(wavelenght, mueller_data[:,7], label=f"M13")
            ax22.plot(wavelenght, mueller_data[:,8], label=f"M14")
            ax21.set_ylim([-15, 0])
            ax22.set_ylim([-0.5, 0.5])
            ax21.set_title(f"{group_tf[opt_ch][1]}")
            ax21.set_xlabel("Wavelength (nm)")
            ax21.set_ylabel("Loss (dB)")
            ax22.set_ylabel("M coefficients")
            h3, l3 = ax21.get_legend_handles_labels()
            h4, l4 = ax22.get_legend_handles_labels()
            ax21.legend(h3 + h4, l3 + l4, loc='best')
            fig2.tight_layout()

        plt.show()
        save_path1 = save_folder / f"{group_tf[opt_ch][0]}.png"
        save_path2 = save_folder / f"{group_tf[opt_ch][1]}.png"
        if not save_path1.parent.exists():
            save_path1.parent.mkdir(parents=True)
        if group_tf[opt_ch][0] not in ['NA','R15','R16']:
            fig1.savefig(save_path1)
        if group_tf[opt_ch][1] not in ['NA','R15','R16']:# and group_tf[opt_ch][0] != 'RL15':
            fig2.savefig(save_path2)
# %% alpha calibration
#%matplotlib qt
measure_name = '260710_alpha'
linear_fit = {}
fit = []
spcm_data = {}
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in sessions:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')
        for spcm in spcm_info:
            path = Path(db.db_path).parent / spcm['file_path']
            data = read_spectrum_all(path)
            alpha_set = float(data['header']['Attenuation Set'][0])
            alpha_set = alpha_set if alpha_set>0 else 0
            x = data['min_max'][:,0]
            y = data['min_max'][:,1]
            spcm_data[spcm['data_id']] = {'alpha_set': alpha_set, 'x': x, 'y': y}
            fit += [[alpha_set]+np.polyfit(x, y, 1).tolist()]
    fit = np.array(fit)
    fit = fit[fit[:,0].argsort()]
    slope = np.polyfit(fit[:,0], fit[:,1], 2)
    intercept = np.polyfit(fit[:,0], fit[:,2], 2)

    fig = plt.figure(figsize=(12, 10))
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.2])
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, :])

    ax1.plot(fit[:,0], fit[:,1], label="slope", marker='o', markersize=2, linestyle='None', c='b')
    ax1.plot(fit[:,0], slope[0]*fit[:,0]**2 + slope[1]*fit[:,0] + slope[2], label="slope_fit", c='r')
    ax1.set_title("Slope vs Attenuation")
    ax1.set_xlabel("Attenuation Set (dB)")
    ax1.set_ylabel("Slope")
    ax1.text(
        0.02, 0.98,
        f"slope fit:\ny = {slope[0]:.3e}*x² + {slope[1]:.3e}*x + {slope[2]:.3e}",
        transform=ax1.transAxes,
        va='top',
        fontsize=9,
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='gray')
    )
    ax1.legend()

    ax2.plot(fit[:,0], fit[:,2], label="intercept", marker='o', markersize=2, linestyle='None', c='b')
    ax2.plot(fit[:,0], intercept[0]*fit[:,0]**2 + intercept[1]*fit[:,0] + intercept[2], label="intercept_fit", c='r')
    ax2.set_title("Intercept vs Attenuation")
    ax2.set_xlabel("Attenuation Set (dB)")
    ax2.set_ylabel("Intercept")
    ax2.text(
        0.02, 0.98,
        f"intercept fit:\ny = {intercept[0]:.3e}*x² + {intercept[1]:.3e}*x + {intercept[2]:.3e}",
        transform=ax2.transAxes,
        va='top',
        fontsize=9,
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='gray')
    )
    ax2.legend()
    print('slope:', slope)
    print('intercept:', intercept)

    for data_id in spcm_data:
        alpha_set = spcm_data[data_id]['alpha_set']
        x = spcm_data[data_id]['x']
        y = spcm_data[data_id]['y']

        slope_a = np.polyval(slope, alpha_set)
        intercept_a = np.polyval(intercept, alpha_set)
        alpha = lambda wavelength: slope_a * wavelength + intercept_a
        y_fit = alpha(x)
        
        if alpha_set>0:
            linear_fit[alpha] = np.polyfit(x, y, 1)
            ax3.plot(x, y, label=f"alpha: {alpha_set} dB", marker='o', markersize=2, linestyle='None', c='b')
            ax3.plot(x, y_fit, label=f"alpha: {alpha_set}_fit dB", c='r')

    ax3.set_title("Spectrum Linear Fit by Attenuation")
    ax3.set_xlabel("Wavelength")
    ax3.set_ylabel("Loss")
    ax3.text(0.02, 0.98,f"fit:\ny = Slope(alpha)*wavelength + Intercept(alpha)",
             transform=ax3.transAxes,
             va='top',
             fontsize=9,
             bbox=dict(facecolor='white', alpha=0.7, edgecolor='gray'))

    fig.tight_layout()
    plt.show()
# %% ch_loss
measure_name = '260723_R01_ch_loss'
loss = []
diff_loss = np.full((14, 14), np.nan)
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in sessions[:]:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')
        for spcm in spcm_info:
            path = Path(db.db_path).parent / spcm['file_path']
            ch_in = int(spcm['optical_input_channel'])
            ch_out = int(spcm['optical_output_channel'])
            data = read_spectrum_all(path)
            y = data['min_max'][:,1]
            y_smooth = signal.savgol_filter(y, y.shape[0]//5, 3)
            #plt.plot(data['min_max'][:,0], y)
            #plt.plot(data['min_max'][:,0], y_smooth)
            loss.append([ch_in, ch_out, np.max(y_smooth)])
            #print(ch_in,ch_out, np.max(y))
#print(max_value)
loss = np.array(loss)
#loss[:,2] -= np.max(loss[:,2])
print(loss)
for raw in loss:
    ch_in = int(raw[0])
    ch_out = int(raw[1])
    diff_loss[ch_in-1, ch_out-1] = raw[2] 
csv_path = Path(__file__).resolve().parent / f"{measure_name}_diff_loss.csv"
np.savetxt(csv_path, diff_loss, delimiter=",", fmt="%.6f")
print(f"Saved diff_loss CSV: {csv_path}")
diff_loss

# %% SSRF_analysis_by_session
measure_name = '260805_AMD_cage158_D4'
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in tqdm(sessions, desc="Sessions"):
        db.SSRF_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()

# %% delete
measure_name = '260731_AMD_cage158_D4'
with DatabaseAPI(db_path) as db:
    res = db.select_analyses(measure_name=measure_name, feature_type='S11 valley')#S11 impedance S11 valley
    for analysis_id in res:
        db.delete_record(DatabaseAPI.TABLE_ANALYSES,
                        analysis_id['analysis_id'],
                        commit=False)
    db.conn.commit()
        
# %%
measure_name = '260731_AMD_cage158_D4'
with DatabaseAPI(db_path) as db:
    analyses = db.select_analyses(measure_name=measure_name, feature_type='SSRF parameters')
    for analysis in analyses:
        data_id = db.select_analysis_sources(analysis['analysis_id'])[0]['data_id']
        electrode_info = db.select_electric(data_id)

# %%
