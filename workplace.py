#%%
from pathlib import Path
import pandas as pd
from database_api import DatabaseAPI
from analysis import *
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from scipy import signal
import re,os

db_path = Path(r"X:\1_Database") / "DataBase.db"
local = Path(r"X:\1_Database\processing")
rawdata = Path(r"X:\1_Database\RawDataFiles")


alpha_calibrate = {'slope':[8.90932512e-06,1.32686933e-03,-2.70565359e-04],
                   'intercept':[ -0.01208658,-2.73947931,0.37529609]}



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
        for spcm_path in spcm_info:
            path = Path(db.db_path).parent / spcm_path['file_path']
            data = read_spectrum_all(path)
            alpha_set = float(data['header']['Attenuation Set'][0])
            alpha_set = alpha_set if alpha_set>0 else 0
            x = data['min_max'][:,0]
            y = data['min_max'][:,1]
            spcm_data[spcm_path['data_id']] = {'alpha_set': alpha_set, 'x': x, 'y': y}
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
loss_dB = []
diff_loss = np.full((14, 14), np.nan)
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in sessions[:]:
        spcm_info = db.select_rawdata_files(session['session_id'],data_type='SPCM')
        for spcm_path in spcm_info:
            path = Path(db.db_path).parent / spcm_path['file_path']
            ch_in = int(spcm_path['optical_input_channel'])
            ch_out = int(spcm_path['optical_output_channel'])
            data = read_spectrum_all(path)
            y = data['min_max'][:,1]
            y_smooth = signal.savgol_filter(y, y.shape[0]//5, 3)
            #plt.plot(data['min_max'][:,0], y)
            #plt.plot(data['min_max'][:,0], y_smooth)
            loss_dB.append([ch_in, ch_out, np.max(y_smooth)])
            #print(ch_in,ch_out, np.max(y))
#print(max_value)
loss_dB = np.array(loss_dB)
#loss[:,2] -= np.max(loss[:,2])
print(loss_dB)
for raw in loss_dB:
    ch_in = int(raw[0])
    ch_out = int(raw[1])
    diff_loss[ch_in-1, ch_out-1] = raw[2] 
csv_path = Path(__file__).resolve().parent / f"{measure_name}_diff_loss.csv"
np.savetxt(csv_path, diff_loss, delimiter=",", fmt="%.6f")
print(f"Saved diff_loss CSV: {csv_path}")
diff_loss


# %% delete
measure_name = '260807_AMD_cage158_D4'
with DatabaseAPI(db_path) as db:
    res = db.select_analyses(measure_name=measure_name, analysis_type='MRM_OMA_analysis')
    for analysis_id in res:
        db.delete_record(DatabaseAPI.TABLE_ANALYSES,
                        analysis_id['analysis_id'],
                        commit=False)
    db.conn.commit()


#%% MRM_SPCM_analysis
measure_name = '260901_AMD_cage158_D4_85C'
print("Starting batch MRM_SPCM analysis...")
print(f"measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in tqdm(sessions, desc="Sessions"):
        db.MRM_SPCM_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()

#%% # %% SSRF_analysis_by_session
print("Starting batch MRM_SSRF analysis...")
print(f"measure_name: {measure_name}")
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in tqdm(sessions, desc="Sessions"):
        db.SSRF_analysis_by_session(session['session_id'],commit=False)
    db.conn.commit()   

# %% MRM Modulation efficiency analysis
print("Starting batch MRM_ME analysis...")
print(f"measure_name: {measure_name}")
commit = False
with DatabaseAPI(db_path) as db:
    analyses = db.select_analyses(measure_name = measure_name, analysis_type='MRM_SPCM_analysis')
    analyses_by_session = {}
    for analysis in analyses[:]:
        data_id = db.select_analysis_sources(analysis['analysis_id'])[0]['data_id']
        info = db.select_rawdata_info(data_id)
        optical = db.select_optical(data_id)
        electric = db.select_electric(data_id)
        another = db.select_another(data_id)
        power = optical['input_power']
        voltage = electric[0]['set_value']
        voltage = float(re.match(r'[-+]?\d+(?:\.\d+)?', voltage).group())/1000
        valley_wavelength = np.array([value['metric_value'] for value in  db.select_featuremetrics(analysis['analysis_id'],'Valley Wavelength')])
        valley_wavelength = valley_wavelength[np.argmin(np.abs(valley_wavelength-1315))]*1000
        if analysis['session_id'] not in analyses_by_session:
            analyses_by_session[analysis['session_id']] = {}
        if power not in analyses_by_session[analysis['session_id']]:
            analyses_by_session[analysis['session_id']][power] = []
        analyses_by_session[analysis['session_id']][power] += [[data_id,voltage, float(valley_wavelength)]]

    for session in analyses_by_session:
        for no, power in enumerate(analyses_by_session[session]):
            v_wavelength = np.array(analyses_by_session[session][power])
            results = np.diff(v_wavelength[:,1:], axis=0)
            results[:,0] = v_wavelength[1:,1]

            analysis_id = db.insert_analysis(session_id = session,
                                             analysis_type = 'MRM_ME_analysis',
                                             instance_no = no,
                                             commit=commit)
            for data_id, voltage, valley_wavelength in v_wavelength:
                db.insert_sources(analysis_id, data_id, commit=commit)
            for i, (vol, eff) in enumerate(results):
                feature_id = db.insert_feature(analysis_id=analysis_id, 
                                               feature_type='Basic', 
                                               feature_idx=i,
                                               algorithm = 'Modulation efficiency',
                                               version = "1.0.0",
                                               commit=commit)
                result_idx = {'voltage': (float(round(vol, 3)), 'V'),
                              'efficiency': (float(round(eff, 3)), 'pm/V')}
                db.insert_metrics(feature_id, result_idx, commit=commit)
    db.conn.commit() 
# %% MRM_OMA_analysis
print("Starting batch MRM_OMA analysis...")
print(f"measure_name: {measure_name}")
commit = False
ssrf = {}
with DatabaseAPI(db_path) as db:
    analyses = db.select_analyses(measure_name = measure_name, analysis_type='SSRF analysis dB-tuning')
    analyses_by_session = {}
    for analysis in analyses[:]:
        data_id = db.select_analysis_sources(analysis['analysis_id'])[0]['data_id']
        info = db.select_rawdata_info(data_id)
        electric = db.select_electric(data_id)
        optical = db.select_optical(data_id)
        another = db.select_another(data_id)[0]

        voltage = electric[0]['set_value']
        voltage = float(re.match(r'[-+]?\d*\.?\d+', voltage).group())/1000
        
        if voltage == 0:
            if analysis['session_id'] not in analyses_by_session:
                analyses_by_session[analysis['session_id']] = {}
            if optical['input_power'] not in analyses_by_session[analysis['session_id']]:
                analyses_by_session[analysis['session_id']][optical['input_power']] = []
            analyses_by_session[analysis['session_id']][optical['input_power']] += [float(another['info_value'])]

    for session in analyses_by_session.keys():
        for no, power in enumerate(analyses_by_session[session].keys()):
            spcm_infos = db.select_rawdata_files(session, data_type='SPCM',optical_input_power = power)
            spcm_files = {}
            for spcm_info in spcm_infos:
                data_id = spcm_info['data_id']
                electric = db.select_electric(data_id)
                optical = db.select_optical(data_id)
                alpha_set = optical.get('attenuation', 0)
                alpha_set = float(re.match(r'[-+]?\d*\.?\d+', str(alpha_set)).group())
                voltage = electric[0]['set_value']
                voltage = float(re.match(r'[-+]?\d*\.?\d+', voltage).group())/1000
                spcm_files[voltage] =  [alpha_set, db_path.parent / spcm_info['file_path']]

            slope = np.polyval(alpha_calibrate['slope'], alpha_set)
            intercept = np.polyval(alpha_calibrate['intercept'], alpha_set)
            alpha = lambda wavelength: slope * wavelength + intercept
            p0 = float(re.match(r'[-+]?\d*\.?\d+', power).group())
            voltage = np.sort(np.array(list(spcm_files.keys())))
            op_lambda = np.array(analyses_by_session[session][power])

            spcm_path = []
            for volt in voltage:
                spcm_path += [read_spectrum_all(spcm_files[volt][1])['average_il']]
                #spcm += [read_spectrum_all(spcm_files[volt][1])['min_max']]

            start_idx = np.argmin(np.abs(spcm_path[0][:, 0] - 1310))
            end_idx = np.argmin(np.abs(spcm_path[0][:, 0] - 1317))

            wavelength_l = spcm_path[0][start_idx:end_idx,0]
            a = alpha(wavelength_l)
            for i, spcm_i in enumerate(spcm_path):
                spcm_j = spcm_i[start_idx:end_idx,1] - spcm_i[start_idx:end_idx,2] - a
                spcm_j -= np.max(spcm_j)
                spcm_path[i] = spcm_j

                
            # for i, spcm_i in enumerate(spcm):
            #     plt.plot(wavelength_l, spcm_i, label=f"{voltage[i]} V")
            # plt.show()

            spcm_0 = spcm_path[0] 
            spcm_v = spcm_path[-1]
            T_0 = 10**(spcm_0/10)
            T_v = 10**(spcm_v/10)

            # plt.plot(wavelength_l, T_0, label='T_0')
            # plt.plot(wavelength_l, T_v, label='T_v')
            # plt.show()

            best_oma_i = np.argmax(T_v-T_0)
            analyses_by_session[session][power] += [round(float(wavelength_l[best_oma_i]),3)]
            oma_db = 10*np.log10(T_v[best_oma_i]-T_0[best_oma_i])

            #loss = []
            transmission = []
            tune_wavelengths = np.array(analyses_by_session[session][power])
            for wl in tune_wavelengths:
                idx = np.argmin(np.abs(wavelength_l-wl))
                #loss += [[spcm_i[idx] for spcm_i in spcm]]
                transmission += [[10**(spcm_i[idx]/10) for spcm_i in spcm_path]]
            #loss_local = np.array(loss_local)
            transmission = np.array(transmission)

            analysis_id = db.insert_analysis(session_id = session,
                                             analysis_type = 'MRM_OMA_analysis',
                                             instance_no = no,
                                             commit=commit)
            for info in spcm_infos:
                db.insert_sources(analysis_id, info['data_id'], commit=commit)

            results = {}
            for tune,trans in enumerate(transmission[:-1]):
                # for volt_idx, volt in enumerate(voltage):
                #     results[f'Loss_at_{volt}V'] = round(float(loss_dB[volt_idx]),3)

                t_min = np.min(trans)
                t_max = np.max(trans)
                results[f'Tuning_wavelength'] = tune_wavelengths[tune]
                results['OMA_dB'] = round(float(10*np.log10(t_max-t_min)),3)


                feature_id = db.insert_feature(analysis_id=analysis_id, 
                                               feature_type='loss', 
                                               feature_idx=tune,
                                               algorithm = 'loss at each tuning dB',
                                               version = "1.0.0",
                                               commit=commit)
                
                db.insert_metrics(feature_id, results, commit=commit)

            best_oma = {'Best_OMA_nm': tune_wavelengths[-1],
                        'Best_OMA_dB': oma_db}

            feature_id = db.insert_feature(analysis_id=analysis_id, 
                                           feature_type='loss', 
                                           feature_idx=tune+1,
                                           algorithm = 'best oma',
                                           version = "1.0.0",
                                           commit=commit)
                
            db.insert_metrics(feature_id, best_oma, commit=commit)
    db.conn.commit() 
#%% 
def plot_metric_boxplot_by_die(df, metric, target_die, target_power, x_col='voltage', result_dir=Path(r"X:\2.Results"), y_limits=(1.605, 1.615)):
    target_powers = target_power if isinstance(target_power, (list, tuple, np.ndarray)) else [target_power]
    target_powers = list(target_powers)
    if len(target_powers) == 0:
        print("target_power is empty")
        return
    if x_col not in df.columns:
        raise ValueError(f"x_col '{x_col}' not found in DataFrame columns")

    fig, axes = plt.subplots(1, len(target_powers), figsize=(8 * len(target_powers), 5), sharey=True)
    axes = np.atleast_1d(axes)

    for idx, (ax, power_i) in enumerate(zip(axes, target_powers)):
        df_i = df[(df['die']== target_die) & (df['power'] == power_i)].copy()
        if df_i.empty:
            ax.set_title(f"{metric} @die:{target_die}, power:{power_i} dBm\nNo data")
            ax.set_xlabel("Bias Voltage (V)" if x_col == 'voltage' else x_col)
            ax.grid(axis='y', alpha=0.3)
            if idx == 0:
                ax.set_ylabel(metric)
            else:
                ax.tick_params(axis='y', left=False, labelleft=False)
                ax.spines['left'].set_visible(False)
            continue

        df_i = df_i.sort_values(x_col)
        x_values = sorted(df_i[x_col].unique())
        box_data = [df_i.loc[df_i[x_col] == x, metric].astype(float).values for x in x_values]

        stats_df = (
            df_i.groupby(x_col, as_index=False)[metric]
            .agg(count='count', mean='mean', std='std', median='median')
            .sort_values(x_col)
        )
        metric_safe = metric.replace("/", "_")
        stats_path = result_dir / f"{metric_safe}_stats_die{target_die}_power{power_i}dBm.csv"
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_df.to_csv(stats_path, index=False, encoding='utf-8-sig')

        ax.boxplot(
            box_data,
            tick_labels=[f"{x:g}" if isinstance(x, (int, float, np.integer, np.floating)) else str(x) for x in x_values],
            showmeans=True,
            showfliers=False,
            meanline=True,
            boxprops=dict(color='red', linewidth=1.5),
            whiskerprops=dict(color='red', linewidth=1.5),
            capprops=dict(color='red', linewidth=1.5),
            meanprops=dict(color='red', linewidth=2, linestyle='-'),
            medianprops=dict(linewidth=0)
        )

        rng = np.random.default_rng(42)
        for i, values in enumerate(box_data, start=1):
            x_jitter = rng.normal(loc=0.0, scale=0.04, size=len(values))
            x = np.full(len(values), i) + x_jitter
            ax.scatter(x, values, s=12, alpha=1, color='black', edgecolors='white', linewidths=0.5, zorder=3)

        ax.set_title(f"{metric} @die:{target_die}, power:{power_i} dBm")
        ax.set_xlabel("Bias Voltage (V)" if x_col == 'voltage' else x_col)
        ax.grid(axis='y', alpha=0.3)
        if idx == 0:
            ax.set_ylabel(metric)
        else:
            ax.tick_params(axis='y', left=False, labelleft=False)
            ax.spines['left'].set_visible(False)
        if y_limits is not None:
            ax.set_ylim(*y_limits)

    fig.tight_layout()
    fig.subplots_adjust(wspace=0)
    power_tag = "_".join([f"{p}dBm" for p in target_powers])
    
    fig_path = result_dir / rf"{metric_safe}_boxplot_die{target_die}_{power_tag}.png"
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_metric_boxplot_by_repeat(df, metric, target_repeat, target_power, x_col='voltage', result_dir=Path(r"X:\2.Results"), y_limits=(1.605, 1.615)):
    target_powers = target_power if isinstance(target_power, (list, tuple, np.ndarray)) else [target_power]
    target_powers = list(target_powers)
    if len(target_powers) == 0:
        print("target_power is empty")
        return
    if x_col not in df.columns:
        raise ValueError(f"x_col '{x_col}' not found in DataFrame columns")

    fig, axes = plt.subplots(1, len(target_powers), figsize=(8 * len(target_powers), 5), sharey=True)
    axes = np.atleast_1d(axes)

    for idx, (ax, power_i) in enumerate(zip(axes, target_powers)):
        df_i = df[(df['repeat'] == target_repeat) & (df['power'] == power_i)].copy()
        if df_i.empty:
            ax.set_title(f"{metric} Full-Wafer power:{power_i} dBm\nNo data")
            ax.set_xlabel("Bias Voltage (V)" if x_col == 'voltage' else x_col)
            ax.grid(axis='y', alpha=0.3)
            if idx == 0:
                ax.set_ylabel(metric)
            else:
                ax.tick_params(axis='y', left=False, labelleft=False)
                ax.spines['left'].set_visible(False)
            continue

        df_i = df_i.sort_values(x_col)
        x_values = sorted(df_i[x_col].unique())
        box_data = [df_i.loc[df_i[x_col] == x, metric].astype(float).values for x in x_values]

        stats_df = (
            df_i.groupby(x_col, as_index=False)[metric]
            .agg(count='count', mean='mean', std='std', median='median')
            .sort_values(x_col)
        )
        metric_safe = metric.replace("/", "_")
        stats_path = result_dir / f"{metric_safe}_stats_Full-Wafer_{power_i}dBm.csv"
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_df.to_csv(stats_path, index=False, encoding='utf-8-sig')

        ax.boxplot(box_data,
                   tick_labels=[f"{x:g}" if isinstance(x, (int, float, np.integer, np.floating)) else str(x) for x in x_values],
                   showmeans=True,
                   showfliers=False,
                   meanline=True,
                   boxprops=dict(color='red', linewidth=1.5),
                   whiskerprops=dict(color='red', linewidth=1.5),
                   capprops=dict(color='red', linewidth=1.5),
                   meanprops=dict(color='red', linewidth=2, linestyle='-'),
                   medianprops=dict(linewidth=0))

        rng = np.random.default_rng(42)
        for i, values in enumerate(box_data, start=1):
            x_jitter = rng.normal(loc=0.0, scale=0.04, size=len(values))
            x = np.full(len(values), i) + x_jitter
            ax.scatter(x, values, s=12, alpha=1, color='black', edgecolors='white', linewidths=0.5, zorder=3)

        ax.set_title(f"{metric} Full-Wafer power:{power_i} dBm")
        ax.set_xlabel("Bias Voltage (V)" if x_col == 'voltage' else x_col)
        ax.grid(axis='y', alpha=0.3)
        if idx == 0:
            ax.set_ylabel(metric)
        else:
            ax.tick_params(axis='y', left=False, labelleft=False)
            ax.spines['left'].set_visible(False)
        if y_limits is not None:
            ax.set_ylim(*y_limits)

    fig.tight_layout()
    fig.subplots_adjust(wspace=0)
    power_tag = "_".join([f"{p}dBm" for p in target_powers])
    metric_safe = metric.replace("/", "_")
    fig_path = result_dir / f"{metric_safe}_boxplot_full_wafer_{power_tag}.png"
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_metric_boxplot(df, metric, target_die,target_repeat, target_power, x_col='voltage', result_dir=Path(r"X:\2.Results")):
    target_powers = target_power if isinstance(target_power, (list, tuple, np.ndarray)) else [target_power]
    target_powers = list(target_powers)
    if len(target_powers) == 0:
        print("target_power is empty")
        return

    required_cols = {'die', 'repeat', 'power', x_col, metric}
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    metric_safe = metric.replace("/", "_")
    result_dir.mkdir(parents=True, exist_ok=True)

    valid_powers = [p for p in target_powers if not df[df['power'] == p].empty]
    if len(valid_powers) == 0:
        print("No data for all target_power")
        return

    for power_i in valid_powers:
        df_power = df[df['power'] == power_i].copy()
        x_values = sorted(df_power[x_col].dropna().unique())
        if len(x_values) == 0:
            print(f"No valid x_col values for power={power_i} dBm")
            continue

        m = len(x_values)
        fig, axes = plt.subplots(1, m, figsize=(3.6 * m, 5.5), sharey=True)
        axes = np.atleast_1d(axes)
        rng = np.random.default_rng(42)

        for x_idx, (ax, x_val) in enumerate(zip(axes, x_values)):
            die_values = df_power[(df_power['die'] == target_die) & (df_power[x_col] == x_val)][metric].astype(float).dropna().values
            repeat_values = df_power[(df_power['repeat'] == target_repeat) & (df_power[x_col] == x_val)][metric].astype(float).dropna().values

            box_data = []
            labels = []
            if len(die_values) > 0:
                box_data.append(die_values)
                labels.append(f"die{target_die}")
            if len(repeat_values) > 0:
                box_data.append(repeat_values)
                labels.append(f"Full-Wafer")

            if len(box_data) > 0:
                ax.boxplot(
                    box_data,
                    tick_labels=labels,
                    showmeans=True,
                    showfliers=False,
                    meanline=True,
                    boxprops=dict(color='red', linewidth=1.5),
                    whiskerprops=dict(color='red', linewidth=1.5),
                    capprops=dict(color='red', linewidth=1.5),
                    meanprops=dict(color='red', linewidth=2, linestyle='-'),
                    medianprops=dict(linewidth=0)
                )

                for i, values in enumerate(box_data, start=1):
                    x_jitter = rng.normal(loc=0.0, scale=0.04, size=len(values))
                    x = np.full(len(values), i) + x_jitter
                    ax.scatter(x, values, s=12, alpha=0.9, color='black', edgecolors='white', linewidths=0.5, zorder=3)
            else:
                ax.set_xticks([1])
                ax.set_xticklabels(["No data"])

            x_label = f"{x_col}={x_val:g}" if isinstance(x_val, (int, float, np.integer, np.floating)) else f"{x_col}={x_val}"
            ax.set_xlabel(x_label, labelpad=8)
            #ax.set_title(x_label, y=-0.2)
            ax.grid(axis='y', alpha=0.3)

            if x_idx == 0:
                ax.set_ylabel(metric)
            else:
                ax.tick_params(axis='y', left=False, labelleft=False)
                ax.spines['left'].set_visible(False)

        fig.suptitle(f"{metric} comparison @ power={power_i} dBm (die{target_die} vs Full-Wafer)", y=1.02)
        fig.tight_layout(pad=0.3, w_pad=0, h_pad=0.3)
        fig.subplots_adjust(wspace=0, bottom=0.24)

        fig_path = result_dir / f"{metric_safe}_boxplot_die{target_die}_Full-Wafer_{power_i}dBm.png"
        fig.savefig(fig_path, dpi=300, bbox_inches='tight')
        print(f"Saved figure: {fig_path}")
        plt.show()

def plot_metric_boxplot_by_diegroup(df, metric, die_group1, die_group2, target_power, x_col='voltage', result_dir=Path(r"X:\2.Results")):
    target_powers = target_power if isinstance(target_power, (list, tuple, np.ndarray)) else [target_power]
    target_powers = list(target_powers)
    if len(target_powers) == 0:
        print("target_power is empty")
        return

    required_cols = {'die', 'repeat', 'power', x_col, metric}
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    metric_safe = metric.replace("/", "_")
    result_dir.mkdir(parents=True, exist_ok=True)

    valid_powers = [p for p in target_powers if not df[df['power'] == p].empty]
    if len(valid_powers) == 0:
        print("No data for all target_power")
        return

    x_values = sorted(df[x_col].dropna().unique())
    m = len(x_values)
    fig, axes = plt.subplots(1, m, figsize=(3.6 * m, 5.5), sharey=True)
    axes = np.atleast_1d(axes)
    rng = np.random.default_rng(42)
    power_colors = {
        power_i: plt.get_cmap('tab10')(index % 10)
        for index, power_i in enumerate(valid_powers)
    }

    for x_idx, (ax, x_val) in enumerate(zip(axes, x_values)):
        box_data = []
        labels = []
        box_colors = []
        for power_i in valid_powers:
            df_power = df[df['power'] == power_i]
            group1_values = df_power[(df_power['die'].isin(die_group1)) & (df_power[x_col] == x_val)][metric].astype(float).dropna().values
            group2_values = df_power[(df_power['die'].isin(die_group2)) & (df_power[x_col] == x_val)][metric].astype(float).dropna().values

            if len(group1_values) > 0:
                box_data.append(group1_values)
                labels.append(f"Center")
                box_colors.append(power_colors[power_i])
            if len(group2_values) > 0:
                box_data.append(group2_values)
                labels.append(f"Surrounding")
                box_colors.append(power_colors[power_i])

        if len(box_data) > 0:
            boxplot = ax.boxplot(
                box_data,
                tick_labels=labels,
                showmeans=True,
                showfliers=False,
                meanline=True,
                boxprops=dict(linewidth=1.5),
                whiskerprops=dict(linewidth=1.5),
                capprops=dict(linewidth=1.5),
                meanprops=dict(linewidth=2, linestyle='-'),
                medianprops=dict(linewidth=0)
            )

            for i, (values, color) in enumerate(zip(box_data, box_colors), start=1):
                boxplot['boxes'][i - 1].set_color(color)
                boxplot['means'][i - 1].set_color(color)
                for line in boxplot['whiskers'][2 * (i - 1):2 * i] + boxplot['caps'][2 * (i - 1):2 * i]:
                    line.set_color(color)
                x_jitter = rng.normal(loc=0.0, scale=0.04, size=len(values))
                x = np.full(len(values), i) + x_jitter
                ax.scatter(x, values, s=12, alpha=0.9, color=color, edgecolors='white', linewidths=0.5, zorder=3)
        else:
            ax.set_xticks([1])
            ax.set_xticklabels(["No data"])

        x_label = f"{x_col}={x_val:g}" if isinstance(x_val, (int, float, np.integer, np.floating)) else f"{x_col}={x_val}"
        ax.set_xlabel(x_label, labelpad=8)
        #ax.set_title(x_label, y=-0.2)
        ax.grid(axis='y', alpha=0.3)

        if x_idx == 0:
            ax.set_ylabel(metric)
        else:
            ax.tick_params(axis='y', left=False, labelleft=False)
            ax.spines['left'].set_visible(False)

    from matplotlib.lines import Line2D

    legend_handles = [
        Line2D([0], [0], color=color, marker='o', linewidth=1.5, label=f"{power_i:g} dBm")
        for power_i, color in power_colors.items()
    ]
    fig.suptitle(f"{metric} comparison (Center vs Surrounding)", y=0.99)
    fig.legend(handles=legend_handles, loc='upper right', ncol=len(legend_handles), bbox_to_anchor=(0.98, 0.96))
    fig.tight_layout(pad=0.3, w_pad=0, h_pad=0.3, rect=(0, 0, 1, 0.94))
    fig.subplots_adjust(wspace=0, bottom=0.24)

    fig_path = result_dir / f"{metric_safe}_boxplot_Center_Surrounding_dBm.png"
    fig.savefig(fig_path, dpi=300, bbox_inches='tight')
    print(f"Saved figure: {fig_path}")
    plt.show()


measure_list = ['260807_AMD_cage158_D4','260901_AMD_cage158_D4_85C','260902_AMD_cage158_D4_85C']
save_folder ="260903_boxplot"

#%% MRM_SPCM_analysis data export
rows = []
analyses = []
with DatabaseAPI(db_path) as db:
    for measure_name in measure_list:
        analyses += db.select_analyses(measure_name = measure_name, analysis_type='MRM_SPCM_analysis')
    for analysis in analyses:
        session_id = analysis['session_id']
        session_info = db.select_session_info(session_id)
        repeat = session_info['session_idx']
        die = session_info['die']
        cage = session_info['cage']
        device = session_info['device']
        sources = db.select_analysis_sources(analysis['analysis_id'])

        electric = db.select_electric(sources[0]['data_id'])[0]
        voltage = float(re.match(r'[-+]?\d*\.?\d+', electric['set_value']).group())/1000
        optical = db.select_optical(sources[0]['data_id'])
        power = float(re.match(r'[-+]?\d*\.?\d+', optical['input_power']).group())
        condition = db.select_conditions(sources[0]['data_id'])[0]
        TC = float(condition['setting_value']) if condition['setting_parameters'] == 'temperature' else np.nan

        Valley_Wavelength = db.select_featuremetrics(analysis['analysis_id'],'Valley Wavelength')
        Valley_Wavelength = np.array([(d['feature_idx'], d['metric_value']) for d in Valley_Wavelength])

        FSR_nm = db.select_featuremetrics(analysis['analysis_id'],'FSR(nm)')
        FSR_nm = np.array([(d['feature_idx'], d['metric_value']) for d in FSR_nm])

        FSR_Thz = db.select_featuremetrics(analysis['analysis_id'],'FSR(THz)')
        FSR_Thz = np.array([(d['feature_idx'], d['metric_value']) for d in FSR_Thz])

        FWHN_nm = db.select_featuremetrics(analysis['analysis_id'],'FWHM(nm)')
        FWHN_nm = np.array([(d['feature_idx'], d['metric_value']) for d in FWHN_nm])

        FWHM_GHz = db.select_featuremetrics(analysis['analysis_id'],'FWHM(GHz)')
        FWHM_GHz = np.array([(d['feature_idx'], d['metric_value']) for d in FWHM_GHz])

        Q_factor = db.select_featuremetrics(analysis['analysis_id'],'Q factor')
        Q_factor = np.array([(d['feature_idx'], d['metric_value']) for d in Q_factor])

        ER = db.select_featuremetrics(analysis['analysis_id'],'Extinction Ratio')
        ER = np.array([(d['feature_idx'], d['metric_value']) for d in ER])

        idx_1310 = int(Valley_Wavelength[np.argmin(np.abs(Valley_Wavelength[:,1]-1312)),0])
        FSR_Thz = FSR_Thz[np.where(FSR_Thz[:,0]==idx_1310)[0][0],1]
        Q_factor = Q_factor[np.where(Q_factor[:,0]==idx_1310)[0][0],1]
        FWHM_nm = FWHN_nm[np.where(FWHN_nm[:,0]==idx_1310)[0][0],1]
        FWHM_GHz = FWHM_GHz[np.where(FWHM_GHz[:,0]==idx_1310)[0][0],1]
        Valley_Wavelength = Valley_Wavelength[np.where(Valley_Wavelength[:,0]==idx_1310)[0][0],1]
        ER = ER[np.where(ER[:,0]==idx_1310)[0][0],1]

        rows.append((cage, device, die, repeat, power, TC, voltage, Valley_Wavelength, FSR_Thz, Q_factor, FWHM_nm, FWHM_GHz, ER))

columns = ['cage', 'Device', 'die', 'repeat', 'power', 'Temperature(C)', 'Bias Voltage(V)','Valley Wavelength','FSR(Thz)', 'Q factor', 'FWHM(nm)', 'FWHM(GHz)', 'Extinction Ratio']
df = pd.DataFrame(rows, columns=columns)
xlsx_path = Path(r"X:\2.Results") / save_folder /"MRM_SPCM.xlsx"
xlsx_path.parent.mkdir(parents=True, exist_ok=True)
df.to_excel(xlsx_path, index=False)
print(f"Saved to: {xlsx_path}")

#%% MRM_OMA_analysis data export
analyses = []
rows = []
best = []
with DatabaseAPI(db_path) as db:
    for measure_name in measure_list:
        analyses += db.select_analyses(measure_name = measure_name, analysis_type='MRM_OMA_analysis')
    for analysis in analyses:
        session_id = analysis['session_id']
        session_info = db.select_session_info(session_id)
        repeat = session_info['session_idx']
        die = session_info['die']
        cage = session_info['cage']
        device = session_info['device']
        sources = db.select_analysis_sources(analysis['analysis_id'])

        optical = db.select_optical(sources[0]['data_id'])
        power = float(re.match(r'[-+]?\d*\.?\d+', optical['input_power']).group())
        condition = db.select_conditions(sources[0]['data_id'])[0]
        TC = float(condition['setting_value']) if condition['setting_parameters'] == 'temperature' else np.nan

        oma_db = db.select_featuremetrics(analysis['analysis_id'],'OMA_dB')
        best_oma_db = db.select_featuremetrics(analysis['analysis_id'],'Best_OMA_dB')
        best_oma_nm = db.select_featuremetrics(analysis['analysis_id'],'Best_OMA_nm')
        tune_lambda = db.select_featuremetrics(analysis['analysis_id'],'Tuning_wavelength')

        for row in tune_lambda:
            tune_idx = row['feature_idx']
            oma_db_value = oma_db[tune_idx]['metric_value']
            best_oma_db_value = best_oma_db[0]['metric_value']
            best_oma_nm_value = best_oma_nm[0]['metric_value']
            tune_lambda_value = row['metric_value']
            offset_tune = 4 if len(tune_lambda) == 4 else 0
            rows.append((cage, device, die, repeat, power, TC, tune_idx + offset_tune, 
                         tune_lambda_value,oma_db_value,
                         np.nan,np.nan))
        best.append((cage, device, die, repeat, power,TC,
                     best_oma_nm_value,
                     best_oma_db_value))

columns = ['cage','Device','die','repeat','power','Temperature(C)',
           'Detuning(dB)','Tuning wavelength(nm)','OMA(dB)',
           'Best OMA wavelength(nm)','Best OMA(dB)']
df_oma = pd.DataFrame(rows, columns=columns)
xlsx_path = Path(r"X:\2.Results") / save_folder / f"MRM_OMA.xlsx"
xlsx_path.parent.mkdir(parents=True, exist_ok=True)
df_oma.to_excel(xlsx_path, index=False)

columns = ['cage','Device','die','repeat','power','Temperature(C)','Best OMA wavelength(nm)','Best OMA(dB)']
df_best = pd.DataFrame(best, columns=columns)
xlsx_path = Path(r"X:\2.Results") / save_folder / f"MRM_Best_OMA.xlsx"
xlsx_path.parent.mkdir(parents=True, exist_ok=True)
df_best.to_excel(xlsx_path, index=False)
#%% MRM_SSRF_analysis data export
rows = []
analyses = []
with DatabaseAPI(db_path) as db:
    for measure_name in measure_list:
        analyses += db.select_analyses(measure_name = measure_name, analysis_type='SSRF analysis dB-tuning')
    for analysis in analyses[:]:
        session_id = analysis['session_id']
        session_info = db.select_session_info(session_id)
        repeat = session_info['session_idx']
        die = session_info['die']
        cage = session_info['cage']
        device = session_info['device']
        sources = db.select_analysis_sources(analysis['analysis_id'])

        optical = db.select_optical(sources[0]['data_id'])
        power = float(re.match(r'[-+]?\d*\.?\d+', optical['input_power']).group())
        electric = db.select_electric(sources[0]['data_id'])[0]
        voltage = float(re.match(r'[-+]?\d*\.?\d+', electric['set_value']).group())/1000
        condition = db.select_conditions(sources[0]['data_id'])[0]
        TC = float(condition['setting_value']) if condition['setting_parameters'] == 'temperature' else np.nan
        tune_dB = int(float(db.select_another(sources[0]['data_id'])[1]['info_value'].split('dB')[0]))
        
        if voltage == 1.0:
            bandwidth = db.select_featuremetrics(analysis['analysis_id'],'Bandwidth')
            rows.append((cage, device, die, repeat, power, TC, voltage, 
                            tune_dB,bandwidth[0]['metric_value']))
columns = ['cage','Device','die','repeat','power','Temperature(C)','voltage',
           'Detuning(dB)', 'Bandwidth(GHz)']
df_ssrf = pd.DataFrame(rows, columns=columns)
xlsx_path = Path(r"X:\2.Results") / save_folder / f"MRM_SSRF.xlsx"
xlsx_path.parent.mkdir(parents=True, exist_ok=True)
df_ssrf.to_excel(xlsx_path, index=False)
#%% MRM_ME_analysis data export
rows = []
analyses = []
with DatabaseAPI(db_path) as db:
    for measure_name in measure_list:
        analyses += db.select_analyses(measure_name = measure_name, analysis_type='MRM_ME_analysis')
    for analysis in analyses[:]:
        session_id = analysis['session_id']
        session_info = db.select_session_info(session_id)
        repeat = session_info['session_idx']
        die = session_info['die']
        cage = session_info['cage']
        device = session_info['device']
        sources = db.select_analysis_sources(analysis['analysis_id'])

        optical = db.select_optical(sources[0]['data_id'])
        power = float(re.match(r'[-+]?\d*\.?\d+', optical['input_power']).group())
        condition = db.select_conditions(sources[0]['data_id'])[0]
        TC = float(condition['setting_value']) if condition['setting_parameters'] == 'temperature' else np.nan

        efficiency = db.select_featuremetrics(analysis['analysis_id'],'efficiency')
        voltage = db.select_featuremetrics(analysis['analysis_id'],'voltage')

        for row in voltage:
            voltage_idx = row['feature_idx']
            voltage_value = row['metric_value']
            efficiency_value = efficiency[voltage_idx]['metric_value']
            rows.append((cage, device, die, repeat, power, TC, voltage_idx, 
                         voltage_value,efficiency_value))

columns = ['cage','Device','die','repeat','power','Temperature(C)','voltage_idx','Modulation voltage(V)', 'Efficiency(pm/V)']
df_me = pd.DataFrame(rows, columns=columns)
xlsx_path = Path(r"X:\2.Results") / save_folder /f"MRM_ME.xlsx"
xlsx_path.parent.mkdir(parents=True, exist_ok=True)
df_me.to_excel(xlsx_path, index=False)


#%%
%matplotlib qt
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


def plot_grouped_boxplots(df,
                          filter_column,filter_values,
                          filter_column2=None,filter_values2=None,
                          group_columns=None,value_column=None,
                          output_dir="./boxplots",figsize=None,dpi=300,
                          rotation=45,colors=None):
    """
    Plot grouped boxplots horizontally for multiple filter values.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame.

    filter_column : str
        Column used to divide the DataFrame into sub-DataFrames.

    filter_values : list
        Values to plot from filter_column.

        Example:
            [0, 1, 2]

    filter_column2 : str or None
        Optional column used to split each filter_column subset.

    filter_values2 : list or None
        Values to plot from filter_column2. Each subset is drawn on the
        same subplot for its filter_column value.

    group_columns : list[str] or None
        Columns used to define each boxplot group.

        Example:
            ["power", "Temperature(C)"]

        This will create groups such as:

            -10 | 25
            -10 | 50
             0  | 25
             0  | 50
            10  | 25
            10  | 50

        The first column, group_columns[0], determines
        the boxplot color.

    value_column : str
        Column used as the boxplot Y-axis value.

    output_dir : str
        Directory used to save the figure.

    figsize : tuple or None
        Figure size.

        If None, the size is automatically calculated.

    dpi : int
        Output image DPI.

    showfliers : bool
        Whether to show outliers.

    box_width : float
        Width of each boxplot.

    rotation : float
        Rotation angle of X-axis labels.

    colors : list or None
        Custom color sequence. Colors repeat when there are more groups than
        colors. If None, the default custom palette is used.

    Returns
    -------
    output_path : str
        Path of the saved figure.
    """


    # ==========================================================
    # 1. Check parameters
    # ==========================================================

    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必須是 pandas.DataFrame")

    if filter_column not in df.columns:
        raise ValueError(f"filter_column '{filter_column}' 不存在於 DataFrame")

    if (filter_column2 is None) != (filter_values2 is None):
        raise ValueError("filter_column2 和 filter_values2 必須同時提供")

    if filter_column2 is not None and filter_column2 not in df.columns:
        raise ValueError(f"filter_column2 '{filter_column2}' 不存在於 DataFrame")

    if value_column not in df.columns:
        raise ValueError(f"value_column '{value_column}' 不存在於 DataFrame")

    if group_columns is not None:
        if len(group_columns) == 0:
            group_columns = None
        else:
            for col in group_columns:
                if col not in df.columns:
                    raise ValueError(f"group column '{col}' 不存在於 DataFrame")
    # 2. Create output directory
    os.makedirs(output_dir,exist_ok=True)
    # 3. Create color map based on group_columns[0]
    if colors is None:
        colors = ["#1F77B4", "#E45756", "#2A9D8F", "#F4A261", "#7B2CBF", "#8AB17D"]
    if len(colors) == 0:
        raise ValueError("colors 不能是空列表")
    if group_columns is not None:
        color_column = group_columns[0]
        color_values = (df[color_column].dropna().drop_duplicates().tolist())
        color_map = {value: colors[index % len(colors)] for index, value in enumerate(color_values)}
    else:
        color_column = None
        color_map = {}
    if filter_column2 is not None:
        secondary_color_map = {
            value: colors[index % len(colors)]
            for index, value in enumerate(filter_values2)
        }
    else:
        secondary_color_map = {}
    # 4. Prepare data for every filter_value
    all_box_data = {}
    # 用來統一所有 subplot 的 group 順序
    group_order = []
    for filter_value in filter_values:
        # Create sub_df
        sub_df = df[df[filter_column] == filter_value].copy()
        if sub_df.empty:
            print(f"[WARNING] {filter_column} = {filter_value} 沒有資料")
            all_box_data[filter_value] = {}
            continue
        group_data = {}
        secondary_values = filter_values2 if filter_column2 is not None else [None]
        for filter_value2 in secondary_values:
            secondary_df = sub_df if filter_column2 is None else sub_df[sub_df[filter_column2] == filter_value2]
            if group_columns is not None:
                secondary_df = secondary_df.copy()
                #secondary_df["_group"] = secondary_df[group_columns].astype(str).agg(" | ".join, axis=1)
                secondary_df["_group"] = (secondary_df[group_columns[0]].astype(str) + "°C | " 
                                          +secondary_df[group_columns[1]].astype(str) + " dBm")
            else:
                secondary_df = secondary_df.assign(_group="All")

            for group_name, group_df in secondary_df.groupby("_group",sort=False):
                values = pd.to_numeric(group_df[value_column],errors="coerce").dropna()
                if len(values) == 0:
                    continue
                if group_columns is not None:
                    color_value = group_df[group_columns[0]].iloc[0]
                else:
                    color_value = None
                group_data[(filter_value2, group_name)] = {"values": values,"color_value": color_value}

                # 建立全域 group order
                if group_name not in group_order:
                    group_order.append(group_name)
        all_box_data[filter_value] = group_data

    # 5. Check data
    if len(group_order) == 0:
        raise ValueError("沒有找到任何有效的 boxplot 資料")

    # 6. Figure size
    n_filter = len(filter_values)
    n_groups = len(group_order)
    if figsize is None:
        width = max(8, n_filter * n_groups * 0.7)
        height = 6
        figsize = (width, height)
    # 7. Create subplots
    fig, axes = plt.subplots(1, n_filter,
                             figsize=figsize,
                             sharey=True,
                             squeeze=False)
    axes = axes[0]
    # 8. Plot every filter_value
    for i, filter_value in enumerate(filter_values):
        ax = axes[i]
        group_data = all_box_data.get(filter_value, {})
        secondary_values = filter_values2 if filter_column2 is not None else [None]
        positions = np.arange(1, n_groups + 1)
        for secondary_index, filter_value2 in enumerate(secondary_values):
            box_data = []
            box_color_values = []
            for group_name in group_order:
                box_info = group_data.get((filter_value2, group_name))
                box_data.append(box_info["values"] if box_info is not None else [])
                box_color_values.append(box_info["color_value"] if box_info is not None else None)

            if not any(len(values) > 0 for values in box_data):
                continue
            position_offset = (secondary_index - (len(secondary_values) - 1) / 2) * 0.3
            bp = ax.boxplot(box_data,
                            positions=positions + position_offset,
                            widths=0.32 if filter_column2 is not None else 0.7,
                            showmeans=True,
                            showfliers=False,
                            meanline=True,
                            boxprops=dict(linewidth=1.5),
                            whiskerprops=dict(linewidth=1.5),
                            capprops=dict(linewidth=1.5),
                            meanprops=dict(linewidth=2, linestyle='-'),
                            medianprops=dict(linewidth=0),
                            patch_artist=filter_column2 is not None and secondary_index > 0)
            for box_index, color_value in enumerate(box_color_values):
                if color_value is None:
                    continue
                color = secondary_color_map[filter_value2] if filter_column2 is not None else color_map[color_value]
                if filter_column2 is not None and secondary_index > 0:
                    bp['boxes'][box_index].set_facecolor(color)
                    bp['boxes'][box_index].set_alpha(0.1)
                bp['boxes'][box_index].set_color(color)
                bp['means'][box_index].set_color(color)
                for line in bp['whiskers'][2 * box_index:2 * box_index + 2] + bp['caps'][2 * box_index:2 * box_index + 2]:
                    line.set_color(color)
        ax.set_xticks(positions, group_order)
        # Title
        #ax.set_title(f"{filter_column} = {filter_value}")
        # X axis
        ax.set_xlabel(f"{filter_column} = {filter_value}")
        plt.setp(ax.get_xticklabels(),
                 rotation=rotation,
                 ha="right",
                 fontsize=8)
        # Y grid
        ax.grid(axis="y",linestyle="--",alpha=0.5)
        # Y axis
        if i == 0:
            ax.set_ylabel(value_column)
        else:
            ax.tick_params(axis="y",labelleft=False)

    # 9. Remove horizontal gaps between subplots
    fig.subplots_adjust(wspace=0)
    # 10. Create legend
    if filter_column2 is not None:
        legend_handles = [
            Patch(facecolor=secondary_color_map[value],
                  edgecolor="black",
                  alpha=0.35,
                  label=str(value))
            for value in filter_values2
        ]
        fig.legend(handles=legend_handles,
                   title=filter_column2,
                   loc="upper right")
    elif group_columns is not None:
        legend_handles = []
        for color_value in color_values:
            if color_value in color_map:
                legend_handles.append(Patch(facecolor=color_map[color_value],
                                            edgecolor="black",
                                            alpha=0.7,
                                            label=str(color_value)))

        if legend_handles:
            fig.legend(handles=legend_handles,
                       title=color_column,
                       loc="upper right")

    # 11. Layout
    # 不使用 tight_layout，
    # 避免破壞 wspace=0 的效果
    fig.subplots_adjust(wspace=0,
                        bottom=0.22,
                        left=0.07,
                        right=0.95,
                        top=0.88)
    fig.suptitle(f"{value_column} comparison (Center vs Surrounding)", y=0.99)
    # 12. Save figure
    filename = (f"{filter_column}_"f"{value_column}_boxplot.png")
    # 移除 Windows 不允許的檔名字元
    filename = "".join(c if c.isalnum() or c in "._-" else "_"for c in filename)
    output_path = os.path.join(output_dir,filename)
    fig.savefig(output_path,dpi=dpi,bbox_inches="tight")
    #plt.close(fig)
    plt.show(block=True)
    print(f"[OK] Boxplot saved:\n"f"{output_path}")
    return None#output_path


# 讀取 Excel
df = pd.read_excel(r"X:\2.Results\260903_boxplot\MRM_SPCM.xlsx")
plot_grouped_boxplots(df=df,
                      filter_column="Bias Voltage(V)",filter_values=[0,1,2],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="FSR(Thz)",
                      output_dir=r"X:\2.Results\260903_boxplot")
plot_grouped_boxplots(df=df,
                      filter_column="Bias Voltage(V)",filter_values=[0,1,2],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="Q factor",
                      output_dir=r"X:\2.Results\260903_boxplot")
plot_grouped_boxplots(df=df,
                      filter_column="Bias Voltage(V)",filter_values=[0,1,2],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="Extinction Ratio",
                      output_dir=r"X:\2.Results\260903_boxplot")

#%%
df = pd.read_excel(r"X:\2.Results\260903_boxplot\MRM_OMA.xlsx")
plot_grouped_boxplots(df=df,
                      filter_column="Detuning(dB)",filter_values=[4,5,6,7],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="OMA(dB)",
                      output_dir=r"X:\2.Results\260903_boxplot")

#%%
df = pd.read_excel(r"X:\2.Results\260903_boxplot\MRM_Best_OMA.xlsx")
plot_grouped_boxplots(df=df,
                      filter_column="repeat",filter_values=[1],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="Best OMA(dB)",
                      output_dir=r"X:\2.Results\260903_boxplot")
#%%
df = pd.read_excel(r"X:\2.Results\260903_boxplot\MRM_SSRF.xlsx")
plot_grouped_boxplots(df=df,
                      filter_column="Detuning(dB)",filter_values=[4,5,6,7],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="Bandwidth(GHz)",
                      output_dir=r"X:\2.Results\260903_boxplot")
#%%
df = pd.read_excel(r"X:\2.Results\260903_boxplot\MRM_ME.xlsx")
plot_grouped_boxplots(df=df,
                      filter_column="Modulation voltage(V)",filter_values=[1,2],
                      filter_column2="position",filter_values2=['Center','Surrounding'],
                      group_columns=["Temperature(C)","power"],
                      value_column="Efficiency(pm/V)",
                      output_dir=r"X:\2.Results\260903_boxplot")

#%%
# 繪製 boxplot
# plot_metric_boxplot_by_die(df=df,
#                             x_col='Bias Voltage(V)',
#                             metric = 'FSR(Thz)',
#                             target_die=[21,23],
#                             target_power=[-10,5],
#                             result_dir=Path(r"X:\2.Results") / save_folder,
#                             y_limits=None)

# plot_metric_boxplot_by_die(df=df,
#                            x_col='Bias Voltage(V)',
#                            metric = 'Q factor',
#                            target_die=[],
#                            target_power=[-10,5],
#                            result_dir=Path(r"X:\2.Results") / save_folder,
#                            y_limits=None)

# plot_metric_boxplot_by_die(df=df,
#                            x_col='Bias Voltage(V)',
#                            metric = 'Extinction Ratio',
#                            target_die=[],
#                            target_power=[-10,5],
#                            result_dir=Path(r"X:\2.Results") / save_folder,
#                            y_limits=None)

# plot_metric_boxplot_by_repeat(df=df,
#                               x_col='Bias Voltage(V)',
#                               metric = 'FSR(Thz)',
#                               target_repeat=1,
#                               target_power=[-10,5],
#                               result_dir=Path(r"X:\2.Results") / save_folder,
#                               y_limits=None)
# plot_metric_boxplot_by_repeat(df=df,
#                               x_col='Bias Voltage(V)',
#                               metric = 'Q factor',
#                               target_repeat=1,
#                               target_power=[-10,5],
#                               result_dir=Path(r"X:\2.Results") / save_folder,
#                               y_limits=None)
# plot_metric_boxplot_by_repeat(df=df,
#                               x_col='Bias Voltage(V)',
#                               metric = 'Extinction Ratio',
#                               target_repeat=1,
#                               target_power=[-10,5],
#                               result_dir=Path(r"X:\2.Results") / save_folder,
#                               y_limits=None)

# plot_metric_boxplot(df=df,
#                     x_col='Bias Voltage(V)',
#                     metric = 'FSR(Thz)',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10,5],
#                     result_dir=Path(r"X:\2.Results") / save_folder)

# plot_metric_boxplot(df=df,
#                     x_col='Bias Voltage(V)',
#                     metric = 'Q factor',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10,5],
#                     result_dir=Path(r"X:\2.Results") / save_folder)

# plot_metric_boxplot(df=df,
#                     x_col='Bias Voltage(V)',
#                     metric = 'Extinction Ratio',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10,5],
#                     result_dir=Path(r"X:\2.Results") / save_folder)

plot_metric_boxplot_by_diegroup(df=df,
                    x_col='Bias Voltage(V)',
                    metric = 'FSR(Thz)',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,9,8],
                    target_power=[-10,5],
                    result_dir=Path(r"X:\2.Results") / save_folder)

plot_metric_boxplot_by_diegroup(df=df,
                    x_col='Bias Voltage(V)',
                    metric = 'Q factor',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,9,8],
                    target_power=[-10,5],
                    result_dir=Path(r"X:\2.Results") / save_folder)

plot_metric_boxplot_by_diegroup(df=df,
                    x_col='Bias Voltage(V)',
                    metric = 'Extinction Ratio',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,9,8],
                    target_power=[-10,5],
                    result_dir=Path(r"X:\2.Results") / save_folder)




# plot_metric_boxplot_by_repeat(df=df_oma,
#                               x_col='Detuning(dB)',
#                               metric = 'OMA(dB)',
#                               target_repeat=1,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot_by_die(df=df_oma,
#                               x_col='Detuning(dB)',
#                               metric = 'OMA(dB)',
#                               target_die=32,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot(df=df_oma,
#                     x_col='Detuning(dB)',
#                     metric = 'OMA(dB)',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10,5],
#                     result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")

plot_metric_boxplot_by_diegroup(df=df_oma,
                    x_col='Detuning(dB)',
                    metric = 'OMA(dB)',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,8],
                    target_power=[-10,5],
                    result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")



columns = ['cage','Device','die','repeat','power','Best OMA wavelength(nm)','Best OMA(dB)']
df_best = pd.DataFrame(best, columns=columns)
# plot_metric_boxplot_by_repeat(df=df_best,
#                               x_col='Device',
#                               metric = 'Best OMA(dB)',
#                               target_repeat=1,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot_by_die(df=df_best,
#                               x_col='Device',
#                               metric = 'Best OMA(dB)',
#                               target_die=32,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot(df=df_best,
#                         x_col='Device',
#                         metric = 'Best OMA(dB)',
#                         target_die=32,
#                         target_repeat=1,
#                         target_power=[-10,5],
#                         result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")

plot_metric_boxplot_by_diegroup(df=df_best,
                        x_col='Device',
                        metric = 'Best OMA(dB)',
                        die_group1=[21,23,32,41,43], 
                        die_group2=[1,2,17,36,55,57,59,47,28,8],
                        target_power=[-10,5],
                        result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")



# plot_metric_boxplot_by_die(df=df_ssrf,
#                            x_col='Detuning(dB)',
#                            metric = 'Bandwidth(GHz)',
#                            target_die=32,
#                            target_power=[-10, 5],
#                            result_dir=Path(r"X:\2.Results\260811-2_boxplot"),
#                            y_limits=None)

# plot_metric_boxplot_by_repeat(df=df_ssrf,
#                            x_col='Detuning(dB)',
#                            metric = 'Bandwidth(GHz)',
#                            target_repeat=1,
#                            target_power=[-10, 5],
#                            result_dir=Path(r"X:\2.Results\260811-2_boxplot"),
#                            y_limits=None)

# plot_metric_boxplot(df=df_ssrf,
#                     x_col='Detuning(dB)',
#                     metric = 'Bandwidth(GHz)',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10, 5],
#                     result_dir=Path(r"X:\2.Results\260811-2_boxplot"))

plot_metric_boxplot_by_diegroup(df=df_ssrf,
                    x_col='Detuning(dB)',
                    metric = 'Bandwidth(GHz)',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,9,8],
                    target_power=[-10, 5],
                    result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")




# plot_metric_boxplot_by_repeat(df=df_me,
#                               x_col='Modulation voltage(V)',
#                               metric = 'Efficiency(pm/V)',
#                               target_repeat=1,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot_by_die(df=df_me,
#                               x_col='Modulation voltage(V)',
#                               metric = 'Efficiency(pm/V)',
#                               target_die=32,
#                               target_power=[-10, 5],
#                               result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot",
#                               y_limits=None)

# plot_metric_boxplot(df=df_me,
#                     x_col='Modulation voltage(V)',
#                     metric = 'Efficiency(pm/V)',
#                     target_die=32,
#                     target_repeat=1,
#                     target_power=[-10, 5],
#                     result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")

plot_metric_boxplot_by_diegroup(df=df_me,
                    x_col='Modulation voltage(V)',
                    metric = 'Efficiency(pm/V)',
                    die_group1=[21,23,32,41,43], 
                    die_group2=[1,2,17,36,55,57,59,47,28,8],
                    target_power=[-10, 5],
                    result_dir=Path(r"X:\2.Results") / f"{measure_name}_boxplot")
# %% test asls_baseline
import numpy as np
from scipy import sparse
from scipy.sparse.linalg import spsolve


def asls_baseline(y, lam=1e6, p=0.01, n_iter=20):

    y = np.asarray(y, dtype=float)
    n = len(y)

    # Second-order difference matrix
    D = sparse.diags([1, -2, 1],[0, 1, 2],shape=(n - 2, n),format="csr")
    DTD = D.T @ D
    # Initial weights
    w = np.ones(n)
    for _ in range(n_iter):
        # Weight matrix
        W = sparse.diags(w,offsets=0,shape=(n, n),format="csr")
        # System matrix
        Z = W + lam * DTD
        # 確保是 CSR
        Z = Z.tocsr()
        # Solve
        baseline = spsolve(Z, w * y)
        # Asymmetric weighting
        w = np.where(y < baseline,p,1 - p)
    return baseline

measure_name = '260807_AMD_cage158_D4'
with DatabaseAPI(db_path) as db:
    sessions = db.select_session(measure_name = measure_name)
    for session in sessions[:1]:
        session_id = session['session_id']
        session_info = db.select_session_info(session_id)
        repeat = session_info['session_idx']
        die = session_info['die']
        cage = session_info['cage']
        device = session_info['device']

        spcm_infos = db.select_rawdata_files(session_id, data_type='SPCM')
        for info in spcm_infos:
            spcm_path = db_path.parent / info['file_path']
            spcm_path = read_spectrum_all(spcm_path)['average_il']
            sg = signal.savgol_filter(spcm_path[:,1], len(spcm_path[:,1]),3)
            baseline = asls_baseline(spcm_path[:,1], lam=2e10, p=0.01, n_iter=20)
            plt.plot(spcm_path[:,0], spcm_path[:,1])
            #plt.plot(spcm[:,0], sg)
            plt.plot(spcm_path[:,0], baseline)
        plt.show()


# %%
with DatabaseAPI(db_path) as db:
    sql_measure_analysis = """
           SELECT
                ms.session_id,
                a.analysis_id,
                d.wafer,
                d.die,
                d.cage,
                d.device,
                ms.session_idx

            FROM Measurement m

            JOIN DUT d
                ON d.DUT_id = m.DUT_id

            JOIN MeasureSession ms
               ON ms.measure_id = m.measure_id

            JOIN Analyses a
              ON a.session_id = ms.session_id

            WHERE m.measure_name = ?
             AND a.analysis_type = ?

            ORDER BY
                d.wafer,
                d.die,
                ms.session_idx,
                a.analysis_id;"""
    metric_keys = ["FSR(nm)", "FSR(THz)", "FWHM(nm)", "FWHM(GHz)",'Q factor']
    placeholders = ",".join("?" for _ in metric_keys)
    sql_metric = f"""
                    SELECT
                        s.analysis_id,
                        s.data_id,

                        f.feature_id,
                        f.feature_type,
                        f.feature_idx,

                        fm.metric_key,
                        fm.metric_value,
                        fm.metric_unit

                    FROM AnalysisSources s

                    JOIN Features f
                        ON f.analysis_id = s.analysis_id

                    JOIN FeatureMetrics fm
                        ON fm.feature_id = f.feature_id

                    WHERE s.analysis_id = ?
                        AND fm.metric_key IN ({placeholders})

                    ORDER BY
                        s.data_id,
                        f.feature_type,
                        f.feature_idx;"""
    sql_data= """
                SELECT
                    r.data_id,

                    -- OpticalInfo
                    oi.input_channel,
                    oi.output_channel,
                    oi.input_power,
                    oi.tls_power,
                    oi.attenuation,
                    oi.wavelengthStart,
                    oi.wavelengthStop,
                    oi.sweepRate,

                    -- Conditions
                    c.condition_id,
                    c.setting_parameters,
                    c.setting_value,
                    c.parameters_unit,

                    -- ElectricInfo
                    ei.element,
                    ei.channel,
                    ei.set_mode,
                    ei.set_value,

                    -- RFInfo
                    rf.modulation,
                    rf.pattern,
                    rf.baud_rate,
                    rf.vpp,

                    -- AnotherInfo
                    ai.info_key,
                    ai.info_value

                FROM RawDataFiles r

                LEFT JOIN OpticalInfo oi
                    ON oi.data_id = r.data_id

                LEFT JOIN Conditions c
                    ON c.data_id = r.data_id

                LEFT JOIN ElectricInfo ei
                    ON ei.data_id = r.data_id

                LEFT JOIN RFInfo rf
                    ON rf.data_id = r.data_id

                LEFT JOIN AnotherInfo ai
                    ON ai.data_id = r.data_id

                WHERE r.data_id = ?;"""

    df = []
    res_measure_analysis = db.query(sql_measure_analysis, ('260901_AMD_cage158_D4_85C', 'MRM_SPCM_analysis'))
    for res_ma in res_measure_analysis:
        analysis_id = res_ma['analysis_id']
        res_metric = db.query(sql_metric, (analysis_id, *metric_keys))
        metric = {}
        for res_m in res_metric:
            data_id = res_m['data_id']
            feature_id = res_m['feature_id']
            feature_idx = res_m['feature_idx']
            if (data_id, feature_id, feature_idx) not in metric:
                metric[(data_id, feature_id, feature_idx)] = res_ma
                for k in ['analysis_id','data_id','feature_id','feature_type','feature_idx']:
                    metric[(data_id, feature_id, feature_idx)][k] = res_m[k]
                metric[(data_id, feature_id, feature_idx)][res_m['metric_key']] = res_m['metric_value']
            else:
                metric[(data_id, feature_id, feature_idx)][res_m['metric_key']] = res_m['metric_value']
        for key, value in metric.items():
            data_id, feature_id, feature_idx = key
            res_data = db.query(sql_data, (data_id,))
            for i, row in enumerate(res_data):
                if row['setting_parameters'] == 'temperature':
                    row['temperature'] = row['setting_value']
                    for key in ['setting_parameters', 'setting_value','parameters_unit']:
                        row.pop(key, None)
                value.update(row)
            # df.append(value)
    # df = pd.DataFrame(df)
    # columns = ['wafer','die', 'cage', 'device','session_idx','feature_idx','input_power',
    #            'input_channel', 'output_channel','setting_value',
    #            'feature_idx',
    #            'FSR(THz)',
    #            'FSR(nm)',
    # ]

# df[columns].to_excel(
#     'output.xlsx',
#     index=False
# )


# %%
