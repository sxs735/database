#%%
import re
import sys
import traceback
import csv
from pathlib import Path
from scipy.signal import savgol_filter

import matplotlib.pyplot as plt
import numpy as np
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from database_api import DatabaseAPI
from analysis import read_spectrum_lite


GROUP_TF = {('1', '1'): ['Group1', 'Group6'],#s15
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


class PlotVariationGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Plot Variation (PySide6)")
        self.resize(920, 620)

        self.db_path_edit = QLineEdit(str(Path(r"X:\1_Database") / "DataBase.db"))
        self.measure_combo = QComboBox()
        self.save_folder_edit = QLineEdit(str(Path(r"X:\2.Results")))
        self.smooth_window_spin = QSpinBox()
        self.smooth_window_spin.setRange(5, 999)
        self.smooth_window_spin.setSingleStep(2)
        self.smooth_window_spin.setValue(31)
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)

        self._build_ui()
        self.refresh_measure_names()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        # DB path
        db_row = QHBoxLayout()
        db_row.addWidget(QLabel("DB Path:"))
        db_row.addWidget(self.db_path_edit, 1)
        db_browse_btn = QPushButton("Browse...")
        db_browse_btn.clicked.connect(self.browse_db_path)
        refresh_measure_btn = QPushButton("Refresh measure_name")
        refresh_measure_btn.clicked.connect(self.refresh_measure_names)
        db_row.addWidget(db_browse_btn)
        db_row.addWidget(refresh_measure_btn)
        root.addLayout(db_row)

        # measure_name
        measure_row = QHBoxLayout()
        measure_row.addWidget(QLabel("measure_name:"))
        measure_row.addWidget(self.measure_combo, 1)
        root.addLayout(measure_row)

        # save folder
        save_row = QHBoxLayout()
        save_row.addWidget(QLabel("save_folder:"))
        save_row.addWidget(self.save_folder_edit, 1)
        save_browse_btn = QPushButton("Browse...")
        save_browse_btn.clicked.connect(self.browse_save_folder)
        save_row.addWidget(save_browse_btn)
        root.addLayout(save_row)

        # smooth window
        smooth_row = QHBoxLayout()
        smooth_row.addWidget(QLabel("smooth_window:"))
        smooth_row.addWidget(self.smooth_window_spin)
        smooth_row.addStretch(1)
        root.addLayout(smooth_row)

        run_row = QHBoxLayout()
        run_row.addStretch(1)
        run_btn = QPushButton("Run Plot")
        run_btn.clicked.connect(self.run_plot)
        run_row.addWidget(run_btn)
        root.addLayout(run_row)

        root.addWidget(QLabel("Log:"))
        root.addWidget(self.log_box, 1)

    def log(self, msg: str):
        self.log_box.append(msg)

    def browse_db_path(self):
        selected, _ = QFileDialog.getOpenFileName(
            self,
            "Select DB file",
            self.db_path_edit.text().strip() or "DataBase.db",
            "SQLite DB (*.db);;All Files (*)",
        )
        if selected:
            self.db_path_edit.setText(selected)
            self.refresh_measure_names()

    def browse_save_folder(self):
        selected = QFileDialog.getExistingDirectory(
            self,
            "Select save folder",
            self.save_folder_edit.text().strip() or str(Path.home()),
        )
        if selected:
            self.save_folder_edit.setText(selected)

    def refresh_measure_names(self):
        self.measure_combo.clear()
        db_path = Path(self.db_path_edit.text().strip())
        if not db_path.exists():
            self.log(f"[WARN] DB 不存在: {db_path}")
            return

        try:
            with DatabaseAPI(db_path) as db:
                measurements = db.select_measurements()

            names = sorted({m.get('measure_name') for m in measurements if m.get('measure_name')})
            if len(names) == 0:
                self.log("[INFO] 查無 measure_name")
                return

            self.measure_combo.addItems(names)
            self.log(f"[INFO] 已載入 {len(names)} 個 measure_name")
        except Exception as e:
            self.log(f"[ERROR] 載入 measure_name 失敗: {e}")

    def run_plot(self):
        db_path = Path(self.db_path_edit.text().strip())
        measure_name = self.measure_combo.currentText().strip()
        save_folder = Path(self.save_folder_edit.text().strip())
        smooth_window = int(self.smooth_window_spin.value())

        # savgol_filter 需要奇數 window 且大於 polyorder(=3)
        if smooth_window <= 3:
            smooth_window = 5
        if smooth_window % 2 == 0:
            smooth_window += 1

        if not db_path.exists():
            QMessageBox.critical(self, "Error", f"DB 不存在:\n{db_path}")
            return

        if not measure_name:
            QMessageBox.warning(self, "Warning", "請先選擇 measure_name")
            return

        try:
            save_folder.mkdir(parents=True, exist_ok=True)

            with DatabaseAPI(db_path) as db:
                sessions = db.select_session(measure_name=measure_name)
                if len(sessions) == 0:
                    QMessageBox.warning(self, "Warning", f"找不到 measure_name: {measure_name}")
                    return

                group = {}
                for session in sessions:
                    raw_files = db.select_rawdata_files(session_id=session['session_id'], data_type='SPCM')
                    if len(raw_files) == 0:
                        continue
                    spcm_info = raw_files[0]

                    opt_ch = (spcm_info['optical_input_channel'], spcm_info['optical_output_channel'])
                    if opt_ch not in group:
                        group[opt_ch] = [spcm_info]
                    else:
                        group[opt_ch].append(spcm_info)

                if len(group) == 0:
                    QMessageBox.warning(self, "Warning", "此 measure_name 找不到 SPCM 資料")
                    return

                fig2,ax3 = plt.subplots(1, 1, figsize=(14, 10))
                smooth_data = {}
                saved_count = 0
                for opt_ch in list(group.keys()):
                    if opt_ch not in GROUP_TF:
                        self.log(f"[SKIP] 未定義群組映射: {opt_ch}")
                        continue

                    group1_name = GROUP_TF[opt_ch][0]
                    group2_name = GROUP_TF[opt_ch][1]
                    draw_ax1 = group1_name != 'NA'
                    draw_ax2 = group2_name != 'NA' and group1_name != 'RL15'

                    repeat = []
                    idx = []
                    spcm_data = None

                    for spcm_info in group[opt_ch]:
                        path = Path(db.db_path).parent / spcm_info['file_path']
                        #path的檔名部分，SPCMs以SPCM取代
                        _, spcm_data = read_spectrum_lite(path)

                        m = re.search(r'#(\d+)', str(spcm_info['file_path']))
                        ind = int(m.group(1)) if m else (len(idx) + 1)
                        idx.append(ind)

                        data = spcm_data[:, [0, 1, 3]]
                        repeat.append(data)

                    if len(repeat) == 0 or spcm_data is None:
                        self.log(f"[SKIP] 無有效重複資料: {opt_ch}")
                        continue

                    repeat = np.array(repeat)
                    wavelength = spcm_data[:, 0]
                    average = np.nanmean(repeat, axis=0)
                    diff = repeat - average

                    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5), sharex=True)
                    variation1 = []
                    variation2 = []

                    local_window = min(smooth_window, average.shape[0])
                    if local_window % 2 == 0:
                        local_window -= 1
                    if local_window <= 3:
                        smooth_avg1 = average[:, 1]
                        smooth_avg2 = average[:, 2]
                    else:
                        smooth_avg1 = savgol_filter(average[:, 1], local_window, 3)
                        smooth_avg2 = savgol_filter(average[:, 2], local_window, 3)
                    if draw_ax1:
                        ax3.plot(wavelength, -smooth_avg1, c='r', zorder=300)
                    if draw_ax2:
                        ax3.plot(wavelength, -smooth_avg2, c='r', zorder=300)
                    smooth_data['wavelength'] = wavelength
                    if draw_ax1:
                        smooth_data[group1_name] = smooth_avg1
                    if draw_ax2:
                        smooth_data[group2_name] = smooth_avg2
                    for ind, i in zip(idx, range(repeat.shape[0])):
                        if draw_ax1:
                            ax1.plot(wavelength, -diff[i, :, 1], label=f"#{ind}")
                        idx1308 = np.argmin(np.abs(wavelength-1308))
                        if draw_ax1:
                            variation1.append(diff[i, idx1308, 1])
                            if group1_name not in ['RL15','R16'] and ind == 1:
                                ax3.plot(wavelength, -repeat[i, :, 1], label=group1_name)
                        #variation1.append(np.max(diff[i, :, 1]))
                        #variation1.append(np.min(diff[i, :, 1]))
                        if draw_ax2:
                            ax2.plot(wavelength, -diff[i, :, 2], label=f"#{ind}")
                            variation2.append(diff[i, idx1308, 2])
                            if group2_name not in ['R13'] and ind == 1:
                                ax3.plot(wavelength, -repeat[i, :, 2], label=group2_name)
                        #variation2.append(np.max(diff[i, :, 2]))
                        #variation2.append(np.min(diff[i, :, 2]))

                    if draw_ax1 and len(variation1) > 0:
                        ax1.set_title(f"{group1_name}, Variation: {(np.max(variation1) - np.min(variation1)):.3f}dB_@1308nm")
                        ax1.set_xlabel("Wavelength (nm)")
                        ax1.set_ylabel("Loss (dB)")
                        ax1.set_ylim(-1, 1)
                        ax1.legend()
                    else:
                        ax1.set_visible(False)

                    if draw_ax2 and len(variation2) > 0:
                        ax2.set_title(f"{group2_name}, Variation: {(np.max(variation2) - np.min(variation2)):.3f}dB_@1308nm")
                        ax2.set_xlabel("Wavelength (nm)")
                        ax2.set_ylabel("Loss (dB)")
                        ax2.tick_params(axis='y', labelleft=True)
                        ax2.set_ylim(-1, 1)
                        ax2.legend()
                    else:
                        ax2.set_visible(False)
                    fig.tight_layout()

                    save_path = save_folder / f"{group1_name}_{group2_name}_variation.png"
                    fig.savefig(save_path)
                    plt.close(fig)
                    saved_count += 1
                    self.log(f"[OK] Saved: {save_path}")
                
                ax3.set_title(f"Insertion Loss for 21 Loopback")
                ax3.set_xlabel("Wavelength (nm)")
                ax3.set_ylabel("Loss (dB)")
                ax3.legend()

                if len(smooth_data) > 0:
                    csv_path = save_folder / "smooth_data.csv"
                    headers = list(smooth_data.keys())
                    columns = [np.asarray(smooth_data[k]).reshape(-1) for k in headers]
                    row_count = min(len(col) for col in columns)

                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([str(h) for h in headers])
                        for r in range(row_count):
                            writer.writerow([col[r] for col in columns])

                    self.log(f"[OK] Saved: {csv_path}")

                save_path2 = save_folder / f"rawdata_average.png"
                fig2.savefig(save_path2)
                #plt.show()
                plt.close(fig2)

            QMessageBox.information(self, "Done", f"完成，輸出 {saved_count} 張圖")

        except Exception as e:
            self.log(f"[ERROR] {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Error", str(e))


def main():
    app = QApplication(sys.argv)
    win = PlotVariationGUI()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

# %%
