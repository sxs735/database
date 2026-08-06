#%%
import matplotlib.pyplot as plt
import csv
import numpy as np
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

def tofloat(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def read_spectrum_all(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        mode = None
        header = []
        Min_Max = []
        Mueller = []
        Avg = []
        PDL = []
        TE_TM = []
        for i,row in enumerate(reader):
            #print(row)
            if '=== Min' in row and mode != 'min_max':
                mode = 'min_max'
            elif '=== Average IL (TLS 0) ===' in row and mode != 'average_il':
                mode = 'average_il'
            elif '=== Mueller Row 1 (TLS 0) ===' in row and mode != 'mueller':
                mode = 'mueller'
            elif '=== PDL (TLS 0) ===' in row and mode != 'pdl':
                mode = 'pdl'
            elif '=== TE' in row and mode != 'te_tm':
                mode = 'te_tm'
            elif mode == 'min_max':
                Min_Max += [[tofloat(value) for value in row]]
            elif mode == 'average_il':
                Avg += [[tofloat(value) for value in row]]
            elif mode == 'mueller':
                Mueller += [[tofloat(value) for value in row]]
            elif mode == 'pdl':
                PDL += [[tofloat(value) for value in row]]
            elif mode == 'te_tm':
                TE_TM += [[tofloat(value) for value in row]]
            else:
                header += [row]
        data = {'min_max': np.array(Min_Max),
                'average_il': np.array(Avg),
                'mueller': np.array(Mueller),
                'pdl': np.array(PDL),
                'te_tm': np.array(TE_TM),
                'header': header}
    return data

DATATYPES = ["min_max", "average_il", "mueller", "pdl", "te_tm"]


class SpectrumViewerGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Spectrum Data Viewer")
        self.root.geometry("760x460")

        self.selected_paths = []
        self.datatype_var = tk.StringVar(value="average_il")
        self.cols_var = tk.StringVar(value="1,3,5")

        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="Datatype:").pack(side="left")
        datatype_box = ttk.Combobox(top, textvariable=self.datatype_var, values=DATATYPES, width=16, state="readonly")
        datatype_box.pack(side="left", padx=(8, 16))

        ttk.Label(top, text="Cols:").pack(side="left")
        ttk.Entry(top, textvariable=self.cols_var, width=16).pack(side="left", padx=(8, 16))

        ttk.Button(top, text="載入多個檔案", command=self.load_files).pack(side="left", padx=4)
        ttk.Button(top, text="清空清單", command=self.clear_files).pack(side="left", padx=4)
        ttk.Button(top, text="繪圖", command=self.plot_selected).pack(side="left", padx=4)

        mid = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        mid.pack(fill="both", expand=True)

        ttk.Label(mid, text="已選檔案:").pack(anchor="w", pady=(0, 6))
        self.file_list = tk.Listbox(mid, selectmode="extended")
        self.file_list.pack(fill="both", expand=True)

        self.status_var = tk.StringVar(value="尚未載入檔案")
        ttk.Label(self.root, textvariable=self.status_var, padding=(12, 0, 12, 12)).pack(anchor="w")

    def load_files(self):
        paths = filedialog.askopenfilenames(
            title="選擇光譜檔案",
            filetypes=[("Spectrum files", "*.csv *.txt *.s2p"), ("All files", "*.*")],
        )
        if not paths:
            return

        existing = set(self.selected_paths)
        for p in paths:
            if p not in existing:
                self.selected_paths.append(p)

        self._refresh_listbox()

    def clear_files(self):
        self.selected_paths.clear()
        self._refresh_listbox()

    def _refresh_listbox(self):
        self.file_list.delete(0, tk.END)
        for p in self.selected_paths:
            self.file_list.insert(tk.END, p)
        self.status_var.set(f"已載入 {len(self.selected_paths)} 個檔案")

    def plot_selected(self):
        if not self.selected_paths:
            messagebox.showwarning("沒有資料", "請先載入至少一個檔案")
            return

        raw_cols = self.cols_var.get().strip()
        try:
            cols = [int(x.strip()) for x in raw_cols.split(",") if x.strip() != ""]
            if len(cols) == 0:
                raise ValueError("請至少輸入一個欄位")
        except Exception:
            messagebox.showerror("欄位格式錯誤", "Cols 請輸入逗號分隔整數，例如: 1,3,5")
            return

        datatype = self.datatype_var.get()
        plt.figure(figsize=(11, 5.5))

        plotted = 0
        errors = []
        missing_datatype_files = []
        for p in self.selected_paths:
            try:
                data = read_spectrum_all(p)
                arr = data.get(datatype)
                if arr is None or arr.size == 0 or arr.ndim != 2 or arr.shape[1] < 2:
                    missing_datatype_files.append(Path(p).name)
                    raise ValueError(f"{datatype} 無有效資料")
            
                wavelength = arr[:, 0] * 1E9
                
                for col in cols:
                    if col < 0 or col >= arr.shape[1]:
                        raise ValueError(f"col {col} 超出欄位範圍 (0~{arr.shape[1]-1})")
                    loss = -arr[:, col]
                    
                    valid = ~np.isnan(wavelength) & ~np.isnan(loss)
                    if not np.any(valid):
                        raise ValueError("資料全為 NaN")

                    plt.plot(wavelength[valid], loss[valid], alpha=0.8, linewidth=1.0, label=Path(p).stem + f" (col {col})")
                plotted += 1
            except Exception as e:
                errors.append(f"{Path(p).name}: {e}")

        if plotted == 0:
            plt.close()
            messagebox.showerror("繪圖失敗", "所有檔案都無法繪圖\n" + "\n".join(errors[:8]))
            return

        plt.xlabel("Wavelength (nm)")
        plt.ylabel(f"{datatype} (dB)")
        plt.title(f"Spectrum Overlay - {datatype}")
        plt.grid(alpha=0.3)
        plt.legend(fontsize=8)
        plt.tight_layout()
        plt.show()

        if missing_datatype_files:
            messagebox.showwarning(
                "找不到選定的 datatype",
                f"以下檔案沒有可用的 {datatype} 資料:\n" + "\n".join(missing_datatype_files[:8]),
            )

        if errors:
            messagebox.showwarning("部分檔案失敗", "以下檔案未成功繪圖:\n" + "\n".join(errors[:8]))


if __name__ == "__main__":
    root = tk.Tk()
    app = SpectrumViewerGUI(root)
    root.mainloop()