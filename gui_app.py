from pathlib import Path
import threading
import traceback
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from database_api import DatabaseAPI


class DatabaseToolGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Measurement Database GUI")
        self.root.geometry("900x680")

        self._build_variables()
        self._build_layout()

    def _build_variables(self):
        self.db_path_var = tk.StringVar(value=r"D:\Data\1_DataBase\DataBase.db")
        self.folder_path_var = tk.StringVar(value=r"D:\Data\1_DataBase\processing")
        self.cage_var = tk.StringVar(value="")
        self.measure_name_var = tk.StringVar(value="")

        self.do_backup_var = tk.BooleanVar(value=True)
        self.do_import_var = tk.BooleanVar(value=False)

        self.run_spcm_var = tk.BooleanVar(value=True)
        self.run_oma_var = tk.BooleanVar(value=True)
        self.run_tuning_var = tk.BooleanVar(value=True)
        self.run_ssrf_var = tk.BooleanVar(value=True)
        self.run_loss_var = tk.BooleanVar(value=False)
        self.run_ssrf_mtk_var = tk.BooleanVar(value=False)

        self.oma_start_var = tk.StringVar(value="1308")
        self.oma_end_var = tk.StringVar(value="1315")
        self.tuning_start_var = tk.StringVar(value="1305")
        self.tuning_end_var = tk.StringVar(value="1315")

    def _build_layout(self):
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill="both", expand=True)

        # Paths
        path_box = ttk.LabelFrame(frame, text="Path Settings", padding=10)
        path_box.pack(fill="x")

        ttk.Label(path_box, text="DB Path").grid(row=0, column=0, sticky="w")
        ttk.Entry(path_box, textvariable=self.db_path_var, width=90).grid(row=0, column=1, padx=6, sticky="ew")
        ttk.Button(path_box, text="Browse", command=self._pick_db_path).grid(row=0, column=2)

        ttk.Label(path_box, text="Import Folder").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(path_box, textvariable=self.folder_path_var, width=90).grid(row=1, column=1, padx=6, pady=(8, 0), sticky="ew")
        ttk.Button(path_box, text="Browse", command=self._pick_folder_path).grid(row=1, column=2, pady=(8, 0))

        path_box.columnconfigure(1, weight=1)

        # Filters
        filter_box = ttk.LabelFrame(frame, text="Session Filters", padding=10)
        filter_box.pack(fill="x", pady=(10, 0))

        ttk.Label(filter_box, text="cage").grid(row=0, column=0, sticky="w")
        ttk.Entry(filter_box, textvariable=self.cage_var, width=30).grid(row=0, column=1, padx=(6, 20), sticky="w")
        ttk.Label(filter_box, text="measure_name").grid(row=0, column=2, sticky="w")
        ttk.Entry(filter_box, textvariable=self.measure_name_var, width=40).grid(row=0, column=3, padx=6, sticky="w")

        # Options
        option_box = ttk.LabelFrame(frame, text="Import / Backup", padding=10)
        option_box.pack(fill="x", pady=(10, 0))
        ttk.Checkbutton(option_box, text="Backup DB before run", variable=self.do_backup_var).pack(anchor="w")
        ttk.Checkbutton(option_box, text="Import folder before analysis", variable=self.do_import_var).pack(anchor="w")

        # Analyses
        analysis_box = ttk.LabelFrame(frame, text="Analysis Options", padding=10)
        analysis_box.pack(fill="x", pady=(10, 0))

        ttk.Checkbutton(analysis_box, text="MRM_SPCM", variable=self.run_spcm_var).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(analysis_box, text="MRM_OMA", variable=self.run_oma_var).grid(row=0, column=1, sticky="w", padx=(20, 0))
        ttk.Label(analysis_box, text="start").grid(row=0, column=2, padx=(20, 4))
        ttk.Entry(analysis_box, textvariable=self.oma_start_var, width=8).grid(row=0, column=3)
        ttk.Label(analysis_box, text="end").grid(row=0, column=4, padx=(10, 4))
        ttk.Entry(analysis_box, textvariable=self.oma_end_var, width=8).grid(row=0, column=5)

        ttk.Checkbutton(analysis_box, text="MRM_tuning", variable=self.run_tuning_var).grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Label(analysis_box, text="start").grid(row=1, column=2, padx=(20, 4), pady=(8, 0))
        ttk.Entry(analysis_box, textvariable=self.tuning_start_var, width=8).grid(row=1, column=3, pady=(8, 0))
        ttk.Label(analysis_box, text="end").grid(row=1, column=4, padx=(10, 4), pady=(8, 0))
        ttk.Entry(analysis_box, textvariable=self.tuning_end_var, width=8).grid(row=1, column=5, pady=(8, 0))

        ttk.Checkbutton(analysis_box, text="MRM_SSRF", variable=self.run_ssrf_var).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Checkbutton(analysis_box, text="Loss", variable=self.run_loss_var).grid(row=2, column=1, sticky="w", padx=(20, 0), pady=(8, 0))
        ttk.Checkbutton(analysis_box, text="MRM_SSRF_MTK", variable=self.run_ssrf_mtk_var).grid(row=2, column=2, columnspan=2, sticky="w", padx=(20, 0), pady=(8, 0))

        # Run button
        self.run_btn = ttk.Button(frame, text="Run", command=self._run_async)
        self.run_btn.pack(anchor="e", pady=(10, 0))

        # Logs
        log_box = ttk.LabelFrame(frame, text="Log", padding=8)
        log_box.pack(fill="both", expand=True, pady=(10, 0))
        self.log_text = tk.Text(log_box, height=18, wrap="word")
        self.log_text.pack(fill="both", expand=True)

    def _pick_db_path(self):
        path = filedialog.asksaveasfilename(
            title="Select DB file",
            defaultextension=".db",
            filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")],
            initialfile=Path(self.db_path_var.get()).name if self.db_path_var.get() else "DataBase.db",
        )
        if path:
            self.db_path_var.set(path)

    def _pick_folder_path(self):
        path = filedialog.askdirectory(title="Select import folder")
        if path:
            self.folder_path_var.set(path)

    def _log(self, message: str):
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.root.update_idletasks()

    def _set_running(self, running: bool):
        self.run_btn.configure(state="disabled" if running else "normal")

    def _run_async(self):
        worker = threading.Thread(target=self._run_pipeline, daemon=True)
        worker.start()

    def _parse_int(self, value: str, name: str) -> int:
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be integer, got: {value}") from exc

    def _run_pipeline(self):
        self._set_running(True)
        try:
            db_path = Path(self.db_path_var.get().strip())
            folder_path = Path(self.folder_path_var.get().strip())
            cage = self.cage_var.get().strip() or None
            measure_name = self.measure_name_var.get().strip() or None

            oma_start = self._parse_int(self.oma_start_var.get().strip(), "OMA start")
            oma_end = self._parse_int(self.oma_end_var.get().strip(), "OMA end")
            tuning_start = self._parse_int(self.tuning_start_var.get().strip(), "Tuning start")
            tuning_end = self._parse_int(self.tuning_end_var.get().strip(), "Tuning end")

            db_path.parent.mkdir(parents=True, exist_ok=True)

            with DatabaseAPI(db_path) as db:
                if self.do_backup_var.get() and db_path.exists():
                    backup = db.backup_database()
                    self._log(f"[OK] Backup created: {backup}")

                if self.do_import_var.get():
                    if not folder_path.exists():
                        raise FileNotFoundError(f"Import folder not found: {folder_path}")
                    self._log(f"[RUN] Importing folder: {folder_path}")
                    db.import_from_measurement_folder(folder_path, schema_file="schema.sql")
                    self._log("[OK] Import done")

                self._log("[RUN] Query sessions...")
                sessions = db.select_session(measure_name=measure_name, cage=cage)
                self._log(f"[OK] Found {len(sessions)} sessions")
                if len(sessions) == 0:
                    self._log("[INFO] No session found. Please adjust cage / measure_name.")
                    return

                for idx, session in enumerate(sessions, start=1):
                    session_id = session["session_id"]
                    self._log(f"[RUN] Session {idx}/{len(sessions)} - session_id={session_id}")

                    if self.run_spcm_var.get():
                        db.MRM_SPCM_analysis_by_session(session_id, commit=False)
                        self._log("  - MRM_SPCM done")

                    if self.run_oma_var.get():
                        db.MRM_OMA_analysis_by_session(session_id, start=oma_start, end=oma_end, commit=False)
                        self._log("  - MRM_OMA done")

                    if self.run_tuning_var.get():
                        db.MRM_tuning_analysis_by_session(session_id, start=tuning_start, end=tuning_end, commit=False)
                        self._log("  - MRM_tuning done")

                    if self.run_ssrf_var.get():
                        db.MRM_SSRF_analysis_by_session(session_id, commit=False)
                        self._log("  - MRM_SSRF done")

                    if self.run_loss_var.get():
                        db.Loss_analysis_by_session(session_id, commit=False)
                        self._log("  - Loss done")

                    if self.run_ssrf_mtk_var.get():
                        db.MRM_SSRF_MTK_analysis_by_session(session_id, commit=False)
                        self._log("  - MRM_SSRF_MTK done")

                db.conn.commit()
                self._log("[OK] All selected analyses completed and committed")

            self.root.after(0, lambda: messagebox.showinfo("Done", "Pipeline completed."))

        except Exception as exc:
            self._log(f"[ERROR] {exc}")
            self._log(traceback.format_exc())
            self.root.after(0, lambda: messagebox.showerror("Error", str(exc)))
        finally:
            self._set_running(False)


def main():
    root = tk.Tk()
    app = DatabaseToolGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
