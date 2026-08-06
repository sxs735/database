from pathlib import Path
import traceback
import sys
from datetime import datetime
import shutil

from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QAbstractItemView,
    QCheckBox,
    QProgressDialog,
)
from PySide6.QtCore import Qt

from database_api import DatabaseAPI


class ImportDatabaseGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Import/Export Data to DataBase")
        self.resize(900, 650)

        self.db_path_edit = QLineEdit(str(Path(r"X:\1_Database") / "DataBase.db"))
        self.local_path_edit = QLineEdit(str(Path(r"X:\1_Database\processing")))
        self.backup_checkbox = QCheckBox("Backup")
        self.backup_checkbox.setChecked(True)

        self.folder_list_widget = QListWidget()
        self.folder_list_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.folder_list_widget.setStyleSheet(
            """
            QListWidget::item:selected {
                background-color: #2D7DFF;
                color: white;
            }
            QListWidget::item:selected:!active {
                background-color: #2D7DFF;
                color: white;
            }
            """
        )

        self.measure_list_widget = QListWidget()
        self.measure_list_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.measure_list_widget.setStyleSheet(
            """
            QListWidget::item:selected {
                background-color: #2D7DFF;
                color: white;
            }
            QListWidget::item:selected:!active {
                background-color: #2D7DFF;
                color: white;
            }
            """
        )

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)

        self._build_ui()
        self.refresh_folders()
        self.refresh_measure_names()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        # DB / Local path grid
        path_grid = QGridLayout()
        db_label = QLabel("DB Path:")
        db_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        path_grid.addWidget(db_label, 0, 0)
        path_grid.addWidget(self.db_path_edit, 0, 1)
        db_browse_btn = QPushButton("...")
        db_browse_btn.clicked.connect(self.browse_db_path)
        path_grid.addWidget(db_browse_btn, 0, 2)
        db_refresh_btn = QPushButton("Refresh")
        db_refresh_btn.clicked.connect(self.refresh_measure_names)
        path_grid.addWidget(db_refresh_btn, 0, 3)

        local_label = QLabel("Local Path:")
        local_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        path_grid.addWidget(local_label, 1, 0)
        path_grid.addWidget(self.local_path_edit, 1, 1)
        local_browse_btn = QPushButton("...")
        local_browse_btn.clicked.connect(self.browse_local_path)
        path_grid.addWidget(local_browse_btn, 1, 2)
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.refresh_folders)
        path_grid.addWidget(refresh_btn, 1, 3)

        path_grid.setColumnStretch(1, 1)
        root.addLayout(path_grid)

        root.addWidget(self.backup_checkbox)

        maintenance_row = QHBoxLayout()
        maintenance_row.addStretch(1)
        check_integrity_btn = QPushButton("Check Rawdata Integrity")
        check_integrity_btn.clicked.connect(self.run_check_rawdata_integrity)
        maintenance_row.addWidget(check_integrity_btn)
        remove_empty_dirs_btn = QPushButton("Clean Empty Dirs")
        remove_empty_dirs_btn.clicked.connect(self.run_remove_empty_dirs)
        maintenance_row.addWidget(remove_empty_dirs_btn)
        root.addLayout(maintenance_row)

        lists_row = QHBoxLayout()

        folder_col = QVBoxLayout()
        folder_col.addWidget(QLabel("Processing:"))
        folder_col.addWidget(self.folder_list_widget, 1)
        run_btn = QPushButton(">>")
        run_btn.clicked.connect(self.run_import)
        folder_col.addWidget(run_btn)

        measure_col = QVBoxLayout()
        measure_col.addWidget(QLabel("In Database:"))
        measure_col.addWidget(self.measure_list_widget, 1)
        delete_btn = QPushButton("<<")
        delete_btn.clicked.connect(self.run_delete_selected)
        measure_col.addWidget(delete_btn)

        lists_row.addLayout(folder_col, 1)
        lists_row.addLayout(measure_col, 1)
        root.addLayout(lists_row, 1)

        root.addWidget(QLabel("Log:"))
        root.addWidget(self.log_box, 1)

    def log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_box.append(f"[{ts}] {msg}")

    def browse_db_path(self):
        self.log("[RUN] 開啟 DB 檔案選擇視窗")
        selected, _ = QFileDialog.getSaveFileName(
            self,
            "Select DB file",
            self.db_path_edit.text().strip() or "DataBase.db",
            "SQLite DB (*.db);;All Files (*)",
        )
        if selected:
            self.db_path_edit.setText(selected)
            self.log(f"[OK] 已選擇 DB 路徑: {selected}")
            self.refresh_measure_names()
        else:
            self.log("[INFO] 取消選擇 DB 檔案")

    def browse_local_path(self):
        self.log("[RUN] 開啟 local 資料夾選擇視窗")
        selected = QFileDialog.getExistingDirectory(
            self,
            "Select local processing folder",
            self.local_path_edit.text().strip() or str(Path.home()),
        )
        if selected:
            self.local_path_edit.setText(selected)
            self.log(f"[OK] 已選擇 local 路徑: {selected}")
            self.refresh_folders()
        else:
            self.log("[INFO] 取消選擇 local 路徑")

    def refresh_folders(self):
        self.folder_list_widget.clear()
        local_path = Path(self.local_path_edit.text().strip())
        if not local_path.exists() or not local_path.is_dir():
            self.log(f"[WARN] 無效 local 路徑: {local_path}")
            return

        folders = sorted([p.name for p in local_path.iterdir() if p.is_dir()])
        for name in folders:
            self.folder_list_widget.addItem(QListWidgetItem(name))
        self.log(f"[INFO] 已載入 {len(folders)} 個資料夾")

    def get_selected_folders(self):
        return [item.text() for item in self.folder_list_widget.selectedItems()]

    def refresh_measure_names(self):
        self.measure_list_widget.clear()
        db_path = Path(self.db_path_edit.text().strip())
        if not db_path.exists() or not db_path.is_file():
            self.log(f"[WARN] 無效 DB 路徑: {db_path}")
            return

        try:
            with DatabaseAPI(db_path) as db:
                measures = db.select_measurements()
            measure_names = sorted({m["measure_name"] for m in measures if m.get("measure_name")})
            for name in measure_names:
                self.measure_list_widget.addItem(QListWidgetItem(name))
            self.log(f"[INFO] 已載入 {len(measure_names)} 個 measure_name")
        except Exception as e:
            self.log(f"[ERROR] 載入 measure_name 失敗: {e}")

    def get_selected_measure_names(self):
        return [item.text() for item in self.measure_list_widget.selectedItems()]

    def run_import(self):
        db_path = Path(self.db_path_edit.text().strip())
        local_path = Path(self.local_path_edit.text().strip())
        folder_list = self.get_selected_folders()
        self.log(f"[RUN] 執行匯入，目標資料夾數量: {len(folder_list)}")

        if not local_path.exists() or not local_path.is_dir():
            self.log(f"[ERROR] local 路徑無效: {local_path}")
            QMessageBox.critical(self, "Error", f"local 路徑無效:\n{local_path}")
            return

        if len(folder_list) == 0:
            self.log("[WARN] 未選擇任何資料夾，匯入中止")
            QMessageBox.warning(self, "Warning", "請至少選擇一個資料夾")
            return

        progress = QProgressDialog("準備匯入...", "取消", 0, len(folder_list), self)
        progress.setWindowTitle("匯入進度")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(0)
        progress.show()

        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            for idx, folder in enumerate(folder_list, start=1):
                progress.setLabelText(f"正在匯入 ({idx}/{len(folder_list)}): {folder}")
                progress.setValue(idx - 1)
                QApplication.processEvents()

                if progress.wasCanceled():
                    self.log("[INFO] 使用者已取消匯入")
                    self.refresh_folders()
                    QMessageBox.information(self, "Canceled", "已取消匯入")
                    return

                folder_path = local_path / folder
                if not folder_path.exists() or not folder_path.is_dir():
                    self.log(f"[SKIP] 找不到資料夾: {folder_path}")
                    continue

                with DatabaseAPI(db_path) as db:
                    if self.backup_checkbox.isChecked() and db_path.exists():
                        backup_path = db.backup_database()
                        self.log(f"[OK] Backup: {backup_path}")

                    self.log(f"[RUN] Importing folder: {folder}")
                    db.import_from_measurement_folder(folder_path,
                                                      schema_file="schema.sql")
                    self.log(f"[OK] Imported: {folder}")

                progress.setValue(idx)
                QApplication.processEvents()

            QMessageBox.information(self, "Done", "匯入完成")
            self.log("[OK] 匯入流程完成")
            self.refresh_folders()
            self.refresh_measure_names()

        except Exception as e:
            self.log(f"[ERROR] {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Error", str(e))
        finally:
            progress.close()

    def run_delete_selected(self):
        db_path = Path(self.db_path_edit.text().strip())
        measure_name_list = self.get_selected_measure_names()
        self.log(f"[RUN] 執行刪除，目標 measure_name 數量: {len(measure_name_list)}")

        if len(measure_name_list) == 0:
            self.log("[WARN] 未選擇任何 measure_name，刪除中止")
            QMessageBox.warning(self, "Warning", "請至少選擇一個 measure_name")
            return

        if not db_path.exists() or not db_path.is_file():
            self.log(f"[ERROR] DB 檔案不存在: {db_path}")
            QMessageBox.critical(self, "Error", f"DB 檔案不存在:\n{db_path}")
            return

        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            "將依照選取的 measure_name 刪除對應的 Measurement 紀錄（含關聯資料）。\n"
            "此操作無法復原，是否繼續？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            self.log("[INFO] 使用者取消刪除確認")
            return

        progress = QProgressDialog("準備刪除...", "取消", 0, len(measure_name_list), self)
        progress.setWindowTitle("刪除進度")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(0)
        progress.show()

        try:
            with DatabaseAPI(db_path) as db:
                if self.backup_checkbox.isChecked() and db_path.exists():
                    backup_path = db.backup_database()
                    self.log(f"[OK] Backup: {backup_path}")

                for idx, measure_name in enumerate(measure_name_list, start=1):
                    progress.setLabelText(f"正在刪除 ({idx}/{len(measure_name_list)}): {measure_name}")
                    progress.setValue(idx - 1)
                    QApplication.processEvents()

                    if progress.wasCanceled():
                        self.log("[INFO] 使用者已取消刪除")
                        QMessageBox.information(self, "Canceled", "已取消刪除")
                        return

                    measures = db.select_measurements(measure_name=measure_name)
                    if len(measures) == 0:
                        self.log(f"[SKIP] 查無 Measurement: {measure_name}")
                        progress.setValue(idx)
                        QApplication.processEvents()
                        continue

                    deleted_count = 0
                    for measure in measures:
                        deleted_count += db.delete_record(DatabaseAPI.TABLE_MEASUREMENTS,
                                                          measure["measure_id"],
                                                          commit=False)
                    db.conn.commit()
                    self.log(f"[OK] Deleted {deleted_count} Measurement(s): {measure_name}")

                    progress.setValue(idx)
                    QApplication.processEvents()

                #db.remove_empty_dirs()

            QMessageBox.information(self, "Done", "刪除完成")
            self.log("[OK] 刪除流程完成")
            self.refresh_folders()
            self.refresh_measure_names()

        except Exception as e:
            self.log(f"[ERROR] {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Error", str(e))
        finally:
            progress.close()

    def run_remove_empty_dirs(self):
        db_path = Path(self.db_path_edit.text().strip())
        self.log("[RUN] 執行清理空資料夾")

        if not db_path.exists() or not db_path.is_file():
            self.log(f"[ERROR] DB 檔案不存在: {db_path}")
            QMessageBox.critical(self, "Error", f"DB 檔案不存在:\n{db_path}")
            return

        try:
            with DatabaseAPI(db_path) as db:
                db.remove_empty_dirs()
                duts = db.select_unused_duts()
                for dut in duts:
                    db.delete_record("DUT", dut['DUT_id'])
            self.log("[OK] 已完成清理空資料夾")
            QMessageBox.information(self, "Done", "空資料夾清理完成")
            self.refresh_folders()
        except Exception as e:
            self.log(f"[ERROR] {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Error", str(e))

    def run_check_rawdata_integrity(self):
        db_path = Path(self.db_path_edit.text().strip())
        selected_measures = self.get_selected_measure_names()
        self.log(f"[RUN] 執行原始檔完整性檢查，measure_name 數量: {len(selected_measures)}")

        if not db_path.exists() or not db_path.is_file():
            self.log(f"[ERROR] DB 檔案不存在: {db_path}")
            QMessageBox.critical(self, "Error", f"DB 檔案不存在:\n{db_path}")
            return

        try:
            with DatabaseAPI(db_path) as db:
                sessions = []
                if len(selected_measures) > 0:
                    for measure_name in selected_measures:
                        sessions.extend(db.select_session(measure_name=measure_name))
                else:
                    self.log("[INFO] 未選取 measure_name，改為檢查全部 session")
                    sessions = db.select_session()

                if len(sessions) == 0:
                    self.log("[INFO] 查無可檢查的 session")
                    QMessageBox.information(self, "Info", "查無可檢查的 session")
                    return

                missing_by_session = {}
                for session in sessions:
                    session_id = session["session_id"]
                    missing_files = db.check_rawdata_integrity(session_id)
                    if len(missing_files) > 0:
                        missing_by_session[session_id] = missing_files

                if len(missing_by_session) == 0:
                    self.log(f"[OK] 檢查完成，共 {len(sessions)} 個 session，未發現缺檔")
                    QMessageBox.information(self, "Done", "檢查完成，未發現缺檔")
                    return

                total_missing = sum(len(v) for v in missing_by_session.values())
                base_dir = db_path.parent / 'RawdataFiles'
                source_root = Path(r'\\DESKTOP-GETONC5\oeic_dc')
                repaired_count = 0
                failed_count = 0
                failed_items = []
                self.log(f"[WARN] 檢查完成，共 {len(missing_by_session)} 個 session 有缺檔，總計 {total_missing} 筆")
                for session_id, files in missing_by_session.items():
                    self.log(f"[WARN] session_id={session_id}, 缺檔 {len(files)} 筆")
                    for file_path in files:
                        try:
                            relative_path = Path(file_path).relative_to(base_dir)
                        except ValueError:
                            relative_path = Path(file_path).name

                        src_file = source_root / relative_path
                        self.log(f"修正: {src_file} -> {file_path}")
                        try:
                            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(src_file, file_path)
                            repaired_count += 1
                        except Exception as copy_error:
                            failed_count += 1
                            failed_items.append((session_id, str(file_path), str(copy_error)))
                            self.log(f"[ERROR] 修正失敗: {file_path} | {copy_error}")
                        


                QMessageBox.warning(
                    self,
                    "Integrity Check Result",
                    (
                        f"檢查完成：\n"
                        f"有缺檔 session 數量: {len(missing_by_session)}\n"
                        f"缺檔總數: {total_missing}\n"
                        f"已修正: {repaired_count}\n"
                        f"修正失敗: {failed_count}\n"
                        f"來源根目錄: {source_root}\n"
                        f"資料庫 RawData 根目錄: {base_dir}\n"
                        f"請查看 Log。"
                    ),
                )

        except Exception as e:
            self.log(f"[ERROR] {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Error", str(e))


def main():
    app = QApplication(sys.argv)
    win = ImportDatabaseGUI()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()