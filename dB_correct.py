#%%
import csv
import re
from pathlib import Path
import numpy as np


# 範例檔名片段: xxx_arg_10_3.5dB.zip
FILENAME_PATTERN = re.compile(
	r"_arg_(?P<arg>[+-]?\d+(?:\.\d+)?)_(?P<db>[+-]?\d+(?:\.\d+)?)dB",
	re.IGNORECASE,
)


def extract_values_from_name(file_name: str) -> tuple[float, float] | None:
	"""從檔名中擷取 arg 與 dB 數值，擷取失敗則回傳 None。"""
	match = FILENAME_PATTERN.search(file_name)
	if not match:
		return None

	arg_value = float(match.group("arg"))
	db_value = float(match.group("db"))
	return arg_value, db_value


def scan_lsrfs_zip_files(root_dir: Path) -> list[dict[str, object]]:
	"""遞迴掃描 root_dir 下所有檔名含 LSRF 的 zip 檔並擷取數值。"""
	results: list[dict[str, object]] = []

	for zip_path in root_dir.rglob("*.zip"):
		if "lsrf" not in zip_path.name.lower():
			continue

		values = extract_values_from_name(zip_path.stem)
		if values is None:
			continue

		arg_value, db_value = values
		results.append(
			{
				"file_path": str(zip_path),
				"arg_value": arg_value,
				"db_value": db_value,
			}
		)

	return results


def save_to_csv(rows: list[dict[str, object]], output_csv: Path) -> None:
	"""將結果寫入 CSV。"""
	with output_csv.open("w", newline="", encoding="utf-8-sig") as f:
		writer = csv.DictWriter(f, fieldnames=["file_path", "arg_value", "db_value"])
		writer.writeheader()
		writer.writerows(rows)


root_dir = Path(r'R:\T&P 量測資料\MTK\MRM_09')
if not root_dir.exists() or not root_dir.is_dir():
    raise SystemExit(f"無效資料夾路徑: {root_dir}")

rows = scan_lsrfs_zip_files(root_dir)
if not rows:
    print("找不到符合條件的檔案，或檔名格式不符合 _arg_x_y dB 規則。")

else:
	print(f"共找到 {len(rows)} 筆:")
	for row in rows:
		new_dB = np.round(row['db_value']/0.5) * 0.5
		new_dB_str = f"{new_dB:g}"
		#print(row['db_value'], new_dB)
		new_file_name = re.sub(
			r"(_arg_[+-]?\d+(?:\.\d+)?_)[+-]?\d+(?:\.\d+)?(dB)",
			rf"\g<1>{new_dB_str}\g<2>",
			Path(row['file_path']).name,
		)
		#print(f"原始檔名: {Path(row['file_path']).name}")
		#print(f"新檔名: {new_file_name}")
		new_file_path = Path(row['file_path']).with_name(new_file_name)
		try:
			Path(row['file_path']).rename(new_file_path)
			print(f"已將檔案重新命名為: {new_file_name}")
		except Exception as e:
			print(f"重新命名檔案時發生錯誤: {e}")
# %%
from pathlib import Path
import re


folder = r"X:\1_Database\Processing\260902_AMD_cage158_D4_85C"
folder = Path(folder)

# 1. 找出開頭前 4 個字符為 "SPCM" 的 CSV
spcm_files = [
	f for f in folder.iterdir()
	if f.is_file()
	and f.suffix.lower() == ".csv"
	and f.name[:4] == "SPCM"
]

for spcm_file in spcm_files:

	# 2. 去除 "SPCM_" 與 ".csv"，取得 C
	C = spcm_file.stem.removeprefix("SPCM_")

	#print(f"\nSPCM file : {spcm_file.name}")
	#print(f"C         : {C}")

	# 3. 搜尋檔名包含 C 的 .s2p 檔案
	s2p_files = [f for f in folder.iterdir() 
			  if f.is_file() and f.suffix.lower() == ".s2p" and C in f.name]

	# 4. 依據檔名中的 "arg_數值_" 排序
	def get_arg_value(file):
		match = re.search(r"arg_([-+]?\d+(?:\.\d+)?)", file.name)
		return float(match.group(1))

	s2p_files.sort(key=get_arg_value)

	# 5. 依序重新命名
	for i, file in enumerate(s2p_files):
		new_name = file.with_name(
			f"{file.stem}_{4 + i}dB{file.suffix}"
		)

		print(f"{file.name} -> {new_name.name}")

		file.rename(new_name)



# %%
