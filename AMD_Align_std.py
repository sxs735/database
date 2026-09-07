#%%
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def read_csv_to_df(csv_path: str | Path, **kwargs) -> pd.DataFrame:
    """Read CSV file and return a DataFrame."""
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    return pd.read_csv(csv_path, **kwargs)


def group_by_die_subdie(df: pd.DataFrame) -> dict:
    """Group rows by Die and SubDie Number, and collect selected columns into dict."""
    group_cols = ["Global Iteration","Die", "SubDie Number"]
    value_cols = ["Row", "Column",
                  "X Motor Position [um]",
                  "Y Motor Position [um]",
                  "Z Motor Position [um]",
                  "X PZT Position [um]",
                  "Y PZT Position [um]",
                  "Z PZT Position [um]",
                  "Power [dBm]"]

    required_cols = group_cols + value_cols
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    grouped_dict: dict = {}
    for (g, die, subdie), value in df.groupby(group_cols, dropna=False):
        grouped_dict[(int(g),int(die), int(subdie))] = {
            col: value[col].tolist() for col in value_cols
        }

    return grouped_dict


if __name__ == "__main__":
    # Example
    #csv_file = r'R:\T&P 量測資料\AMD\Gage R&R\260814\FAU_Motor_Coordinates_Repeatability_2026-08-14-18-52-14.csv'
    csv_file = r'R:\T&P 量測資料\AMD\Gage R&R\260814\FAU_Motor_Coordinates_Stability_2026-08-14-18-52-14.csv'
    #csv_file = r'R:\T&P 量測資料\AMD\Gage R&R\260819\FAU_Motor_Coordinates_Stability_2026-08-19-17-35-36.csv'
    df = read_csv_to_df(csv_file)
    grouped = group_by_die_subdie(df)




#%%
    rows = []
    for (die, subdie), data in grouped.items():
        #G = data['Global Iteration']
        X = data["Row"][0]
        Y = data["Column"][0]
        x_motor = data["X Motor Position [um]"]
        y_motor = data["Y Motor Position [um]"]
        z_motor = data["Z Motor Position [um]"]
        x_pzt = data["X PZT Position [um]"]
        y_pzt = data["Y PZT Position [um]"]
        z_pzt = data["Z PZT Position [um]"]
        power = data["Power [dBm]"]
        power_mean = sum(power) / len(power)
        power_std = np.std(power)
        m_center_x = sum(x_motor) / len(x_motor)
        m_center_y = sum(y_motor) / len(y_motor)
        m_center_z = sum(z_motor) / len(z_motor)
        m_delta_xy = [np.sqrt((x-m_center_x)**2 + (y-m_center_y)**2) for x, y in zip(x_motor, y_motor)]
        m_delta_z = [abs(z - m_center_z) for z in z_motor]
        m_delta_xy_std = np.std(m_delta_xy)
        m_delta_z_std = np.std(m_delta_z)
        m_delta_xy_max = np.max(m_delta_xy)
        m_delta_z_max = np.max(m_delta_z)

        p_center_x = sum(x_pzt) / len(x_pzt)
        p_center_y = sum(y_pzt) / len(y_pzt)
        p_center_z = sum(z_pzt) / len(z_pzt)
        p_delta_xy = [np.sqrt((x-p_center_x)**2 + (y-p_center_y)**2) for x, y in zip(x_pzt, y_pzt)]
        p_delta_z = [z - p_center_z for z in z_pzt]
        p_delta_xy_std = np.std(p_delta_xy)
        p_delta_z_std = np.std(p_delta_z)
        p_delta_xy_max = np.max(p_delta_xy)
        p_delta_z_max = np.max(p_delta_z)

        rows.append([
            die, X, Y, subdie,
            power_mean, power_std,
            m_center_x, m_center_y, m_center_z,
            m_delta_xy_std, m_delta_z_std,m_delta_xy_max, m_delta_z_max,
            p_center_x, p_center_y, p_center_z,
            p_delta_xy_std, p_delta_z_std,p_delta_xy_max, p_delta_z_max
        ])

    columns = [
        "Die", "Row", "Column", "SubDie Number",
        "Power mean [dBm]", "Power std [dBm]",
        "X Motor center [um]", "Y Motor center [um]", "Z Motor center [um]",
        "Motor delta XY std [um]", "Motor delta Z std [um]", "Motor delta XY max [um]", "Motor delta Z max [um]",
        "X PZT center [um]", "Y PZT center [um]", "Z PZT center [um]",
        "PZT delta XY std [um]", "PZT delta Z std [um]", "PZT delta XY max [um]", "PZT delta Z max [um]",
    ]

    df_out = pd.DataFrame(rows, columns=columns)
    xlsx_path = Path(csv_file).with_name(f"{Path(csv_file).stem}_summary.xlsx")
    df_out.to_excel(xlsx_path, index=False)
    print(f"Saved XLSX: {xlsx_path}")
# %%
SubDie = 1
rows = np.array(rows)
x_index = rows[:,1]-min(rows[:,1])
y_index = rows[:,2]-min(rows[:,2])
Power_mean = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Power_std = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
X_Motor_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Y_Motor_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Z_Motor_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Motor_delta_XY_std = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Motor_delta_Z_std = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Motor_delta_XY_max = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Motor_delta_Z_max = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
X_PZT_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Y_PZT_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
Z_PZT_center = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
PZT_delta_XY_std = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
PZT_delta_Z_std = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
PZT_delta_XY_max = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
PZT_delta_Z_max = np.full((int(max(x_index)+1), int(max(y_index)+1)), np.nan)
#subdie_index = np.unique(rows[:,3])
for row in rows:
    if row[3] == SubDie:
        Power_mean[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[4]
        Power_std[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[5]
        X_Motor_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[6]
        Y_Motor_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[7]
        Z_Motor_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[8]
        Motor_delta_XY_std[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[9]
        Motor_delta_Z_std[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[10]
        Motor_delta_XY_max[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[11]
        Motor_delta_Z_max[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[12]
        X_PZT_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[13]
        Y_PZT_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[14]
        Z_PZT_center[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[15]
        PZT_delta_XY_std[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[16]
        PZT_delta_Z_std[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[17]
        PZT_delta_XY_max[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[18]
        PZT_delta_Z_max[int(row[1]-min(rows[:,1])), int(row[2]-min(rows[:,2]))] = row[19]

heatmaps = [
    ("Power mean [dBm]", Power_mean),
    ("Power std [dBm]", Power_std),
    ("X Motor center [um]", X_Motor_center),
    ("Y Motor center [um]", Y_Motor_center),
    ("Z Motor center [um]", Z_Motor_center),
    ("Motor delta XY std [um]", Motor_delta_XY_std),
    ("Motor delta Z std [um]", Motor_delta_Z_std),
    ("Motor delta XY max [um]", Motor_delta_XY_max),
    ("Motor delta Z max [um]", Motor_delta_Z_max),
    ("X PZT center [um]", X_PZT_center),
    ("Y PZT center [um]", Y_PZT_center),
    ("Z PZT center [um]", Z_PZT_center),
    ("PZT delta XY std [um]", PZT_delta_XY_std),
    ("PZT delta Z std [um]", PZT_delta_Z_std),
    ("PZT delta XY max [um]", PZT_delta_XY_max),
    ("PZT delta Z max [um]", PZT_delta_Z_max),
]

fig, axes = plt.subplots(4, 4, figsize=(20, 16), constrained_layout=True)
axes = axes.ravel()

for ax, (title, data) in zip(axes, heatmaps):
    data_masked = np.ma.masked_invalid(data)
    im = ax.imshow(data_masked, origin='upper', cmap='rainbow', aspect='auto')
    ax.set_title(title)
    ax.set_xlabel('Column Index')
    ax.set_ylabel('Row Index')
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

fig.suptitle(f'Stability (SubDie = {SubDie})', fontsize=16)
fig_path = Path(csv_file).with_name(f"{Path(csv_file).stem}_summary_heatmaps_SubDie{SubDie}.png")
fig.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.show()
# %%
rows = []
for (g, die, subdie), data in grouped.items():
    G = g
    X = data["Row"][0]
    Y = data["Column"][0]
    x_motor = data["X Motor Position [um]"]
    y_motor = data["Y Motor Position [um]"]
    z_motor = data["Z Motor Position [um]"]
    x_pzt = data["X PZT Position [um]"]
    y_pzt = data["Y PZT Position [um]"]
    z_pzt = data["Z PZT Position [um]"]
    power = data["Power [dBm]"]
    rows += [[G, X, Y, x_motor[0], y_motor[0], z_motor[0], x_pzt[0], y_pzt[0], z_pzt[0], power[0]]]
#%%
x_index = []
y_index = []
for (g, die, subdie), data in grouped.items():
    x_index += [data["Row"][0]]
    y_index += [data["Column"][0]]
x_idx0 = min(x_index)
y_idx0 = min(y_index)
m = max(x_index)-x_idx0
n= max(y_index)-y_idx0

#%%
%matplotlib qt
fig, axes = plt.subplots(m + 1, n + 1, figsize=(4 * (n + 1), 3 * (m + 1)), squeeze=False)
SubDie = 1
#依據m,n繪製等數量與排列的子圖
for (g, die, subdie), data in grouped.items():
    m_i = data["Row"][0] - x_idx0
    n_i = data["Column"][0] - y_idx0
    x_motor = np.array(data["X Motor Position [um]"])
    y_motor = np.array(data["Y Motor Position [um]"])
    z_motor = np.array(data["Z Motor Position [um]"])
    x_pzt = np.array(data["X PZT Position [um]"])-50
    y_pzt = np.array(data["Y PZT Position [um]"])-50
    z_pzt = np.array(data["Z PZT Position [um]"])-50
    power = data["Power [dBm]"]

    if subdie == SubDie:
        #繪製plot(y) 在對應m_i, n_i的子圖位置
        ax = axes[m_i, n_i]
        x = np.arange(len(power))
        ax.plot(x, power, linestyle='-', linewidth=1)
        #ax.scatter(x_motor+x_pzt, y_motor+y_pzt, s=2)
        # ax.set_xlim(np.mean(x_motor+x_pzt) - 5, np.mean(x_motor+x_pzt) + 5)
        # ax.set_ylim(np.mean(y_motor+y_pzt) - 5, np.mean(y_motor+y_pzt) + 5)
        # ax.set_aspect('equal')
        ax.set_title(f"Die: {die}", fontsize=8)


for r in range(m + 1):
    for c in range(n + 1):
        if not axes[r, c].has_data():
            axes[r, c].axis('off')

fig.suptitle(f"{Path(csv_file).stem}, power, SubDie = {SubDie}", fontsize=14)
fig.tight_layout(rect=[0, 0.03, 1, 0.96])
trend_fig_path = Path(csv_file).with_name(f"{Path(csv_file).stem}_power_SubDie{SubDie}.png")
fig.savefig(trend_fig_path, dpi=300, bbox_inches="tight")
plt.show()



# %%
for (g, die, subdie), data in grouped.items():
    #m_i = data["Row"][0] - x_idx0
    #n_i = data["Column"][0] - y_idx0
    x_motor = data["X Motor Position [um]"]
    y_motor = data["Y Motor Position [um]"]
    z_motor = data["Z Motor Position [um]"]
    x_pzt = data["X PZT Position [um]"]
    y_pzt = data["Y PZT Position [um]"]
    z_pzt = data["Z PZT Position [um]"]
    power = data["Power [dBm]"]

    if subdie == 0:
        #繪製plot(y) 在對應m_i, n_i的子圖位置
        #ax = axes[m_i, n_i]
        x = np.arange(len(power))
        plt.plot(x, power, linestyle='-', linewidth=1)
        #plt.scatter(x_motor, y_motor, s=2)
        #plt.xlim(np.mean(x_motor) - 5, np.mean(x_motor) + 5)
        #plt.ylim(np.mean(y_motor) - 5, np.mean(y_motor) + 5)
        #plt.gca().set_aspect('equal')
        #plt.title(f"Die: {die}", fontsize=8)
plt.show()

# %%
