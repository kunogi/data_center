import pandas as pd
import sqlite3
import numpy as np

# ===================== 【SQLite 配置】=====================
DB_PATH = "quant_data.db"
TABLE1 = "financial_factors"
TABLE2 = "financial_factors_ak_staging"

COMMON_COLS = [
    "code", "stat_date", "pub_date",
    "roe_avg", "yoy_profit_growth", "eps_ttm",
    "np_margin", "cfo_to_gr", "yoy_pni"
]
# ===========================================================

# 1. 连接 SQLite
conn = sqlite3.connect(DB_PATH)

# 2. 读取两张表
print(f"正在读取表：{TABLE1}")
df1 = pd.read_sql(f"SELECT {','.join(COMMON_COLS)} FROM {TABLE1}", conn)

print(f"正在读取表：{TABLE2}")
df2 = pd.read_sql(f"SELECT {','.join(COMMON_COLS)} FROM {TABLE2}", conn)

# 3. 合并对比
df_merged = pd.merge(
    df1, df2,
    on=["code", "stat_date"],
    how="inner",
    suffixes=("_主表", "_临时表")
)

# 4. 输出差异
print("\n" + "="*80)
print("🔍 开始对比两张表字段差异...")
print("="*80)

diff_count = 0

for col in COMMON_COLS:
    if col in ["code", "stat_date"]:
        continue

    col_a = f"{col}_主表"
    col_b = f"{col}_临时表"

    # --- 核心修复：智能对比逻辑 ---
    # 情况1：如果是浮点数，用 np.isclose 处理精度问题
    if pd.api.types.is_numeric_dtype(df_merged[col_a]) and pd.api.types.is_numeric_dtype(df_merged[col_b]):
        # 计算绝对值差
        abs_diff = (df_merged[col_a] - df_merged[col_b]).abs()
        
        # 对比：差小于0.005 OR 两边都是 NaN
        diff_mask = ~(
            (abs_diff < 9) | 
            (df_merged[col_a].isna() & df_merged[col_b].isna())
        )
    # 情况2：其他类型（字符串、日期等）直接对比
    else:
        diff_mask = ~(
            (df_merged[col_a] == df_merged[col_b]) | 
            (df_merged[col_a].isna() & df_merged[col_b].isna())
        )

    diff_rows = df_merged[diff_mask]

    if not diff_rows.empty:
        for _, row in diff_rows.iterrows():
            diff_count += 1
            print(f"\n不一致：code={row['code']}, stat_date={row['stat_date']}")
            print(f"   字段：{col}")
            print(f"   主表：{row[col_a]}")
            print(f"   临时表：{row[col_b]}")

print("\n" + "="*80)
if diff_count == 0:
    print("✅ 两张表完全一致！")
else:
    print(f"⚠️  共发现 {diff_count} 处不一致！")
print("="*80)

conn.close()