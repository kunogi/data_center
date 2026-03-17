import sqlite3
import pandas as pd
import akshare as ak
from config import DB_PATH

# 全局配置
TARGET_DATE = '2024-12-31' # 穿越回 2024 年最后一天
LOOKBACK_DAYS = 120

_stock_name_cache = {}

def get_stock_name(code):
    global _stock_name_cache
    if not _stock_name_cache:
        try:
            print("📦 正在拉取真实名称映射...")
            df = ak.stock_info_a_code_name()
            _stock_name_cache = dict(zip(df['code'], df['name']))
        except:
            pass
    pure_code = code.split('.')[-1]
    return _stock_name_cache.get(pure_code, "未知名称")

def run_historical_snapshot():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 找到指定日期之前的 120 个交易日
    dates_query = f"SELECT DISTINCT date FROM daily_k_data WHERE date <= '{TARGET_DATE}' ORDER BY date DESC LIMIT {LOOKBACK_DAYS}"
    dates_df = pd.read_sql_query(dates_query, conn)
    
    if dates_df.empty or len(dates_df) < 65:
        print(f"❌ 数据库中 {TARGET_DATE} 之前的数据不足，无法计算！")
        return
        
    start_date = dates_df['date'].min()
    end_date = dates_df['date'].max() # 这个就是实际上能取到的最接近 TARGET_DATE 的日期
    print(f"🕰️ 时光机已启动，锁定截面日期: {end_date}")

    # 2. 读取数据并做成矩阵
    query = f"SELECT date, code, close, volume FROM daily_k_data WHERE date >= '{start_date}' AND date <= '{end_date}'"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    closes = df.pivot(index='date', columns='code', values='close').ffill()
    volumes = df.pivot(index='date', columns='code', values='volume').fillna(0)

    # 3. 计算多因子
    mean_20 = closes.rolling(window=20).mean()
    std_20 = closes.rolling(window=20).std()
    mean_60 = closes.rolling(window=60).mean()
    vol_mean_60 = volumes.rolling(window=60).mean()
    vol_mean_5 = volumes.rolling(window=5).mean()

    current_close = closes.iloc[-1]
    current_vol = volumes.iloc[-1]
    last_vol_mean_5 = vol_mean_5.iloc[-1]
    last_vol_mean_60 = vol_mean_60.iloc[-1]
    active_mask = (current_vol > 0) & (current_close.notna())

    print("\n=======================================================")
    print(f"📊 时光回测战报 - 截面日期: {end_date}")
    print("=======================================================\n")

    # 🗡️ Z-Score (抄底)
    z_score = (closes - mean_20) / std_20
    current_z = z_score.iloc[-1]
    z_candidates = current_z[active_mask & (current_z < -2.0) & (current_vol > last_vol_mean_5)].sort_values(ascending=True).dropna().head(10)
    
    print("🗡️ 【Z-Score 极寒恐慌跌透名单】(赌跨年反弹修复):")
    for code, val in z_candidates.items():
        print(f"   - {code} ({get_stock_name(code)}) | Z值: {val:.2f}")

    # 🐎 RS (主升浪)
    ret_60 = closes.pct_change(periods=60)
    current_ret_60 = ret_60.iloc[-1]
    rs_rank = current_ret_60[active_mask].rank(pct=True) * 100
    rs_candidates = rs_rank[(rs_rank > 95.0) & (current_close > mean_20.iloc[-1])].sort_values(ascending=False).dropna().head(10)
    
    print("\n🐎 【RS 跨年妖股预备役】(过去3个月绝对龙头):")
    for code, val in rs_candidates.items():
        print(f"   - {code} ({get_stock_name(code)}) | 击败全市场 {val:.2f}%")

    # 🎯 VCP + 量窒息 (完美融合你的建议)
    bbw = (4 * std_20) / mean_20
    current_bbw = bbw.iloc[-1]
    vcp_rank = current_bbw[active_mask].rank(pct=True, ascending=True) * 100
    
    # 🌟 核心升级：增加“量窒息”条件 -> 当日成交量必须小于 60 日均量的 60%
    volume_shrink_mask = (current_vol < (last_vol_mean_60 * 0.6))
    
    vcp_candidates = vcp_rank[
        (vcp_rank < 5.0) & 
        (current_close > mean_60.iloc[-1]) & 
        volume_shrink_mask  # 必须满足量窒息！彻底剔除银行织布机
    ].sort_values(ascending=True).dropna().head(10)

    print("\n🎯 【VCP + 量窒息 潜伏名单】(振幅极窄且成交量极度枯竭，随时变盘):")
    for code, val in vcp_candidates.items():
        vol_ratio = (current_vol[code] / last_vol_mean_60[code]) * 100
        print(f"   - {code} ({get_stock_name(code)}) | 窄幅前 {val:.2f}% | 成交量仅为平时 {vol_ratio:.1f}%")

if __name__ == "__main__":
    run_historical_snapshot()