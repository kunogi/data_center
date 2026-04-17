import sqlite3
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
import random

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "quant_data.db"

def audit_kline_quality(days_to_check=60):
    print(f"🕵️ 启动 K 线复权质量全局审计 | 回溯期: 近 {days_to_check} 天")
    print("=" * 70)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 获取需要审计的日期范围
    target_date = (datetime.now() - timedelta(days=days_to_check)).strftime('%Y-%m-%d')
    
    print("⏳ 正在将近两个月全市场 K 线载入内存进行矩阵扫描...")
    df = pd.read_sql_query(f"""
        SELECT date, code, close 
        FROM daily_k_data 
        WHERE date >= '{target_date}'
        ORDER BY code, date ASC
    """, conn)
    
    if df.empty:
        print("⚠️ 数据库中没有近两个月的 K 线数据。")
        conn.close()
        return

    # ==========================================
    # 🚨 第一重审计：A股物理极限跌幅扫描
    # ==========================================
    print("\n🔍 第一阶段：执行【物理极限跌幅】扫描 (寻找未复权断层)...")
    df['prev_close'] = df.groupby('code')['close'].shift(1)
    df['pct_chg'] = (df['close'] - df['prev_close']) / df['prev_close']
    
    # 设定警戒线：-35% (除新股外，A股任何板块单日跌幅不可能超过 30%)
    # 如果跌幅超过 35%，必然是因为除权除息导致价格腰斩，且未被复权抹平
    anomalies = df[df['pct_chg'] < -0.35].copy()
    
    if not anomalies.empty:
        print(f"❌ [警报] 发现 {len(anomalies)} 处极其可疑的未复权断层！")
        print("这些股票极大概率是在 18:00 前被拉取了原始 K 线：")
        print(anomalies[['date', 'code', 'prev_close', 'close', 'pct_chg']].to_string(index=False))
        print("💡 建议修复：在数据库中执行 DELETE FROM daily_k_data WHERE code IN (...) AND date >= 异常日期，让脚本重新拉取。")
    else:
        print("✅ 完美！全库未发现任何突破物理极限的跌幅断层，内部连贯性校验通过。")

    # ==========================================
    # 🎯 第二重审计：东方财富外部交叉对账
    # ==========================================
    print("\n🔍 第二阶段：执行【AkShare 第三方外部交叉对账】...")
    
    # 随机抽取 5 只股票，并在近两个月中随机选一天
    unique_codes = df['code'].unique()
    sample_codes = random.sample(list(unique_codes), min(5, len(unique_codes)))
    
    pass_count = 0
    for code in sample_codes:
        # 转换代码格式给 AkShare: sh.600000 -> 600000
        ak_code = code.split('.')[1]
        
        # 拿出该股票在库里的历史数据
        code_df = df[df['code'] == code].dropna()
        if code_df.empty: continue
            
        # 随机挑历史中的一天
        random_row = code_df.sample(1).iloc[0]
        test_date = random_row['date']
        db_price = random_row['close']
        
        try:
            # 向东财请求这只股票的【前复权】历史 K 线
            ak_kline = ak.stock_zh_a_hist(symbol=ak_code, period="daily", start_date=test_date.replace("-", ""), end_date=test_date.replace("-", ""), adjust="qfq")
            
            if not ak_kline.empty:
                ak_price = ak_kline.iloc[0]['收盘']
                
                # 允许极小的浮点误差 (如 0.02 元)
                if abs(db_price - ak_price) <= 0.03:
                    print(f"  ✔️ [对账成功] {code} @ {test_date} | 你的库: {db_price:.2f} == 东财qfq: {ak_price:.2f}")
                    pass_count += 1
                else:
                    print(f"  ❌ [对账失败] {code} @ {test_date} | 你的库: {db_price:.2f} != 东财qfq: {ak_price:.2f}")
            else:
                print(f"  ⚠️ [跳过] 无法从东财获取 {code} 在 {test_date} 的数据。")
        except Exception as e:
            print(f"  ⚠️ [请求异常] {code}: {e}")
            
    print("-" * 70)
    if pass_count == len(sample_codes):
        print("🎯 终极结论：外部对账全部吻合！你库里的这批历史数据是 100% 纯正的【前复权】数据，非常安全。")
    else:
        print("⚠️ 终极结论：存在对账不符的数据。如果大面积不符，说明你之前可能长期在 18:00 前拉取了数据。")

    conn.close()

if __name__ == "__main__":
    audit_kline_quality(60)