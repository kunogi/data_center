import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
import random

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "quant_data.db"

def get_last_trade_date(conn):
    """获取数据库中最新的全市场交易日"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM daily_k_data")
    result = cursor.fetchone()
    return result[0] if result[0] else None

def get_tencent_qfq_close(code, target_date):
    """
    🔌 腾讯财经前复权 K 线极速获取探针
    支持 A 股、ETF，自带穿透物理代理属性。
    """
    tx_code = code.replace('.', '')
    
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={tx_code},day,{target_date},{target_date},10,qfq"
    
    proxies = {
        "http": None,
        "https": None
    }
    
    try:
        resp = requests.get(url, proxies=proxies, timeout=5)
        data = resp.json()
        
        if data.get('code') == 0:
            stock_data = data['data'].get(tx_code, {})
            if not stock_data:
                return None
                
            kline_list = stock_data.get('qfqday') or stock_data.get('day')
            
            if kline_list:
                for k in kline_list:
                    if k[0] == target_date:
                        return float(k[2])
            return None
        return None
        
    except Exception as e:
        print(f"  ⚠️ [腾讯接口受阻] {code}: {str(e)}")
        return None

def audit_kline_quality(days_to_check=90):
    print(f"🕵️ 启动实盘 K 线质量联合审计 | 回溯纵深: 近 {days_to_check} 天")
    print("=" * 80)
    
    conn = sqlite3.connect(DB_PATH)
    
    last_trade_date = get_last_trade_date(conn)
    if not last_trade_date:
        print("⚠️ 数据库为空，中止审计。")
        conn.close()
        return
        
    target_date = (datetime.now() - timedelta(days=days_to_check)).strftime('%Y-%m-%d')
    
    print(f"⏳ 正在载入 [{target_date}] 至 [{last_trade_date}] 的 K 线矩阵...")
    df = pd.read_sql_query(f"""
        SELECT date, code, close 
        FROM daily_k_data 
        WHERE date >= '{target_date}'
        ORDER BY code, date ASC
    """, conn)
    
    if df.empty:
        print("⚠️ 指定区间内无 K 线数据。")
        conn.close()
        return

    # ==========================================
    # 🚨 第一重审计：内部物理极限防断层扫描
    # ==========================================
    print(f"\n🔍 阶段一：内部断层扫描 (容忍极限：单日跌幅 35%)")
    df['prev_close'] = df.groupby('code')['close'].shift(1)
    df['pct_chg'] = (df['close'] - df['prev_close']) / df['prev_close']
    
    anomalies = df[df['pct_chg'] < -0.35].copy()
    
    if not anomalies.empty:
        print(f"❌ [警报] 发现 {len(anomalies)} 处严重未复权断层！系统均线已受污染。")
        print("-" * 50)
        sql_commands = []
        for _, row in anomalies.iterrows():
            code = row['code']
            sql = f"DELETE FROM daily_k_data WHERE code = '{code}';"
            sql_commands.append(sql)
            print(f"[{row['date']}] {code} : 昨日 {row['prev_close']:.2f} -> 今日 {row['close']:.2f} (断崖 {row['pct_chg']*100:.1f}%)")
        
        print("-" * 50)
        print("💡 [一键自愈指南] 请在 SQLite 终端执行以下指令，然后重新运行 data_sync.py：")
        for sql in set(sql_commands):
            print(sql)
    else:
        print("✅ 完美通过！近 90 天未发现物理级跌幅断层。")

    # ==========================================
    # 🎯 第二重审计：腾讯财经外部双边核查
    # ==========================================
    print(f"\n🔍 阶段二：腾讯财经外部交叉对账网络")
    
    unique_codes = df['code'].unique()
    sample_codes_history = random.sample(list(unique_codes), min(5, len(unique_codes)))
    sample_codes_latest = random.sample(list(unique_codes), min(5, len(unique_codes)))
    
    pass_count = 0
    total_checks = 0

    def check_against_tx(code, test_date, db_price):
        nonlocal pass_count, total_checks
        
        tx_price = get_tencent_qfq_close(code, test_date)
        total_checks += 1
        
        if tx_price is not None:
            if abs(db_price - tx_price) <= 0.03:
                print(f"  ✔️ [对账通过] {code} @ {test_date} | Local: {db_price:.2f} == Tencent: {tx_price:.2f}")
                pass_count += 1
            else:
                print(f"  ❌ [严重偏离] {code} @ {test_date} | Local: {db_price:.2f} != Tencent: {tx_price:.2f} (价差: {abs(db_price - tx_price):.2f})")
        else:
            print(f"  ⚠️ [数据缺失] 腾讯财经无 {code} 在 {test_date} 的记录 (可能停牌)。")

    print(f"  ➤ 任务 A：最新交易日 [{last_trade_date}] 强制对账 (监控 Baostock 昨日数据状态)")
    for code in sample_codes_latest:
        latest_row = df[(df['code'] == code) & (df['date'] == last_trade_date)]
        if not latest_row.empty:
            check_against_tx(code, last_trade_date, latest_row.iloc[0]['close'])

    print(f"\n  ➤ 任务 B：近 90 天历史盲盒抽查 (监控时序数据稳定性)")
    for code in sample_codes_history:
        code_df = df[df['code'] == code].dropna()
        if not code_df.empty:
            random_row = code_df.sample(1).iloc[0]
            check_against_tx(code, random_row['date'], random_row['close'])
            
    print("=" * 80)
    if total_checks > 0 and pass_count == total_checks:
        print("🎯 审计结论：A 级 (极优)。实盘底层数据清澈见底，请放心交由 Screener 屠戮市场！")
    elif total_checks > 0:
        print(f"⚠️ 审计结论：存在 {total_checks - pass_count} 处外部对账偏离。请仔细排查原因。")
    
    conn.close()

if __name__ == "__main__":
    audit_kline_quality(120)