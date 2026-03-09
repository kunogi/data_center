import baostock as bs
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
import os
import argparse
import sys
from multiprocessing import Pool, cpu_count
from config import DB_PATH, BLACKLIST_FILE, COMPLETED_FILE

# 💥 为了强制全量更新补充新字段，设为 -1。跑完今天这遍后，请务必改回 30！
EXPIRE_DAYS = 30                
ACTIVE_DAYS = 30                # 只更新最近 X 天活跃股
PROCESS_COUNT = min(8, cpu_count() * 2)

def get_latest_financial_quarter():
    """根据当前物理时间，推算全市场最完整的最新财报季度"""
    now = datetime.now()
    year = now.year
    month = now.month
    
    if month <= 4:
        # 1-4月：去年年报和今年一季报在4月底才披露完，最完整的是【去年Q3】
        return str(year - 1), "3"
    elif month <= 8:
        # 5-8月：一季报已出完，半年报要到8月底，最完整的是【今年Q1】
        return str(year), "1"
    elif month <= 10:
        # 9-10月：半年报已出完，三季报要到10月底，最完整的是【今年Q2】
        return str(year), "2"
    else:
        # 11-12月：三季报已出完，最完整的是【今年Q3】
        return str(year), "3"

def load_blacklist():
    if not os.path.exists(BLACKLIST_FILE): return set()
    with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def load_completed():
    if not os.path.exists(COMPLETED_FILE): return {}
    completed = {}
    now = datetime.now()
    if os.path.exists(COMPLETED_FILE):
        with open(COMPLETED_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 2:
                    code, ts_str = parts
                    try:
                        ts = datetime.fromisoformat(ts_str)
                        if (now - ts).days <= EXPIRE_DAYS: completed[code] = ts
                    except: pass
    return completed

def init_financial_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_factors (
            code TEXT, stat_date TEXT, pub_date TEXT,
            roe_avg REAL, yoy_profit_growth REAL, np_margin REAL,
            gp_margin REAL, eps_ttm REAL, net_profit REAL,
            mb_revenue REAL, 
            liability_ratio REAL, cash_flow REAL, 
            update_date TEXT,
            PRIMARY KEY (code, stat_date, pub_date)
        )
    """)
    conn.commit()
    conn.close()

def worker_init():
    bs.login()

def fetch_single_stock(code):
    try:
        # 💥 动态获取最新财报季
        year, quarter = get_latest_financial_quarter()
        
        profit_df = bs.query_profit_data(code=code, year=year, quarter=quarter).get_data()
        growth_df = bs.query_growth_data(code=code, year=year, quarter=quarter).get_data()
        balance_df = bs.query_balance_data(code=code, year=year, quarter=quarter).get_data()
        cash_df = bs.query_cash_flow_data(code=code, year=year, quarter=quarter).get_data()

        if not profit_df.empty:
            p = profit_df.iloc[0]
            g = growth_df.iloc[0] if not growth_df.empty else {}
            b = balance_df.iloc[0] if not balance_df.empty else {}
            c = cash_df.iloc[0] if not cash_df.empty else {}

            roe_val = float(p.get('roeAvg', 0) or 0)
            net_profit_val = float(p.get('netProfit', 0) or 0)
            cfo_ratio = float(c.get('CFOToNP', 0) or 0)

            # 严格按照列名顺序打包 tuple (共13个)
            data_tuple = (
                code, p.get('statDate', '未知'), p.get('pubDate', '未知'),
                roe_val, 
                float(g.get('YOYPNI', 0) or 0), 
                float(p.get('npMargin', 0) or 0),
                float(p.get('gpMargin', 0) or 0),
                float(p.get('epsTTM', 0) or 0), 
                net_profit_val,
                float(p.get('MBRevenue', 0) or 0),
                # 将 Baostock 错误的 0.003173 放大 100 倍，还原成正常的 0.3173 (即 31.73%)
                float(b.get('liabilityToAsset', 0) or 0) * 100,
                net_profit_val * cfo_ratio,                   # ✅ 推算出现金流绝对值
                datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
            )
            return {'code': code, 'status': 'success', 'data': data_tuple, 'roe': roe_val}
        return {'code': code, 'status': 'no_data'}
    except Exception as e:
        return {'code': code, 'status': 'error', 'msg': str(e)}

def sync_financial_factors(limit=None):
    init_financial_table()
    blacklist = load_blacklist()
    completed = load_completed() 
    
    conn = sqlite3.connect(DB_PATH)
    recent_date = (datetime.now() - timedelta(days=ACTIVE_DAYS)).strftime('%Y-%m-%d')
    active_df = pd.read_sql_query(f"SELECT DISTINCT code FROM daily_k_data WHERE date >= '{recent_date}' AND volume > 0", conn)
    stock_codes = [c for c in active_df['code'].tolist() if c not in blacklist and c not in completed]
    
    if limit: stock_codes = stock_codes[:limit]
    total_tasks = len(stock_codes)
    if total_tasks == 0:
        print("✅ 财务数据已是最新，无需处理。")
        conn.close()
        return

    print(f"🚀 启动 {PROCESS_COUNT} 个进程，拉取 {total_tasks} 只股票财务数据...")
    cursor = conn.cursor()
    start_time = time.time()
    
    # 💥 显式指定列名，彻底无视数据库物理字段顺序
    insert_sql = """
        INSERT OR REPLACE INTO financial_factors 
        (code, stat_date, pub_date, roe_avg, yoy_profit_growth, np_margin, 
         gp_margin, eps_ttm, net_profit, mb_revenue, liability_ratio, cash_flow, update_date) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    
    # 改为 'a' (追加模式)，保护已完成的断点记录不被清空
    with open(COMPLETED_FILE, 'a', encoding='utf-8') as f_comp: 
        with Pool(processes=PROCESS_COUNT, initializer=worker_init) as pool:
            for idx, result in enumerate(pool.imap_unordered(fetch_single_stock, stock_codes), 1):
                if result['status'] == 'success':
                    cursor.execute(insert_sql, result['data'])
                    conn.commit()
                    f_comp.write(f"{result['code']},{datetime.now().isoformat()}\n")
                    f_comp.flush()
                    res_msg = f"✅ ROE: {result['roe']:.2f}"
                elif result['status'] == 'no_data':
                    res_msg = "⚠️ 暂无财报数据"
                else:
                    res_msg = f"❌ 错误: {result['msg'][:20]}"
                
                # 打印进度
                elapsed = time.time() - start_time
                eta = str(timedelta(seconds=int((elapsed/idx)*(total_tasks-idx))))
                print(f"[{idx}/{total_tasks}] {result['code']} {res_msg} | ETA: {eta}")
    
    conn.close()
    bs.logout()

# ==========================================
# 🌟 暴露给 main.py 的统一入口 (带交互确认)
# ==========================================
def run_factor_sync():
    """带交互确认的同步入口"""
    try:
        user_input = input("\n>> 是否更新全量财务数据？(这将会全量覆盖以补充新指标) (Y/n) [默认Y]: ").strip().lower()
        if user_input in ['', 'y', 'yes']:
            sync_financial_factors()
        else:
            print("⏭️ 跳过财务因子同步。")
    except KeyboardInterrupt:
        print("\n👋 用户取消同步。")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run_factor_sync()