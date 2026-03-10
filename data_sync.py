import baostock as bs
import pandas as pd
import sqlite3
import requests
import time
import os
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

try:
    from config import DB_PATH, CORE_INDICES, BLACKLIST_FILE
except ImportError:
    DB_PATH = "quant_data.db"
    BLACKLIST_FILE = "blacklist.txt"
    CORE_INDICES = ['sh.000001', 'sz.399001', 'sz.399107', 'sh.000300', 'sz.399006', 'sh.000905', 'sh.000852', 'bj.899050']

def get_db_conn():
    return sqlite3.connect(DB_PATH)

def load_blacklist():
    if not os.path.exists(BLACKLIST_FILE): return set()
    with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def get_todo_list(target_date, blacklist):
    """【真·全市场动态对齐】通过花名册 Diff 机制，自动捕捉新股并彻底过滤垃圾指数"""
    conn = get_db_conn()
    sql = "SELECT code, MAX(date) as max_date FROM daily_k_data GROUP BY code"
    df_db = pd.read_sql_query(sql, conn)
    db_progress = dict(zip(df_db['code'], df_db['max_date']))
    conn.close()

    print(f"📡 正在向交易所请求 {target_date} 的全市场动态花名册...")
    bs.login() 
    rs = bs.query_all_stock(day=target_date)
    all_active_codes = []
    
    # 💥 核心白名单：只放行沪深纯正A股
    valid_prefixes = ('sh.6', 'sz.00', 'sz.30')
    
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(valid_prefixes) or code in CORE_INDICES:
            all_active_codes.append(code)
            
    bs.logout()
    
    if not all_active_codes:
        print("⚠️ 花名册请求失败，降级为本地存量更新模式...")
        all_active_codes = [c for c in db_progress.keys() if c.startswith(valid_prefixes)] + CORE_INDICES

    todo_list = []
    for code in all_active_codes:
        # 💥 救命补丁：如果是 bj 或 399，必须检查它是不是 VIP 指数。只有不是 VIP 的才杀！
        if (code.startswith('bj.') or code.startswith('sz.399')) and (code not in CORE_INDICES):
            continue
            
        if code in blacklist:
            continue
            
        last_date = db_progress.get(code)
        if last_date is None or last_date < target_date:
            todo_list.append(code)
            
    # 保底：无论如何，VIP 指数必须检查
    for idx in CORE_INDICES:
        if idx not in todo_list and idx not in blacklist:
            last_date = db_progress.get(idx)
            if last_date is None or last_date < target_date:
                todo_list.append(idx)

    return list(set(todo_list))

def fetch_eastmoney_kline(code, start_date, end_date):
    """💥 专门为 Baostock 不支持的指数（如北证50）开的东方财富小灶"""
    if code == 'bj.899050':
        secid = "0.899050"
    else:
        return []
        
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": "20500101",
        "lmt": "200"  # 拉取最近 200 天，足够覆盖各种断点
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        if not data.get("data") or not data["data"].get("klines"):
            return []
            
        klines = data["data"]["klines"]
        result = []
        for k in klines:
            parts = k.split(',')
            date = parts[0]
            if not (start_date <= date <= end_date):
                continue
                
            open_val, close_val, high_val, low_val = parts[1], parts[2], parts[3], parts[4]
            vol, amount, pct_chg = parts[5], parts[6], parts[8]
            turn = parts[10] if parts[10] != '-' else '0'
            
            # 严格对齐 daily_k_data 表结构: (date, code, open, high, low, close, volume, amount, turn, pctChg)
            result.append((date, code, open_val, high_val, low_val, close_val, vol, amount, turn, pct_chg))
        return result
    except Exception as e:
        print(f"⚠️ 东方财富 API 请求 {code} 失败: {e}")
        return []

def worker_init():
    bs.login()

def sync_single_stock(args):
    code, start_date, end_date = args
    try:
        # 💥 拦截分流：如果是北交所，走特权通道
        if code == 'bj.899050':
            data_list = fetch_eastmoney_kline(code, start_date, end_date)
        else:
            # 正常 A股走 Baostock
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,volume,amount,turn,pctChg",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="2"
            )
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
        
        return {'code': code, 'status': 'success', 'data': data_list}
    except Exception as e:
        return {'code': code, 'status': 'error', 'msg': str(e)}

def run_kline_sync():
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_k_data (
            date TEXT, code TEXT, open REAL, high REAL, low REAL, close REAL,
            volume REAL, amount REAL, turn REAL, pctChg REAL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.commit()
    
    target_date = datetime.now().strftime('%Y-%m-%d')
    blacklist = load_blacklist()
    todo_list = get_todo_list(target_date, blacklist)
    
    if not todo_list:
        print("✅ 所有 K 线数据均已是最新，无需同步。")
        conn.close()
        return

    print(f"🚀 开始同步 {len(todo_list)} 只标的 K 线数据...")
    
    sql = "SELECT code, MAX(date) as max_date FROM daily_k_data GROUP BY code"
    df_db = pd.read_sql_query(sql, conn)
    db_progress = dict(zip(df_db['code'], df_db['max_date']))
    
    tasks = []
    for code in todo_list:
        last_date = db_progress.get(code)
        if last_date:
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        tasks.append((code, start_date, target_date))
        
    insert_sql = "INSERT OR REPLACE INTO daily_k_data VALUES (?,?,?,?,?,?,?,?,?,?)"
    process_count = min(8, cpu_count() * 2)
    start_time = time.time()
    
    with Pool(processes=process_count, initializer=worker_init) as pool:
        for idx, result in enumerate(pool.imap_unordered(sync_single_stock, tasks), 1):
            if result['status'] == 'success' and result['data']:
                cursor.executemany(insert_sql, result['data'])
                conn.commit()
                res_msg = f"✅ 更新了 {len(result['data'])} 条"
            elif result['status'] == 'success':
                res_msg = "⚠️ 无新数据"
            else:
                res_msg = f"❌ 失败: {result['msg'][:20]}"
            
            elapsed = time.time() - start_time
            eta = str(timedelta(seconds=int((elapsed/idx)*(len(tasks)-idx))))
            print(f"[{idx}/{len(tasks)}] {result['code']} {res_msg} | ETA: {eta}")
            
    conn.close()
    bs.logout()
    print("🎉 K线数据同步完成！")

if __name__ == "__main__":
    run_kline_sync()