import baostock as bs
import pandas as pd
import sqlite3
import requests
import time
import os
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from config import DB_PATH, CORE_INDICES, BLACKLIST_FILE, DAILY_K_DAYS

def get_db_conn():
    return sqlite3.connect(DB_PATH)

def load_blacklist():
    if not os.path.exists(BLACKLIST_FILE): return set()
    with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def get_todo_list(target_date, blacklist):
    """【真·全市场动态对齐】通过花名册 Diff 机制，自动捕捉新股并彻底过滤垃圾指数"""
    conn = get_db_conn()
    
    # 1. 读取 K 线表已有进度 (断点续传与日常增量核心)
    sql = "SELECT code, MAX(date) as max_date FROM daily_k_data GROUP BY code"
    try:
        df_db = pd.read_sql_query(sql, conn)
        db_progress = dict(zip(df_db['code'], df_db['max_date']))
    except Exception:
        db_progress = {} # 表不存在或为空时容错

    # 2. 尝试：向交易所请求当日最新花名册
    print(f"📡 正在向交易所请求 {target_date} 的全市场动态花名册...")
    bs.login()
    rs = bs.query_all_stock(day=target_date)
    stock_list = []
    while (rs.error_code == '0') and rs.next():
        stock_list.append(rs.get_row_data()[0])
    bs.logout()

    # 💥 核心修复：双重降级保险！如果交易所抽风，直接读取本地 stock_basic 表！
    if not stock_list:
        print("⚠️ 花名册请求失败，智能降级为读取本地 stock_basic 画像库名单...")
        try:
            df_basic = pd.read_sql_query("SELECT code FROM stock_basic", conn)
            stock_list = df_basic['code'].tolist()
        except Exception:
            stock_list = list(db_progress.keys()) # 终极兜底
            
    conn.close()

    # 3. 将核心指数和全市场股票合并，并执行【严格无菌清洗】
    raw_roster = set(CORE_INDICES + stock_list)
    full_roster = set()
    
    for c in raw_roster:
        clean_c = str(c).strip()
        if len(clean_c) == 9 and clean_c[2] == '.':
            full_roster.add(clean_c)

    # 4. 过滤黑名单
    todo_list = [c for c in full_roster if c not in blacklist]
    
    tasks = []
    for code in todo_list:
        last_date = db_progress.get(code)
        if last_date:
            # 🚀 日常增量绝技：直接从本地日期的下一天开始索要数据
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            # 如果是今天新上的 IPO 新股，往前回溯配置的天数
            start_date = (datetime.now() - timedelta(days=DAILY_K_DAYS)).strftime('%Y-%m-%d')
            
        # 防止越界：如果 start_date 还没超过 target_date，才需要更新
        if start_date <= target_date:
            tasks.append((code, start_date, target_date))
            
    return tasks

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
        "lmt": "10000"
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
        if code == 'bj.899050':
            data_list = fetch_eastmoney_kline(code, start_date, end_date)
        else:
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

    # 自动建立时间轴加速索引！
    # 使用 IF NOT EXISTS，保证哪怕后续重复执行这个脚本，也不会报错
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_kdata_date ON daily_k_data(date DESC);
    ''')

    conn.commit()
    
    target_date = datetime.now().strftime('%Y-%m-%d')
    blacklist = load_blacklist()
    
    tasks = get_todo_list(target_date, blacklist)
    
    if not tasks:
        print("✅ 所有 K 线数据均已是最新，当前无需同步。")
        conn.close()
        return

    print(f"🚀 开始日常增量同步 {len(tasks)} 只标的 K 线数据...")
        
    insert_sql = "INSERT OR REPLACE INTO daily_k_data VALUES (?,?,?,?,?,?,?,?,?,?)"
    process_count = min(8, cpu_count() * 2)
    start_time = time.time()
    
    # ==========================================
    # 💥 高频写库优化：内存缓冲池 (Buffer Batching)
    # ==========================================
    data_buffer = []
    BUFFER_SIZE = 1000  # 满 5000 条记录才执行一次磁盘 commit
    
    try:
        with Pool(processes=process_count, initializer=worker_init) as pool:
            for idx, result in enumerate(pool.imap_unordered(sync_single_stock, tasks), 1):
                if result['status'] == 'success' and result['data']:
                    data_buffer.extend(result['data'])
                    res_msg = f"✅ 缓存了 {len(result['data'])} 条"
                elif result['status'] == 'success':
                    res_msg = "⚠️ 无新数据"
                else:
                    res_msg = f"❌ 失败: {result['msg'][:20]}"
                
                # 缓冲池满了，执行一次批量落库，保护硬盘
                if len(data_buffer) >= BUFFER_SIZE:
                    cursor.executemany(insert_sql, data_buffer)
                    conn.commit()
                    data_buffer = [] # 清空缓冲池
                
                elapsed = time.time() - start_time
                eta = str(timedelta(seconds=int((elapsed/idx)*(len(tasks)-idx))))
                print(f"[{idx}/{len(tasks)}] {result['code']} {res_msg} | ETA: {eta}")
                
    except KeyboardInterrupt:
        # 💥 紧急抢救气囊：当按 Ctrl+C 强杀时触发
        print("\n🚨🚨🚨 收到人工中止信号 (Ctrl+C)！正在执行内存紧急抢救...")
        
    finally:
        # 💥 无论正常跑完还是被强杀，都会执行这里把剩下的尾巴落库
        if data_buffer:
            cursor.executemany(insert_sql, data_buffer)
            conn.commit()
            print(f"💾 紧急抢救成功/尾部数据落库！已将内存中最后的 {len(data_buffer)} 条数据安全落库。")
            
        conn.close()
        print("🏁 K线数据同步进程已安全终止。")

if __name__ == "__main__":
    run_kline_sync()