import os
# 💥 物理封印底层多线程，防止与 Python 多进程发生“线程爆炸”卡死 CPU
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import baostock as bs
import pandas as pd
import sqlite3
import requests
import time
import socket
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
    """【真·全市场动态对齐】通过花名册 Diff 机制，自动捕捉新股、过滤基金、并斩杀退市股"""
    conn = get_db_conn()
    
    # 💥 必须带上 close 字段，供后续重叠日比对复权变化
    sql = """
        SELECT a.code, a.date as max_date, a.close
        FROM daily_k_data a
        INNER JOIN (
            SELECT code, MAX(date) as max_date
            FROM daily_k_data
            GROUP BY code
        ) b ON a.code = b.code AND a.date = b.max_date
    """
    try:
        df_db = pd.read_sql_query(sql, conn)
        db_progress = {row['code']: {'max_date': row['max_date'], 'close': row['close']} for _, row in df_db.iterrows()}
    except Exception:
        db_progress = {} 

    print(f"📡 正在向交易所请求 {target_date} 的全市场动态花名册...")
    lg = bs.login()
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)
    if lg.error_code == "10001011":
        print("❌ IP已经加入黑名单, 需要去QQ群里求助解封！")
        
    rs = bs.query_all_stock(day=target_date)
    stock_list = []
    delisted_codes = set() # 💥 隔离区：专门用来关押退市股
    
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        code_name = row[2]
        
        # ==========================================
        # 💥 斩杀退市股烦恼：只要名字带“退”，直接打入死牢
        # ==========================================
        if '退' in code_name:
            delisted_codes.add(code)
        else:
            stock_list.append(code)
            
    bs.logout()

    if not stock_list:
        print("⚠️ 花名册请求失败，智能降级为读取本地 stock_basic 画像库名单...")
        try:
            df_basic = pd.read_sql_query("SELECT code, name FROM stock_basic", conn)
            for _, row in df_basic.iterrows():
                name_str = str(row['name'])
                if '退' in name_str:
                    delisted_codes.add(row['code'])
                else:
                    stock_list.append(row['code'])
        except Exception:
            stock_list = list(db_progress.keys()) 
            
    conn.close()

    raw_roster = set(CORE_INDICES + stock_list)
    full_roster = set()
    
    for c in raw_roster:
        clean_c = str(c).strip()
        if len(clean_c) == 9 and clean_c[2] == '.':
            # 物理拦截场内基金
            if clean_c.startswith('sh.5') or clean_c.startswith('sz.1'):
                continue
                
            # 💥 物理拦截退市股，不再为它们浪费任何 API 请求
            if clean_c in delisted_codes:
                continue
                
            full_roster.add(clean_c)

    todo_list = [c for c in full_roster if c not in blacklist]
    
    tasks = []
    for code in todo_list:
        info = db_progress.get(code)
        if info:
            last_date = info['max_date']
            db_close = info['close']
            start_date = last_date
        else:
            last_date = None
            db_close = None
            start_date = (datetime.now() - timedelta(days=DAILY_K_DAYS)).strftime('%Y-%m-%d')
            
        if start_date <= target_date:
            tasks.append({
                'code': code,
                'start_date': start_date,
                'end_date': target_date,
                'last_date': last_date,
                'db_close': db_close
            })
            
    return tasks

def fetch_eastmoney_kline(code, start_date, end_date):
    """专门为 Baostock 不支持的指数（如北证50）开的东方财富小灶"""
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
    # 💥 给底层的所有的网络请求强加 30 秒超时物理锁！
    # 如果 Baostock 服务器 30 秒不回话，直接抛出 timeout 异常打断假死，释放进程
    socket.setdefaulttimeout(30.0) 
    bs.login()

def sync_single_stock(task):
    code = task['code']
    start_date = task['start_date']
    end_date = task['end_date']
    last_date = task['last_date']
    db_close = task['db_close']
    need_full_reload = False
    
    try:
        if code == 'bj.899050':
            data_list = fetch_eastmoney_kline(code, start_date, end_date)
        else:
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,volume,amount,turn,pctChg",
                start_date=start_date, end_date=end_date,
                # 💥 还原为前复权！
                frequency="d", adjustflag="2"  
            )
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
                
        if not data_list:
            return {'code': code, 'status': 'success', 'msg': '无新数据', 'data': [], 'warnings': []}

        df = pd.DataFrame(data_list, columns=["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg"])
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # ==========================================
        # 💥 除权自愈校验逻辑
        # ==========================================
        if last_date is not None and db_close is not None:
            overlap_row = df[df['date'] == last_date]
            if not overlap_row.empty:
                fetched_close = float(overlap_row.iloc[0]['close'])
                if abs(fetched_close - db_close) > 0.01:
                    need_full_reload = True
                    print(f"\n   🔄 触发自愈：{code} 发生除权/复权变化 (本地:{db_close} 最新:{fetched_close})，全量重载...")
                else:
                    df = df[df['date'] > last_date]
            else:
                df = df[df['date'] > last_date]

        # ==========================================
        # 💥 触发全量重载
        # ==========================================
        if need_full_reload:
            try:
                conn = get_db_conn()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM daily_k_data WHERE code = ?", (code,))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Exception: 清除旧数据失败 {code} - {e}")
                
            full_start = (datetime.now() - timedelta(days=DAILY_K_DAYS)).strftime('%Y-%m-%d')
            
            if code == 'bj.899050':
                full_data_list = fetch_eastmoney_kline(code, full_start, end_date)
            else:
                rs_full = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,volume,amount,turn,pctChg",
                    start_date=full_start, end_date=end_date,
                    # 💥 还原为前复权！
                    frequency="d", adjustflag="2"  
                )
                full_data_list = []
                while (rs_full.error_code == '0') and rs_full.next():
                    full_data_list.append(rs_full.get_row_data())
            
            if not full_data_list:
                return {'code': code, 'status': 'success', 'msg': '无新数据(重拉后)', 'data': [], 'warnings': []} # 👈 补全空 warnings
                
            df = pd.DataFrame(full_data_list, columns=["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg"])
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if df.empty:
            return {'code': code, 'status': 'success', 'msg': '无新数据', 'data': [], 'warnings': []} # 👈 补全空 warnings
        
        # ==========================================
        # 💥 物理常识预警：扫描异常暴跌（跌幅超 31%）
        # ==========================================
        warnings = []
        abnormal_drops = df[df['pctChg'] < -31.0]
        if not abnormal_drops.empty:
            for _, row in abnormal_drops.iterrows():
                warnings.append(f"⚠️ {code} 在 {row['date']} 跌幅达 {row['pctChg']:.2f}% (收盘价: {row['close']})")

        records = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']].values.tolist()
        return {'code': code, 'status': 'success', 'msg': 'ok', 'data': records, 'warnings': warnings} # 👈 返回警告列表
        
    except Exception as e:
        return {'code': code, 'status': 'error', 'msg': str(e), 'warnings': []} # 👈 补全空 warnings

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

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_kdata_date ON daily_k_data(date DESC);
    ''')
    conn.commit()
    
    # ==========================================
    # 💥 抢救 V1.0 的黄金逻辑：加装 18:30 物理时间屏障
    # 防范 Baostock 复权因子未入库导致的脏数据污染
    # ==========================================
    now = datetime.now()
    if now.hour < 18 or (now.hour == 18 and now.minute < 30):
        # 18:30 之前，目标日期强制退回昨天
        target_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"🛡️ [时间屏障] 当前未到 18:30 (复权因子尚未就绪)。安全起见，同步目标日退回至: {target_date}")
    else:
        # 18:30 之后，允许拉取今日最新复权数据
        target_date = now.strftime('%Y-%m-%d')
    
    blacklist = load_blacklist()
    
    tasks = get_todo_list(target_date, blacklist)
    
    if not tasks:
        print(f"✅ 所有 K 线数据均已对齐至 {target_date}，当前无需同步。")
        conn.close()
        return

    print(f"🚀 开始日常增量同步 {len(tasks)} 只标的 K 线数据 (目标日: {target_date})...")
        
    insert_sql = "INSERT OR REPLACE INTO daily_k_data VALUES (?,?,?,?,?,?,?,?,?,?)"
    process_count = min(8, cpu_count() * 2)
    start_time = time.time()
    
    data_buffer = []
    BUFFER_SIZE = 1000
    all_warnings = [] # 💥 新增：全局警告收集器
    
    try:
        with Pool(processes=process_count, initializer=worker_init) as pool:
            for idx, result in enumerate(pool.imap_unordered(sync_single_stock, tasks), 1):
                # 💥 收集警告信息
                if result.get('warnings'):
                    all_warnings.extend(result['warnings'])
                if result['status'] == 'success' and result['data']:
                    data_buffer.extend(result['data'])
                    res_msg = f"✅ 缓存了 {len(result['data'])} 条"
                elif result['status'] == 'success':
                    res_msg = "⚠️ 无新数据"
                else:
                    res_msg = f"❌ 失败: {result['msg'][:20]}"
                
                if len(data_buffer) >= BUFFER_SIZE:
                    cursor.executemany(insert_sql, data_buffer)
                    conn.commit()
                    data_buffer = [] 
                
                elapsed = time.time() - start_time
                eta = str(timedelta(seconds=int((elapsed/idx)*(len(tasks)-idx))))
                print(f"[{idx}/{len(tasks)}] {result['code']} {res_msg} | ETA: {eta}")
                
    except KeyboardInterrupt:
        print("\n🚨🚨🚨 收到人工中止信号 (Ctrl+C)！正在执行内存紧急抢救...")
        
    finally:
        if data_buffer:
            cursor.executemany(insert_sql, data_buffer)
            conn.commit()
            print(f"💾 紧急抢救成功/尾部数据落库！已将内存中最后的 {len(data_buffer)} 条数据安全落库。")
            
        conn.close()
        # ==========================================
        # 💥 巡检报告：集中展示异常跌幅
        # ==========================================
        if all_warnings:
            print("\n" + "!" * 80)
            print("🚨 发现以下股票存在异常暴跌（单日跌幅 > 35%），请人工核实是否为 Baostock 脏数据：")
            for w in all_warnings:
                print(f"   {w}")
            print("-" * 80)
            print("💡 提示：如果确认为 Baostock 未复权脏数据，请进入 SQLite 清理，例如：")
            print("   DELETE FROM daily_k_data WHERE date='20xx-xx-xx' AND code='sx.xxxxxx' limit 1;")
            print("!" * 80 + "\n")
            
        print("🏁 K线数据同步进程已安全终止。")

if __name__ == "__main__":
    run_kline_sync()