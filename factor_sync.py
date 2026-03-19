import baostock as bs
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import re
from multiprocessing import Pool, cpu_count

# 💥 快速失败机制：直接强制导入
from config import DB_PATH, FINANCIAL_QUARTERS, COMPLETED_FILE, EXPIRE_DAYS

# ==========================================
# 🛠️ 辅助魔法：时序集合换算
# ==========================================
def stat_date_to_yq(date_str):
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return (dt.year, (dt.month - 1) // 3 + 1)
    except:
        return None

def get_target_quarters(num_quarters=FINANCIAL_QUARTERS):
    now = datetime.now()
    year = now.year
    month = now.month
    
    if month <= 4: 
        year -= 1; quarter = 4
    elif month <= 8: quarter = 1
    elif month <= 10: quarter = 2
    else: quarter = 3
        
    targets = set()
    for _ in range(num_quarters):
        targets.add((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4; year -= 1
    return targets

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_factors (
            code TEXT, stat_date TEXT, pub_date TEXT, roe_avg REAL, yoy_profit_growth REAL,
            np_margin REAL, gp_margin REAL, eps_ttm REAL, net_profit REAL, mb_revenue REAL,
            update_date TEXT, liability_ratio REAL, cash_flow REAL, gross_margin REAL, 
            net_margin REAL, cfo_to_np REAL, cfo_to_gr REAL, inv_turn_days REAL, 
            nr_turn_days REAL, yoy_pni REAL, total_share REAL,
            PRIMARY KEY (code, stat_date, pub_date)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY, name TEXT, industry TEXT, industry_classification TEXT
        )
    ''')
    conn.commit()
    conn.close()

def sync_stock_basic():
    print("📡 正在同步全市场股票基础信息(行业分类)...")
    bs.login()
    rs = bs.query_stock_industry()
    if rs.error_code != '0':
        print(f"⚠️ 行业信息获取失败: {rs.error_msg}")
        bs.logout()
        return

    basic_data = []
    while rs.next():
        row = rs.get_row_data()
        code = row[1]
        if code.startswith(('sh.6', 'sz.0', 'sz.30')):
            name = row[2]
            raw_industry = row[3] 
            match = re.match(r'^([A-Za-z0-9]+)(.*)$', raw_industry)
            if match:
                ind_code = match.group(1); ind_name = match.group(2) 
            else:
                ind_code = "未知"; ind_name = raw_industry if raw_industry else "未知"
            basic_data.append((code, name, ind_name, ind_code))
    bs.logout()

    if basic_data:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO stock_basic (code, name, industry, industry_classification)
            VALUES (?, ?, ?, ?)
        ''', basic_data)
        conn.commit()
        conn.close()
        print(f"✅ 成功更新 {len(basic_data)} 只股票的基础行业画像！")

def load_progress():
    progress = {}
    if os.path.exists(COMPLETED_FILE):
        with open(COMPLETED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                parts = line.split(',')
                if len(parts) >= 2: progress[parts[0]] = parts[1]
                else: progress[parts[0]] = "2000-01-01 00:00:00"
    return progress

def save_progress(progress_dict):
    with open(COMPLETED_FILE, "w", encoding="utf-8") as f:
        for code, ts in progress_dict.items():
            f.write(f"{code},{ts}\n")

# ==========================================
# 🚀 多进程工作器配置
# ==========================================
def worker_init():
    """每个进程单独登录，打碎长连接，防止被 T"""
    bs.login()

def fetch_worker(args):
    """单独一个进程负责拉取一只股票的数据"""
    code, quarters_set = args
    now = datetime.now()
    results = []
    
    try:
        for year, quarter in sorted(quarters_set, reverse=True):
            profit_df = bs.query_profit_data(code=code, year=year, quarter=quarter).get_data()
            
            if profit_df is not None and not profit_df.empty:
                growth_df = bs.query_growth_data(code=code, year=year, quarter=quarter).get_data()
                operation_df = bs.query_operation_data(code=code, year=year, quarter=quarter).get_data()
                cash_flow_df = bs.query_cash_flow_data(code=code, year=year, quarter=quarter).get_data()
                
                def safe_float(df, col, default=0.0):
                    if df is not None and not df.empty and col in df.columns:
                        val = df[col].iloc[0]
                        try: return float(val) if val else default
                        except: return default
                    return default

                def safe_str(df, col, default=""):
                    if df is not None and not df.empty and col in df.columns:
                        val = df[col].iloc[0]
                        return str(val) if val else default
                    return default
                
                net_profit = safe_float(profit_df, 'netProfit')
                cfo_to_np = safe_float(cash_flow_df, 'CFOToNP')
                cash_flow = net_profit * cfo_to_np if cfo_to_np != 0 else 0.0
                    
                data = (
                    code, safe_str(profit_df, 'statDate'), safe_str(profit_df, 'pubDate'),
                    now.strftime('%Y-%m-%d %H:%M:%S'), safe_float(profit_df, 'roeAvg'),
                    safe_float(growth_df, 'YOYNI'), net_profit, safe_float(profit_df, 'epsTTM'),
                    cash_flow, safe_float(profit_df, 'MBRevenue'), safe_float(profit_df, 'totalShare'),
                    safe_float(profit_df, 'liabRatio'), safe_float(profit_df, 'gpMargin'),    
                    safe_float(profit_df, 'npMargin'), cfo_to_np, safe_float(cash_flow_df, 'CFOToGr'),       
                    safe_float(operation_df, 'INVTurnDays'), safe_float(operation_df, 'NRTurnDays'),   
                    safe_float(growth_df, 'YOYPNI')
                )
                results.append(data)
        
        return {'code': code, 'status': 'success', 'data': results}
    except Exception as e:
        return {'code': code, 'status': 'error', 'msg': str(e)}

# ==========================================
# 🎯 主控调度中心
# ==========================================
def run_factor_sync(auto_confirm=False):
    init_db()
    sync_stock_basic()

    print("📡 正在获取 A股 股票列表...")
    bs.login()
    rs = bs.query_stock_basic()
    stock_list = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(('sh.6', 'sz.0', 'sz.30')):
            stock_list.append(code)
    bs.logout()

    print("📡 正在全盘扫描本地数据库，执行时空集合比对与空洞探测...")
    target_set = get_target_quarters(FINANCIAL_QUARTERS)
    
    conn = sqlite3.connect(DB_PATH)
    try:
        df_existing = pd.read_sql_query("SELECT code, stat_date FROM financial_factors", conn)
        db_inventory = {}
        for _, row in df_existing.iterrows():
            code = row['code']
            yq = stat_date_to_yq(row['stat_date'])
            if yq:
                if code not in db_inventory: db_inventory[code] = set()
                db_inventory[code].add(yq)
    except Exception:
        db_inventory = {}

    progress = load_progress()
    now = datetime.now()
    todo_dict = {} 
    
    for code in stock_list:
        existing_set = db_inventory.get(code, set())
        missing_set = target_set - existing_set 
        
        # 🛡️ 核心：【次新股免疫盾】(跑完历史回测后，取消注释以恢复该功能)
        # if existing_set:
        #     min_existing = min(existing_set)
        #     missing_set = {mq for mq in missing_set if mq >= min_existing}
            
        if not missing_set: continue 
        
        last_sync_time = progress.get(code)
        is_expired = True
        if last_sync_time:
            try:
                last_ts = datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S")
                is_expired = (now - last_ts).days >= EXPIRE_DAYS
            except: pass
                
        if is_expired:
            todo_dict[code] = missing_set

    total = len(todo_dict)
    if total == 0:
        print("✅ 本地数据检查完毕，当前无需网络请求！")
        conn.close()
        return

    print(f"\n📊 审计完毕：共 {total} 只股票需要填补历史空洞。")
    if not auto_confirm:
        if input("❓ 是否开始定点填补空洞与更新？ [默认回车继续] (Y/n): ").strip().lower() == 'n':
            conn.close()
            return

    # ==========================================
    # 💥 多进程并发执行区
    # ==========================================
    cursor = conn.cursor()
    start_time = time.time()
    
    # 组装任务池
    tasks = list(todo_dict.items())
    
    # 计算安全的并发数 (最大不要超过 16，怕把 Baostock 挤爆)
    process_count = min(16, cpu_count() * 2, total)
    print(f"🚀 启动 {process_count} 个子进程开始狂奔...")

    insert_sql = '''
        INSERT OR REPLACE INTO financial_factors (
            code, stat_date, pub_date, update_date, roe_avg, yoy_profit_growth, net_profit, 
            eps_ttm, cash_flow, mb_revenue, total_share, liability_ratio,
            gp_margin, np_margin, cfo_to_np, cfo_to_gr,
            inv_turn_days, nr_turn_days, yoy_pni
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    # 使用进程池发起猛攻
    with Pool(processes=process_count, initializer=worker_init) as pool:
        for idx, result in enumerate(pool.imap_unordered(fetch_worker, tasks), 1):
            code = result['code']
            if result['status'] == 'success' and result['data']:
                cursor.executemany(insert_sql, result['data'])
                conn.commit()
                res_msg = f"✅ 成功补入 {len(result['data'])} 季"
            elif result['status'] == 'success':
                res_msg = "⚠️ 暂无发布数据"
            else:
                res_msg = f"❌ 失败: {result['msg'][:20]}"

            # 无论成功失败，进入冷却期
            progress[code] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 定期保存进度，防止中途崩溃
            if idx % 50 == 0 or idx == total:
                save_progress(progress)

            elapsed = time.time() - start_time
            eta_seconds = (elapsed / idx) * (total - idx)
            eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
            
            print(f"[{idx}/{total} | ETA: {eta_str}] {code} {res_msg}")
            
    conn.close()
    print("🎉 全市场历史空洞填补与财务护城河更新完毕！")

if __name__ == '__main__':
    run_factor_sync(auto_confirm=False)