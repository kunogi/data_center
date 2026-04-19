import baostock as bs
import sqlite3
import pandas as pd
from datetime import datetime
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
    
    # 💥 修复：精准适配 A 股财报披露日历
    if month <= 3: 
        # 1-3月：最新只能是去年的年报(Q4)
        year -= 1
        quarter = 4
    elif month <= 6: 
        # 4-6月：最新是一季报(Q1)。(代码会自动往前推算包含4月份同步披露的去年Q4)
        quarter = 1
    elif month <= 9: 
        # 7-9月：最新是中报(Q2)
        quarter = 2
    else: 
        # 10-12月：最新是三季报(Q3)
        quarter = 3
        
    targets = set()
    for _ in range(num_quarters):
        targets.add((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return targets

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_factors (
            code TEXT, stat_date TEXT, pub_date TEXT, roe_avg REAL, yoy_profit_growth REAL,
            np_margin REAL, gp_margin REAL, eps_ttm REAL, net_profit REAL, mb_revenue REAL,
            update_date TEXT, cash_flow REAL, gross_margin REAL, 
            net_margin REAL, cfo_to_np REAL, cfo_to_gr REAL, yoy_pni REAL, total_share REAL,
            PRIMARY KEY (code, stat_date, pub_date)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY, name TEXT, industry TEXT, industry_classification TEXT,
            list_date TEXT
        )
    ''')
    conn.commit()
    conn.close()

def sync_stock_basic():
    print("📡 正在同步全市场股票基础信息(行业分类与上市时间) [耗时约1分钟，请耐心等待]...")
          
    lg = bs.login()
    if lg.error_code != '0':
        print(f"⚠️ 登录失败: error_code={lg.error_code}, error_msg={lg.error_msg}")
        if lg.error_code == "10001011":
            print("❌ IP已经加入黑名单, 需要去QQ群里求助解封！")
        return

    # 1. 先抓取行业字典
    rs_ind = bs.query_stock_industry()
    if rs_ind.error_code != '0':
        print(f"⚠️ 行业信息获取失败: {rs_ind.error_msg}")
        bs.logout()
        return

    ind_dict = {}
    while rs_ind.next():
        row = rs_ind.get_row_data()
        code = row[1]
        raw_industry = row[3] 
        match = re.match(r'^([A-Za-z0-9]+)(.*)$', raw_industry)
        if match:
            ind_code = match.group(1); ind_name = match.group(2) 
        else:
            ind_code = "未知"; ind_name = raw_industry if raw_industry else "未知"
        ind_dict[code] = (ind_name, ind_code)

    # 2. 再抓取上市时间，并在内存中进行完美合并 (Join)
    rs_basic = bs.query_stock_basic(code="")
    if rs_basic.error_code != '0':
        print(f"⚠️ 上市时间获取失败: {rs_basic.error_msg}")
        bs.logout()
        return

    basic_data = []
    while rs_basic.next():
        row = rs_basic.get_row_data()
        code = row[0]
        # 极简前缀匹配：兼容所有现有及未来的沪深 A 股，天然拦截北交所与B股
        if code.startswith(('sh.6', 'sz.0', 'sz.3')):
            name = row[1]
            list_date = row[2] # ipoDate
            ind_name, ind_code = ind_dict.get(code, ("未知", "未知"))
            basic_data.append((code, name, ind_name, ind_code, list_date))
            
    bs.logout()

    if basic_data:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO stock_basic (code, name, industry, industry_classification, list_date)
            VALUES (?, ?, ?, ?, ?)
        ''', basic_data)
        conn.commit()
        conn.close()
            
        print(f"✅ 成功更新 {len(basic_data)} 只股票的基础画像（含双接口合并）！")

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
    bs.login()

def fetch_worker(args):
    code, quarters_set = args
    now = datetime.now()
    results = []
    
    try:
        for year, quarter in sorted(quarters_set, reverse=True):
            profit_df = bs.query_profit_data(code=code, year=year, quarter=quarter).get_data()
            
            if profit_df is not None and not profit_df.empty:
                growth_df = bs.query_growth_data(code=code, year=year, quarter=quarter).get_data()
                cash_flow_df = bs.query_cash_flow_data(code=code, year=year, quarter=quarter).get_data()
                
                def safe_float(df, col, default=0.0):
                    if df is not None and not df.empty and col in df.columns:
                        val = df[col].iloc[0]
                        try: 
                            return float(val) if val else default
                        except Exception as e: 
                            # 坚决不静默处理，打印确切的异常值
                            print(f"⚠️ [警告] {code} {year}Q{quarter} 字段 {col} 转换异常: '{val}'")
                            return default
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
                    safe_float(profit_df, 'gpMargin'),    
                    safe_float(profit_df, 'npMargin'), cfo_to_np, safe_float(cash_flow_df, 'CFOToGr'),       
                    safe_float(growth_df, 'YOYPNI')
                )
                results.append(data)
        
        return {'code': code, 'status': 'success', 'data': results}
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"❌ {code} 数据抓取发生未捕获异常:\n{error_msg}")
        return {'code': code, 'status': 'error', 'msg': error_msg}

# ==========================================
# 🎯 主控调度中心
# ==========================================
def run_factor_sync(auto_confirm=False):
    init_db()
    sync_stock_basic()

    print("⚡ 正在从本地画像库极速提取 A股 股票列表...")
    conn = sqlite3.connect(DB_PATH)
    try:
        df_basic = pd.read_sql_query("SELECT code, list_date FROM stock_basic", conn)
        stock_list = df_basic['code'].tolist()
        list_date_dict = dict(zip(df_basic['code'], df_basic['list_date']))
    except Exception as e:
        print(f"⚠️ 提取股票列表异常: {e}，将临时降级为无免疫盾模式。")
        stock_list = []
        list_date_dict = {}

    print("📡 正在全盘扫描本地数据库，执行时空集合比对与空洞探测...")
    target_set = get_target_quarters(FINANCIAL_QUARTERS)
    
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
        
        # 🛡️ 核心：【次新股免疫盾 - 招股书三年宽限期修复版】
        list_date = list_date_dict.get(code)
        if list_date and str(list_date).strip():
            try:
                list_year = int(str(list_date).split('-')[0])
                allowed_start_year = list_year - 3
                # 避开日期拼接引发的 31 号超界崩溃，纯靠年份拦截
                missing_set = {yq for yq in missing_set if yq[0] >= allowed_start_year}
            except Exception as e:
                print(f"⚠️ {code} 宽限期计算异常: {e}")
                pass
                
        # 兜底：保留已有残缺历史修复逻辑
        if existing_set:
            min_existing = min(existing_set)
            missing_set = {mq for mq in missing_set if mq >= min_existing}
            
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
    
    tasks = list(todo_dict.items())
    process_count = min(16, cpu_count() * 2, total)
    print(f"🚀 启动 {process_count} 个子进程开始狂奔...")

    insert_sql = '''
        INSERT OR REPLACE INTO financial_factors (
            code, stat_date, pub_date, update_date, roe_avg, yoy_profit_growth, net_profit, 
            eps_ttm, cash_flow, mb_revenue, total_share,
            gp_margin, np_margin, cfo_to_np, cfo_to_gr,
            yoy_pni
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

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

            progress[code] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
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