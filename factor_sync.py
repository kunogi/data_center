import baostock as bs
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import re  

# 💥 快速失败机制：直接强制导入，如果缺少配置直接报错阻断，拒绝产生幽灵数据
from config import DB_PATH, FINANCIAL_QUARTERS, COMPLETED_FILE, EXPIRE_DAYS

# ==========================================
# 🛠️ 辅助魔法：时序集合换算
# ==========================================
def stat_date_to_yq(date_str):
    """将 stat_date ('2024-09-30') 转换为 (2024, 3) 的格式"""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return (dt.year, (dt.month - 1) // 3 + 1)
    except:
        return None

def get_target_quarters(num_quarters=FINANCIAL_QUARTERS):
    """生成我们【期望拥有】的完美 12 季度集合"""
    now = datetime.now()
    year = now.year
    month = now.month
    
    if month <= 4: 
        year -= 1
        quarter = 4
    elif month <= 8:
        quarter = 1
    elif month <= 10:
        quarter = 2
    else:
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
    rs = bs.query_stock_industry()
    if rs.error_code != '0':
        print(f"⚠️ 行业信息获取失败: {rs.error_msg}")
        return

    basic_data = []
    while rs.next():
        row = rs.get_row_data()
        code = row[1]
        if code.startswith(('sh.6', 'sz.0', 'sz.3')):
            name = row[2]
            raw_industry = row[3] 
            match = re.match(r'^([A-Za-z0-9]+)(.*)$', raw_industry)
            if match:
                ind_code = match.group(1) 
                ind_name = match.group(2) 
            else:
                ind_code = "未知"
                ind_name = raw_industry if raw_industry else "未知"
            basic_data.append((code, name, ind_name, ind_code))

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

def fetch_specific_financial_quarters(code, quarters_set):
    now = datetime.now()
    results = []
    
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
                
            data = {
                'stat_date': safe_str(profit_df, 'statDate'),
                'pub_date': safe_str(profit_df, 'pubDate'),
                'update_date': now.strftime('%Y-%m-%d %H:%M:%S'), 
                'roe_avg': safe_float(profit_df, 'roeAvg'),
                'yoy_profit_growth': safe_float(growth_df, 'YOYNI'),
                'net_profit': net_profit,
                'eps_ttm': safe_float(profit_df, 'epsTTM'),
                'cash_flow': cash_flow,
                'mb_revenue': safe_float(profit_df, 'MBRevenue'),
                'total_share': safe_float(profit_df, 'totalShare'),
                'liability_ratio': safe_float(profit_df, 'liabRatio'),
                'gp_margin': safe_float(profit_df, 'gpMargin'),     
                'np_margin': safe_float(profit_df, 'npMargin'),        
                'cfo_to_np': cfo_to_np,       
                'cfo_to_gr': safe_float(cash_flow_df, 'CFOToGr'),       
                'inv_turn_days': safe_float(operation_df, 'INVTurnDays'), 
                'nr_turn_days': safe_float(operation_df, 'NRTurnDays'),   
                'yoy_pni': safe_float(growth_df, 'YOYPNI')              
            }
            results.append(data)
            
    return results

def run_factor_sync(auto_confirm=False):
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        return

    init_db()
    sync_stock_basic()

    print("📡 正在获取 A股 股票列表...")
    rs = bs.query_stock_basic()
    stock_list = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(('sh.6', 'sz.0', 'sz.3')):
            stock_list.append(code)

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
                if code not in db_inventory:
                    db_inventory[code] = set()
                db_inventory[code].add(yq)
    except Exception as e:
        print("⚠️ 无法读取存量数据，将执行全量同步。")
        db_inventory = {}

    progress = load_progress()
    now = datetime.now()
    todo_dict = {} 
    
    for code in stock_list:
        existing_set = db_inventory.get(code, set())
        missing_set = target_set - existing_set 
        
        # 🛡️ 核心：【次新股免疫盾】
        # 如果我们手里已经有了它的财报，那么它最老的那份财报季度，就是它的“物理边界”。
        # 我们无条件抛弃 missing_set 中所有比它还要老的季度（史前数据）。
        if existing_set:
            min_existing = min(existing_set)
            # 利用元组比较特性 (2023, 2) >= (2023, 1) 进行完美降维拦截
            missing_set = {mq for mq in missing_set if mq >= min_existing}
            
        if not missing_set:
            continue # 如果抛弃史前数据后，啥也不缺了，直接放行次新股！
        
        last_sync_time = progress.get(code)
        is_expired = True
        if last_sync_time:
            try:
                last_ts = datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S")
                is_expired = (now - last_ts).days > EXPIRE_DAYS
            except: pass
                
        needs_update = False
        if is_expired:
            needs_update = True
        else:
            # 没过期时，检测“内部真空洞”（比如不小心删除了中间一个季度的记录）
            if existing_set:
                min_existing = min(existing_set)
                max_existing = max(existing_set)
                if any(min_existing < mq < max_existing for mq in missing_set):
                    needs_update = True
                    
        if needs_update:
            todo_dict[code] = missing_set

    # 🩺 插入心跳检测网：彻底剥离僵尸股
    if todo_dict:
        print(f"\n🩺 正在对 {len(todo_dict)} 只嫌疑空洞股进行【心跳存活检测】...")
        cutoff_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        codes_to_check = list(todo_dict.keys())
        alive_codes = set()
        
        chunk_size = 900
        for i in range(0, len(codes_to_check), chunk_size):
            chunk = codes_to_check[i:i+chunk_size]
            placeholders = ','.join('?' for _ in chunk)
            query = f"SELECT DISTINCT code FROM daily_k_data WHERE date >= ? AND volume > 0 AND code IN ({placeholders})"
            try:
                alive_df = pd.read_sql_query(query, conn, params=[cutoff_date] + chunk)
                alive_codes.update(alive_df['code'].tolist())
            except Exception: pass
                
        zombies = [c for c in codes_to_check if c not in alive_codes]
        print(f"💀 过滤结果: 成功剔除 {len(zombies)} 只已退市或长期停牌的【僵尸股】！")
        
        # 喂给僵尸股时间戳，未来 EXPIRE_DAYS 天内不再诈尸
        for z_code in zombies:
            progress[z_code] = now.strftime("%Y-%m-%d %H:%M:%S")
        if zombies: save_progress(progress)
            
        todo_dict = {code: quarters for code, quarters in todo_dict.items() if code in alive_codes}

    total = len(todo_dict)
    if total == 0:
        print("✅ 全盘数据完美连续 (已智能豁免次新股与僵尸股)，无历史空洞！")
        conn.close()
        bs.logout()
        return

    print(f"\n📊 审计完毕：排雷后共 {total} 只活股确实需要更新。")
    
    if not auto_confirm:
        user_input = input("❓ 是否开始定点填补空洞与更新？ [默认回车继续] (Y/n): ")
        if user_input.strip().lower() == 'n':
            print("🛑 已取消财务更新。")
            conn.close()
            bs.logout()
            return

    cursor = conn.cursor()
    start_time = time.time()
    todo_items = list(todo_dict.items())

    for idx, (code, missing_quarters) in enumerate(todo_items):
        if idx > 0:
            elapsed = time.time() - start_time
            eta_seconds = (elapsed / idx) * (total - idx)
            eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
        else: eta_str = "计算中..."

        print(f"[{idx+1}/{total} | ETA: {eta_str}] 填补 {code} (需补 {len(missing_quarters)} 季)...", end=" ", flush=True)
        try:
            records = fetch_specific_financial_quarters(code, quarters_set=missing_quarters)
            if records:
                for rec in records:
                    cursor.execute('''
                        INSERT OR REPLACE INTO financial_factors (
                            code, stat_date, pub_date, update_date, roe_avg, yoy_profit_growth, net_profit, 
                            eps_ttm, cash_flow, mb_revenue, total_share, liability_ratio,
                            gp_margin, np_margin, cfo_to_np, cfo_to_gr,
                            inv_turn_days, nr_turn_days, yoy_pni
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        code, rec['stat_date'], rec['pub_date'], rec['update_date'], rec['roe_avg'], rec['yoy_profit_growth'], rec['net_profit'],
                        rec['eps_ttm'], rec['cash_flow'], rec['mb_revenue'], rec['total_share'], rec['liability_ratio'],
                        rec['gp_margin'], rec['np_margin'], rec['cfo_to_np'], rec['cfo_to_gr'],
                        rec['inv_turn_days'], rec['nr_turn_days'], rec['yoy_pni']
                    ))
                conn.commit()
                print(f"✅ 成功补入 {len(records)} 季")
            else:
                print("⚠️ 暂无发布数据")
            
            progress[code] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_progress(progress)
        except Exception as e: print(f"❌ 失败: {e}")
            
    conn.close()
    bs.logout()
    print("🎉 全市场历史空洞填补与财务护城河更新完毕！")

if __name__ == '__main__':
    run_factor_sync(auto_confirm=False)