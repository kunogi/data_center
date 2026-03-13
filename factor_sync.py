import baostock as bs
import sqlite3
import pandas as pd
from datetime import datetime
import os
import time
import re  # 💥 引入正则库，用于清洗 Baostock 的脏数据

# 💥 快速失败机制：直接强制导入，如果缺少配置直接报错阻断，拒绝产生幽灵数据
from config import DB_PATH, FINANCIAL_QUARTERS, COMPLETED_FILE, EXPIRE_DAYS

def init_db():
    """💥 静态化数据结构：一次性建表，包含财务事实表与基础维度表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 创建财务因子事实表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_factors (
            code TEXT,
            stat_date TEXT,
            pub_date TEXT,
            roe_avg REAL,
            yoy_profit_growth REAL,
            np_margin REAL,
            gp_margin REAL,
            eps_ttm REAL,
            net_profit REAL,
            mb_revenue REAL,
            update_date TEXT, 
            liability_ratio REAL, 
            cash_flow REAL, 
            gross_margin REAL, 
            net_margin REAL, 
            cfo_to_np REAL, 
            cfo_to_gr REAL, 
            inv_turn_days REAL, 
            nr_turn_days REAL, 
            yoy_pni REAL, 
            total_share REAL,
            PRIMARY KEY (code, stat_date, pub_date)
        )
    ''')
    
    # 2. 创建股票基础信息维度表 (星型模型架构核心)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY,
            name TEXT,
            industry TEXT,
            industry_classification TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def sync_stock_basic():
    """💥 毫秒级雷达：同步全市场股票的基础静态信息 (自带脏数据清洗过滤)"""
    print("📡 正在同步全市场股票基础信息(行业分类)...")
    rs = bs.query_stock_industry()
    
    if rs.error_code != '0':
        print(f"⚠️ 行业信息获取失败: {rs.error_msg}")
        return

    basic_data = []
    while rs.next():
        row = rs.get_row_data()
        # Baostock 原生 row: [updateDate, code, code_name, industry(含代码的脏数据), industryClassification(废话)]
        code = row[1]
        if code.startswith(('sh.6', 'sz.0', 'sz.3')):
            name = row[2]
            raw_industry = row[3]  # 例如: "J66货币金融服务"
            
            # 💥 数据清洗：用正则分离行业代码 (字母+数字) 和纯中文行业名
            match = re.match(r'^([A-Za-z0-9]+)(.*)$', raw_industry)
            if match:
                ind_code = match.group(1)   # 提取出 "J66"
                ind_name = match.group(2)   # 提取出 "货币金融服务"
            else:
                ind_code = "未知"
                ind_name = raw_industry if raw_industry else "未知"
                
            # 我们直接废弃 Baostock 返回的废话 "证监会行业分类"
            # 将纯净的中文名存入 industry，将提取的代码存入 industry_classification
            basic_data.append((code, name, ind_name, ind_code))

    if basic_data:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # 使用 REPLACE，如果有股票改名或者行业变更，自动热更新
        cursor.executemany('''
            INSERT OR REPLACE INTO stock_basic (code, name, industry, industry_classification)
            VALUES (?, ?, ?, ?)
        ''', basic_data)
        conn.commit()
        conn.close()
        print(f"✅ 成功更新 {len(basic_data)} 只股票的基础行业画像！数据已极致净化。")

def load_progress():
    """读取带有时间戳的断点续传记录"""
    progress = {}
    if os.path.exists(COMPLETED_FILE):
        with open(COMPLETED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                parts = line.split(',')
                if len(parts) >= 2:
                    progress[parts[0]] = parts[1]
                else:
                    progress[parts[0]] = "2000-01-01 00:00:00"
    return progress

def save_progress(progress_dict):
    """保存进度与时间戳"""
    with open(COMPLETED_FILE, "w", encoding="utf-8") as f:
        for code, ts in progress_dict.items():
            f.write(f"{code},{ts}\n")

def fetch_historical_financial_data(code, num_quarters=FINANCIAL_QUARTERS):
    """提取过去 N 个季度的完整财务与护城河指标"""
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
        
    results = []
    checks = 0
    max_checks = num_quarters + 4 
    
    while len(results) < num_quarters and checks < max_checks:
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
            
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
        checks += 1
        
    return results

def run_factor_sync(auto_confirm=False):
    """主入口：带有断点续传、过期检测与人工确认的财务同步引擎"""
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        return

    init_db()
    
    # 💥 首先同步轻量级的静态基础维度数据
    sync_stock_basic()

    print("📡 正在获取 A股 股票列表...")
    rs = bs.query_stock_basic()
    stock_list = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(('sh.6', 'sz.0', 'sz.3')):
            stock_list.append(code)

    progress = load_progress()
    now = datetime.now()
    
    todo_list = []
    for code in stock_list:
        if code in progress:
            try:
                last_sync_time = datetime.strptime(progress[code], "%Y-%m-%d %H:%M:%S")
                if (now - last_sync_time).days > EXPIRE_DAYS:
                    todo_list.append(code)
            except ValueError:
                todo_list.append(code) 
        else:
            todo_list.append(code)

    total = len(todo_list)
    if total == 0:
        print("✅ 所有财务数据均在有效期内，无需更新。")
        bs.logout()
        return

    print(f"\n📊 审计完毕：共有 {total} 只股票的财务数据缺失或已过期（>{EXPIRE_DAYS}天）。")
    
    if not auto_confirm:
        user_input = input("❓ 是否开始更新全量 12 季度财报？首次更新时间可能较长 [默认回车继续] (Y/n): ")
        if user_input.strip().lower() == 'n':
            print("🛑 已取消财务更新。")
            bs.logout()
            return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    start_time = time.time()

    for idx, code in enumerate(todo_list):
        if idx > 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / idx
            eta_seconds = avg_time * (total - idx)
            eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
        else:
            eta_str = "计算中..."

        print(f"[{idx+1}/{total} | ETA: {eta_str}] 同步时序财报护城河: {code} ...", end=" ", flush=True)
        try:
            records = fetch_historical_financial_data(code, num_quarters=FINANCIAL_QUARTERS)
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
                print(f"✅ 提取 {len(records)} 季")
            else:
                print("⚠️ 暂无数据")
            
            progress[code] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_progress(progress)
                
        except Exception as e:
            print(f"❌ 失败: {e}")
            
    conn.close()
    bs.logout()
    print("🎉 全市场时序财务护城河数据更新完毕！")

if __name__ == '__main__':
    run_factor_sync(auto_confirm=False)