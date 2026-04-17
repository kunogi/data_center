import os
# 💥 物理封印底层多线程，防止与底层 C 库发生“线程爆炸”
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import akshare as ak
import pandas as pd
import sqlite3
import time
import traceback
from datetime import datetime
import numpy as np

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "quant_data.db"

def get_db_conn():
    return sqlite3.connect(DB_PATH)

def init_ak_table():
    """初始化东财财务总表，字段与 Baostock 严格对齐，专供高质量因子覆盖"""
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_factors_ak (
            code TEXT,
            stat_date TEXT,
            pub_date TEXT,
            update_date TEXT,
            roe_avg REAL,
            yoy_profit_growth REAL,
            net_profit REAL,
            eps_ttm REAL,
            cash_flow REAL,
            mb_revenue REAL,
            total_share REAL,
            gp_margin REAL,
            np_margin REAL,
            cfo_to_np REAL,
            cfo_to_gr REAL,
            yoy_pni REAL,
            PRIMARY KEY (code, stat_date)
        )
    """)
    # 建立查询加速索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ak_stat_date ON financial_factors_ak(stat_date);")
    conn.commit()
    conn.close()

def get_target_quarters(start_year=2015):
    """生成从 2015 年至今的所有财报截止日"""
    quarters = []
    today = datetime.now()
    for year in range(start_year, today.year + 1):
        for md in ['03-31', '06-30', '09-30', '12-31']:
            q_date = datetime.strptime(f"{year}-{md}", "%Y-%m-%d")
            if q_date <= today:
                quarters.append(q_date.strftime("%Y-%m-%d"))
    return sorted(quarters)

def get_todo_quarters():
    """
    智能进度判定：
    - 如果历史季度已有数据 (count > 0)，直接跳过。
    - 强制重新拉取最新 2 个季度 (防企业财报晚披露)。
    """
    all_q = get_target_quarters()
    conn = get_db_conn()
    cursor = conn.cursor()
    
    todo = []
    for i, q in enumerate(all_q):
        try:
            cursor.execute("SELECT COUNT(1) FROM financial_factors_ak WHERE stat_date = ?", (q,))
            count = cursor.fetchone()[0]
        except Exception:
            count = 0
            
        # 核心逻辑：没数据，或者是最近两个季度，就必须拉
        is_recent_2 = (i >= len(all_q) - 2)
        if count == 0 or is_recent_2:
            todo.append(q)
            
    conn.close()
    return todo

def format_code(code_str):
    """将东财 6位代码 标准化为 sh. / sz. / bj."""
    c = str(code_str).zfill(6)
    if c.startswith('6'): return 'sh.' + c
    elif c.startswith('0') or c.startswith('3'): return 'sz.' + c
    elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return 'bj.' + c
    return c

def fetch_and_clean_quarter(stat_date):
    """
    核心爬取与清洗引擎：向东财请求【业绩报表】，并在源头完成财务公式推导。
    加入弹性字典映射机制，兼容东财长达十年的历史表头命名变更。
    """
    em_date = stat_date.replace("-", "")
    print(f"📡 正在向东方财富请求 {stat_date} 财报总表...")
    
    try:
        df = ak.stock_yjbb_em(date=em_date)
        if df is None or df.empty:
            print(f"⚠️ {stat_date} 东方财富暂无数据。")
            return pd.DataFrame()
            
        res = pd.DataFrame()
        res['code'] = df['股票代码'].apply(format_code)
        res['stat_date'] = stat_date
        
        # 处理公告日期：如果没有公告日，用当前拉取日期兜底
        if '最新公告日期' in df.columns:
            res['pub_date'] = pd.to_datetime(df['最新公告日期']).dt.strftime('%Y-%m-%d')
        else:
            res['pub_date'] = datetime.now().strftime('%Y-%m-%d')
            
        res['update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # ==========================================
        # 💥 核心修复：引入弹性安全取值器，防备历史字段更名
        # ==========================================
        def safe_get(col_names):
            for name in col_names:
                if name in df.columns:
                    return df[name]
            return pd.Series([np.nan] * len(df))

        # 核心财务字段清洗与转化 (百分比需 /100)
        res['roe_avg'] = pd.to_numeric(safe_get(['净资产收益率']), errors='coerce') / 100.0
        res['yoy_profit_growth'] = pd.to_numeric(safe_get(['净利润-同比增长', '净利润同比增长']), errors='coerce') / 100.0
        res['net_profit'] = pd.to_numeric(safe_get(['净利润-净利润', '净利润']), errors='coerce')
        res['eps_ttm'] = pd.to_numeric(safe_get(['每股收益']), errors='coerce') # 这里是当期基本 EPS
        res['mb_revenue'] = pd.to_numeric(safe_get(['营业收入-营业收入', '营业收入']), errors='coerce')
        res['gp_margin'] = pd.to_numeric(safe_get(['销售毛利率']), errors='coerce') / 100.0
        
        # 提取极度关键的现金流指标
        cfo_per_share = pd.to_numeric(safe_get(['每股经营现金流量']), errors='coerce')
        
        # 💥 降维推导算法
        # 1. 经营现金流 = 每股现金流 * (净利润 / 每股收益) -> 数学等效，绕开总股本盲区
        res['cash_flow'] = cfo_per_share * (res['net_profit'] / res['eps_ttm'].replace(0, np.nan))
        
        # 2. CFO/NP 核心避雷指标 = 每股现金流 / 每股收益
        res['cfo_to_np'] = cfo_per_share / res['eps_ttm'].replace(0, np.nan)
        
        # 3. 净利率 = 净利润 / 营收
        res['np_margin'] = res['net_profit'] / res['mb_revenue'].replace(0, np.nan)
        
        # 4. 营收现金比 = 经营现金流 / 营收
        res['cfo_to_gr'] = res['cash_flow'] / res['mb_revenue'].replace(0, np.nan)
        
        # 5. 东财业绩报表无“扣非”，直接用归母净利润增速平替
        res['yoy_pni'] = res['yoy_profit_growth'] 
        
        # 6. 总股本估算兜底
        res['total_share'] = res['net_profit'] / res['eps_ttm'].replace(0, np.nan)

        # 全局 NaN 替换为 0.0，防止入库报错
        res = res.fillna(0.0)
        # 剔除无穷大或极度离谱的值 (除零带来的 Inf)
        res = res.replace([np.inf, -np.inf], 0.0)
        
        print(f"   ✔️ 成功清洗 {len(res)} 只股票的 {stat_date} 财报数据。")
        return res
        
    except Exception as e:
        print(f"Exception: 抓取 {stat_date} 时发生严重错误！")
        traceback.print_exc()
        return pd.DataFrame()

def run_ak_sync():
    print("🚀 启动 AKShare (东方财富) 财报全量/增量守护进程...")
    init_ak_table()
    
    todo_list = get_todo_quarters()
    if not todo_list:
        print("✅ 所有历史季度数据均已完备，无需更新。")
        return
        
    print(f"📌 发现 {len(todo_list)} 个需要更新的财报季：{todo_list}")
    
    conn = get_db_conn()
    cursor = conn.cursor()
    
    for q in todo_list:
        df = fetch_and_clean_quarter(q)
        
        if not df.empty:
            # 入库前，先清空该季度的旧数据 (实现覆盖更新)
            try:
                cursor.execute("DELETE FROM financial_factors_ak WHERE stat_date = ?", (q,))
                conn.commit()
                
                # 批量写入
                records = df.values.tolist()
                insert_sql = f"INSERT INTO financial_factors_ak ({','.join(df.columns)}) VALUES ({','.join(['?']*len(df.columns))})"
                cursor.executemany(insert_sql, records)
                conn.commit()
                print(f"   💾 {q} 数据 ({len(records)} 条) 已安全落库。")
            except Exception as e:
                print(f"Exception: 写入 {q} 数据失败 - {e}")
                traceback.print_exc()
        
        # 柔性防封：停顿 3 秒，防止被东财防火墙拦截
        time.sleep(3)
        
    conn.close()
    print("\n🏁 东财高质量底稿数据同步完成！")

if __name__ == "__main__":
    run_ak_sync()