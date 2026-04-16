import os
import sys
import re
import traceback

# 💥 暴力屏蔽代理，确保 A 股接口走本地直连
for proxy_env in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']:
    if proxy_env in os.environ:
        os.environ[proxy_env] = ''

import akshare as ak
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# 从配置中导入
from config import DB_PATH, FINANCIAL_QUARTERS

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 财务因子表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_factors (
            code TEXT, stat_date TEXT, pub_date TEXT, 
            update_date TEXT, roe_avg REAL, yoy_pni REAL, 
            net_profit REAL, eps_ttm REAL, eps_raw REAL, 
            cash_flow REAL, cfo_to_np REAL, liability_ratio REAL, 
            gp_margin REAL, np_margin REAL, mb_revenue REAL,
            PRIMARY KEY (code, stat_date, pub_date)
        )
    ''')
    # 基础画像表 (Screener 依赖此表进行行业排名及防黑户)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY, name TEXT, industry TEXT, industry_classification TEXT
        )
    ''')
    conn.commit()
    conn.close()

def format_ak_code(symbol):
    """
    终极前缀匹配：仅通过首位数字判定，完全免疫未来交易所号段扩容
    沪市 6 开头，深市 0 或 3 开头，北交所 4/8/9 开头
    """
    symbol = str(symbol).strip()
    if symbol.startswith('6'): 
        return f"sh.{symbol}"
    elif symbol.startswith(('0', '3')): 
        return f"sz.{symbol}"
    elif symbol.startswith(('8', '4', '9')): 
        return f"bj.{symbol}"
    return symbol

def get_target_quarters_ak(num_quarters):
    """生成季度序列，并反转为‘由远及近’，以便精准计算跨期 TTM"""
    now = datetime.now()
    year, month = now.year, now.month
    if month <= 3: year -= 1; quarter = 4
    elif month <= 6: quarter = 1
    elif month <= 9: quarter = 2
    else: quarter = 3
        
    dates = []
    for _ in range(num_quarters):
        if quarter == 1: date_str = f"{year}0331"
        elif quarter == 2: date_str = f"{year}0630"
        elif quarter == 3: date_str = f"{year}0930"
        elif quarter == 4: date_str = f"{year}1231"
        stat_date_db = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        dates.append((date_str, stat_date_db))
        quarter -= 1
        if quarter == 0: quarter = 4; year -= 1
    
    return dates[::-1] # 💥 由远及近，保证跑今年数据时去年的数据已入库

def run_factor_sync():
    init_db()
    
    print(f"\n🚀 [AkShare 核心引擎] 正在构建全量财务底稿 (深度: {FINANCIAL_QUARTERS} 季度)...")
    targets = get_target_quarters_ak(FINANCIAL_QUARTERS)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    insert_sql = '''
        INSERT OR REPLACE INTO financial_factors (
            code, stat_date, pub_date, update_date, roe_avg, yoy_pni, net_profit, 
            eps_ttm, eps_raw, cash_flow, cfo_to_np, liability_ratio, gp_margin, np_margin, mb_revenue
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_inserted = 0
    basic_data_dict = {}

    for ak_date, db_date in targets:
        print(f"📡 正在处理 [{db_date}] ...")
        try:
            df_yjbb = ak.stock_yjbb_em(date=ak_date)
            if df_yjbb is None or df_yjbb.empty: continue
            
            try:
                df_zcfz = ak.stock_zcfz_em(date=ak_date)
            except Exception as e:
                print(f"   ⚠️ 获取 {ak_date} 资产负债表异常，部分字段将填 0")
                traceback.print_exc()
                df_zcfz = pd.DataFrame() 
                
            df_merge = pd.merge(df_yjbb, df_zcfz, on="股票代码", how="left", suffixes=('', '_zcfz')) if not df_zcfz.empty else df_yjbb

            records = []
            for _, row in df_merge.iterrows():
                raw_code = str(row['股票代码']).strip()
                
                # 💥 终极白名单正则过滤：
                # 必须满足: (60|68|00|30 加上任意数字组成的号段) 或 (4/8/9 开头)，且总长严格为 6 位数字
                # 这直接秒杀了所有的 ETF (51xxxx, 15xxxx) 以及债券 (11xxxx)。
                if not re.match(r'^(60|68|00|30|[489]\d)\d{4}$', raw_code): continue 
                
                code = format_ak_code(raw_code)
                name = str(row.get('股票简称', '未知'))
                industry = str(row.get('所处行业', '未知'))
                pub_date = str(row.get('最新公告日期', '未知'))[:10]
                
                # [核心联动] 顺手更新最新花名册（增量新股的来源）
                if industry not in ['nan', 'None', '', '未知']:
                    basic_data_dict[code] = (code, name, industry, industry)

                if pub_date == "未知" or pub_date == "NaT": continue
                
                def get_val(col_name, default=0.0):
                    val = row.get(col_name, default)
                    return float(val) if pd.notna(val) else default

                eps_raw = get_val('每股收益', 0.0001)
                if eps_raw == 0: eps_raw = 0.0001
                
                # ---------------------------------------------------------
                # 🧬 核心：True TTM 跨期滚动算法 (抗财报修正版)
                # ---------------------------------------------------------
                cur_year = int(db_date[:4])
                last_year = cur_year - 1
                q_tag = db_date[5:].replace('-', '')

                if q_tag == '1231':
                    eps_ttm = eps_raw 
                else:
                    # 抓取去年全年的 EPS (若有多次修正公告，取最后一次)
                    cursor.execute(
                        "SELECT eps_raw FROM financial_factors WHERE code=? AND stat_date=? ORDER BY pub_date DESC LIMIT 1", 
                        (code, f"{last_year}-12-31")
                    )
                    res_ly_full = cursor.fetchone()
                    
                    # 抓取去年同期的 EPS
                    cursor.execute(
                        "SELECT eps_raw FROM financial_factors WHERE code=? AND stat_date=? ORDER BY pub_date DESC LIMIT 1", 
                        (code, f"{last_year}-{db_date[5:]}")
                    )
                    res_ly_same = cursor.fetchone()

                    if res_ly_full and res_ly_same:
                        eps_ttm = eps_raw + (res_ly_full[0] - res_ly_same[0])
                    else:
                        q_map = {'0331': 4.0, '0630': 2.0, '0930': 1.333}
                        eps_ttm = eps_raw * q_map.get(q_tag, 1.0)
                # ---------------------------------------------------------

                net_profit = get_val('净利润-净利润')
                revenue = get_val('营业总收入-营业总收入')
                roe_avg = get_val('净资产收益率') / 100.0
                yoy_pni = get_val('净利润-同比增长') / 100.0
                gp_margin = get_val('销售毛利率') / 100.0
                np_margin = round((net_profit / revenue), 4) if revenue != 0 else 0.0
                
                liab = get_val('负债合计')
                assets = get_val('资产总计')
                liab_ratio = round((liab / assets), 4) if assets != 0 else 0.0
                cfo_per = get_val('每股经营现金流量')
                cfo_to_np = round(cfo_per / eps_raw, 4)
                cash_flow = net_profit * cfo_to_np 

                records.append((
                    code, db_date, pub_date, now_str, roe_avg, yoy_pni, net_profit, 
                    eps_ttm, eps_raw, cash_flow, cfo_to_np, liab_ratio, gp_margin, np_margin, revenue
                ))
                
            if records:
                cursor.executemany(insert_sql, records)
                conn.commit()
                total_inserted += len(records)
                print(f"   ✅ 已同步 {len(records)} 条标的至底层财务舱")

        except Exception as e:
            print(f"   ❌ 严重阻断异常发生于处理 {db_date} 期间:")
            traceback.print_exc()
            
    # 全量/增量写入最新画像库，供给 K 线模块提取
    if basic_data_dict:
        cursor.executemany('INSERT OR REPLACE INTO stock_basic VALUES (?,?,?,?)', list(basic_data_dict.values()))
        conn.commit()
        print(f"\n✅ 成功刷新全市场 {len(basic_data_dict)} 只正股的花名册字典 (彻底免疫杂项与基金)。")

    conn.close()
    print(f"\n🏁 财务底稿与花名册构建完毕，可进入 K 线同步阶段。")

if __name__ == '__main__':
    run_factor_sync()