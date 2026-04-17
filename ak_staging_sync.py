import akshare as ak
import pandas as pd
import sqlite3
import re
from datetime import datetime
import traceback

# ==========================================
# 🎯 配置区：在此添加需要雷达扫描的财报季
# ==========================================
DB_PATH = "quant_data.db"
# 同时扫描 25年报 和 26年一季报
TARGET_PERIODS = ["20251231", "20260331"]

def format_ak_code(symbol):
    """转换代码格式，同时物理拦截北交所和B股"""
    symbol = str(symbol).strip()
    if re.match(r'^60\d{4}$|^68\d{4}$', symbol):
        return f"sh.{symbol}"
    elif re.match(r'^00\d{4}$|^30\d{4}$', symbol):
        return f"sz.{symbol}"
    return None

def fetch_ak_staging():
    print(f"🚀 [雷达系统] 启动多季度财报防雷网 | 目标: {TARGET_PERIODS}")
    print("=" * 70)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 确保表结构存在
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_factors_ak_staging (
            code TEXT, stat_date TEXT, pub_date TEXT, 
            update_date TEXT, roe_avg REAL, yoy_profit_growth REAL, 
            net_profit REAL, eps_ttm REAL, cash_flow REAL, mb_revenue REAL, total_share REAL,
            gp_margin REAL, np_margin REAL, cfo_to_np REAL, cfo_to_gr REAL,
            yoy_pni REAL,
            PRIMARY KEY (code, stat_date)
        )
    ''')
    
    all_records = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for period in TARGET_PERIODS:
        # 格式化日期：20251231 -> 2025-12-31
        stat_date_formatted = f"{period[:4]}-{period[4:6]}-{period[6:]}"
        print(f"\n📡 正在向东方财富请求 [{stat_date_formatted}] 季度的数据...")
        
        try:
            df_yj = ak.stock_yjbb_em(date=period)
            df_xj = ak.stock_xjll_em(date=period)
            
            df_merged = pd.merge(df_yj, df_xj, on="股票代码", how="inner")
            print(f"✅ {stat_date_formatted}：成功拉取并合并 {len(df_merged)} 条数据。")
            
            success_count = 0
            for _, row in df_merged.iterrows():
                raw_code = row['股票代码']
                code = format_ak_code(raw_code)
                if not code: continue
                    
                try:
                    pub_date = str(row.get('最新公告日期', ''))[:10]
                    if not pub_date or pub_date == 'NaT' or pub_date == 'None':
                        continue
                    
                    net_profit = float(row.get('净利润-净利润', 0.0) or 0.0)
                    mb_revenue = float(row.get('营业总收入-营业总收入', 0.0) or 0.0)
                    cash_flow = float(row.get('经营性现金流-现金流量净额', 0.0) or 0.0)
                    
                    roe_avg = float(row.get('净资产收益率', 0.0) or 0.0) / 100.0
                    gp_margin = float(row.get('销售毛利率', 0.0) or 0.0) / 100.0
                    yoy_pni = float(row.get('净利润-同比增长', 0.0) or 0.0) / 100.0
                    yoy_profit_growth = yoy_pni
                    
                    eps_ttm = float(row.get('每股收益', 0.0) or 0.0)
                    
                    np_margin = (net_profit / mb_revenue) if mb_revenue != 0 else 0.0
                    cfo_to_np = (cash_flow / net_profit) if net_profit != 0 else 0.0
                    cfo_to_gr = (cash_flow / mb_revenue) if mb_revenue != 0 else 0.0
                    total_share = (net_profit / eps_ttm) if eps_ttm != 0 else 0.0

                    all_records.append((
                        code, stat_date_formatted, pub_date, now_str,
                        roe_avg, yoy_profit_growth, net_profit, eps_ttm,
                        cash_flow, mb_revenue, total_share,
                        gp_margin, np_margin, cfo_to_np, cfo_to_gr, yoy_pni
                    ))
                    success_count += 1
                except Exception as e:
                    continue
            print(f"✅ {stat_date_formatted}：成功解析 {success_count} 条 A 股底稿。")
            
        except Exception as e:
            print(f"❌ {stat_date_formatted} 数据抓取发生异常: {e}")
            
    if all_records:
        print(f"\n💾 正在将汇总的 {len(all_records)} 条防雷补丁写入 Staging 表...")
        cursor.executemany('''
            INSERT OR REPLACE INTO financial_factors_ak_staging (
                code, stat_date, pub_date, update_date, roe_avg, yoy_profit_growth, net_profit, 
                eps_ttm, cash_flow, mb_revenue, total_share,
                gp_margin, np_margin, cfo_to_np, cfo_to_gr, yoy_pni
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', all_records)
        conn.commit()
        print("🎯 雷达网部署完毕！最新预警数据已落库。")
    else:
        print("\n⚠️ 未获取到任何需要更新的雷达数据。")
        
    conn.close()

if __name__ == "__main__":
    fetch_ak_staging()