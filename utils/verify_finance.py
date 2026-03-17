import sqlite3
import pandas as pd
from config import DB_PATH

def verify_random_financials(db_path, limit=5):
    """随机抽取N条带有最新防雷指标的财务数据用于人工核对"""
    print(f"🎲 正在从数据库随机抽取 {limit} 只股票的财务数据...")
    
    conn = sqlite3.connect(db_path)
    # 只抽取刚刚加入了 liability_ratio 的最新数据
    query = f"""
    SELECT code, stat_date, roe_avg, yoy_profit_growth, liability_ratio, cash_flow, net_profit
    FROM financial_factors
    WHERE liability_ratio IS NOT NULL AND liability_ratio != 0
    ORDER BY RANDOM()
    LIMIT {limit}
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        if df.empty:
            print("⚠️ 未找到带有新防雷指标的数据，是 factor_sync.py 还没跑完吗？")
            return
            
        print("\n" + "="*60)
        print(" 🕵️ 请打开同花顺/东方财富，核对以下【2024年三季报】数据 ")
        print("="*60)
        
        for _, row in df.iterrows():
            code = row['code']
            # 将金额换算为“亿元”方便阅读
            np_yi = row['net_profit'] / 1e8 if pd.notna(row['net_profit']) else 0
            cf_yi = row['cash_flow'] / 1e8 if pd.notna(row['cash_flow']) else 0
            
            print(f"📌 股票代码: {code} | 报告期: {row['stat_date']}")
            print(f"   - ROE (平均净资产收益率): {row['roe_avg']}")
            print(f"   - 净利润同比增长率:       {row['yoy_profit_growth']}")
            print(f"   - 资产负债率:             {row['liability_ratio']}")
            print(f"   - 净利润:                 {np_yi:.2f} 亿")
            print(f"   - 经营现金流净额 (推算):  {cf_yi:.2f} 亿")
            print("-" * 60)
            
    except Exception as e:
        print(f"❌ 查询出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    verify_random_financials(DB_PATH)