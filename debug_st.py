import sqlite3
import pandas as pd
from config import DB_PATH

def investigate_all_st():
    print("🔍 启动 ST 垃圾股全面时空审计 (自动反查代码)...\n")
    conn = sqlite3.connect(DB_PATH)
    
    # 案件清单：(案发日期, 现在的名称) - 取了每只股票的第一次买入日期
    st_cases = [
        ('2019-03-11', '*ST艾格'),
        ('2019-03-12', '*ST紫鑫'),
        ('2019-03-13', '*ST泛海'),
        ('2020-08-13', '*ST新潮'),
        ('2022-10-26', '*ST传智'),
        ('2023-01-03', 'ST人福'),
        ('2023-10-27', 'ST汇洲'),
        ('2025-06-23', 'ST萃华'),
        ('2025-09-24', '*ST威尔')
    ]
    
    for target_date, st_name in st_cases:
        print(f"{'='*70}")
        print(f"🕵️‍♂️ 调查目标: {st_name} | 案发时间: {target_date}")
        
        # 1. 智能反查股票代码 (兼容带有或不带 ST 前缀的情况)
        clean_name = st_name.replace('*', '').replace('ST', '')
        code_query = f"SELECT code, name FROM stock_basic WHERE name LIKE '%{clean_name}%' LIMIT 1"
        code_df = pd.read_sql_query(code_query, conn)
        
        if code_df.empty:
            print(f"❌ 无法在 stock_basic 中查到包含 '{clean_name}' 的股票，跳过。")
            continue
            
        code = code_df.iloc[0]['code']
        db_name = code_df.iloc[0]['name']
        print(f"🆔 锁定代码: {code} (当前数据库名称: {db_name})")
        
        # 2. 扒出当年的财报底牌
        sql = f"""
            SELECT 
                pub_date AS '发布日期', 
                stat_date AS '财报期', 
                yoy_pni AS '扣非同增', 
                net_profit AS '净利润',
                cash_flow AS '经营现金流',
                cfo_to_np AS '净现比', 
                roe_avg AS 'ROE', 
                gp_margin AS '毛利率',
                liability_ratio AS '负债率'
            FROM financial_factors 
            WHERE code = '{code}' AND pub_date <= '{target_date}'
            ORDER BY pub_date DESC LIMIT 1
        """
        try:
            df = pd.read_sql_query(sql, conn)
            if not df.empty:
                # 格式化，让人类一眼看穿造假手法
                def format_money(x):
                    if pd.isna(x): return "NaN"
                    if abs(x) >= 100000000: return f"{x/100000000:.2f} 亿"
                    return f"{x/10000:.2f} 万"
                    
                df['净利润'] = df['净利润'].apply(format_money)
                df['经营现金流'] = df['经营现金流'].apply(format_money)
                df['扣非同增'] = df['扣非同增'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "NaN")
                df['净现比'] = df['净现比'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "NaN")
                df['ROE'] = df['ROE'].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "NaN")
                df['毛利率'] = df['毛利率'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "NaN")
                df['负债率'] = df['负债率'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "NaN")
                
                print(df.to_markdown(index=False))
            else:
                print("⚠️ 案发前未找到财报数据！(可能是刚上市或数据缺失)")
        except Exception as e:
            print(f"查询失败: {e}")
            
    conn.close()
    print(f"\n{'='*70}\n✅ 审计结束。")

if __name__ == "__main__":
    investigate_all_st()