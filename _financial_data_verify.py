import sqlite3
import pandas as pd
import sys

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "quant_data.db"

def format_money(x):
    """将绝对金额转换为 '亿元' 格式，方便与同花顺对账"""
    if pd.isna(x) or x == "" or x is None: 
        return "N/A"
    try:
        return f"{float(x) / 100000000:.2f}亿"
    except:
        return "N/A"

def format_pct(x):
    """将小数转换为百分比格式"""
    if pd.isna(x) or x == "" or x is None: 
        return "N/A"
    try:
        return f"{float(x) * 100:.2f}%"
    except:
        return "N/A"

def format_num(x):
    """格式化普通数字保留两位小数"""
    if pd.isna(x) or x == "" or x is None: 
        return "N/A"
    try:
        return f"{float(x):.2f}"
    except:
        return "N/A"

def audit_stock(code=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. 如果没有指定股票，则从已有数据的表里随机抽签一只
        if not code:
            cursor.execute("SELECT DISTINCT code FROM financial_factors ORDER BY RANDOM() LIMIT 1")
            row = cursor.fetchone()
            if not row:
                print("⚠️ 数据库中还没有任何财务数据！请等待爬虫抓取...")
                conn.close()
                return False
            code = row[0]
            
        # 2. 获取股票基础画像
        basic_df = pd.read_sql_query("SELECT name, industry FROM stock_basic WHERE code = ?", conn, params=(code,))
        name = basic_df['name'].iloc[0] if not basic_df.empty else "未知名称"
        industry = basic_df['industry'].iloc[0] if not basic_df.empty else "未知行业"
        
        print(f"\n" + "━"*100)
        print(f" 🎯 【同花顺 F10 交叉对账】: {name} ({code}) | 所属板块: {industry}")
        print("━"*100)
        
        # 3. 提取最新的历史财报切片 (已剔除废弃的 liability_ratio 和不存在的 eps_raw)
        df = pd.read_sql_query('''
            SELECT stat_date AS 财报季, pub_date AS 公告日,
                   mb_revenue AS 营业总收入, net_profit AS 归母净利润,
                   yoy_pni AS 净利润同增,
                   eps_ttm AS 每股收益TTM,
                   gp_margin AS 销售毛利率, np_margin AS 销售净利率,
                   roe_avg AS 净资产收益率, 
                   cash_flow AS 经营现金流, cfo_to_np AS 净现比
            FROM financial_factors
            WHERE code = ?
            ORDER BY stat_date DESC
            LIMIT 8
        ''', conn, params=(code,))
        
        conn.close()
        
        if df.empty:
            print(f"⚠️ 股票 {code} 暂无历史财务数据。")
            return True
            
        # 4. 施加“同花顺视觉滤镜”
        df['营业总收入'] = df['营业总收入'].apply(format_money)
        df['归母净利润'] = df['归母净利润'].apply(format_money)
        df['经营现金流'] = df['经营现金流'].apply(format_money)
        
        df['净利润同增'] = df['净利润同增'].apply(format_pct)
        df['销售毛利率'] = df['销售毛利率'].apply(format_pct)
        df['销售净利率'] = df['销售净利率'].apply(format_pct)
        df['净资产收益率'] = df['净资产收益率'].apply(format_pct)
        
        df['每股收益TTM'] = df['每股收益TTM'].apply(format_num)
        df['净现比'] = df['净现比'].apply(format_num)
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print(df.to_string(index=False))
        print("━"*100)
        return True
        
    except Exception as e:
        print(f"❌ 查询异常: {e}")
        import traceback
        traceback.print_exc()
        return True

def main():
    print("🕵️ 数据中台交叉审计探针已启动...")
    print("你可以直接打开同花顺的【F10 -> 财务分析】页面进行对比。")
    
    audit_stock()
    
    while True:
        print("\n💡 操作指引：")
        print(" [回车] 🎲 随机抽取下一只盲盒股票")
        print(" [代码] 🔍 强制查询指定股票 (例如输入: sh.600519 或 301345)")
        print(" [q]    🚪 退出审计")
        
        user_input = input("👉 请输入指令: ").strip().lower()
        
        if user_input in ['q', 'quit']:
            print("👋 审计结束，再见！")
            break
        elif user_input == '':
            audit_stock()
        else:
            code = user_input
            if len(code) == 6 and code.isdigit():
                # 自动分配前后缀，兼容各种输入习惯
                if code.startswith('6'): code = f"sh.{code}"
                elif code.startswith(('0', '3')): code = f"sz.{code}"
                elif code.startswith(('4', '8', '9')): code = f"bj.{code}"
            audit_stock(code)

if __name__ == "__main__":
    main()