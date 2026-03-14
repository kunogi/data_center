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
            
        # 2. 获取股票基础画像 (关联我们刚刚秒速建好的 stock_basic)
        basic_df = pd.read_sql_query("SELECT name, industry FROM stock_basic WHERE code = ?", conn, params=(code,))
        name = basic_df['name'].iloc[0] if not basic_df.empty else "未知名称"
        industry = basic_df['industry'].iloc[0] if not basic_df.empty else "未知行业"
        
        print(f"\n" + "━"*90)
        print(f" 🎯 【财务数据交叉审计】: {name} ({code}) | 所属行业: {industry}")
        print("━"*90)
        
        # 3. 提取它所有的历史财报切片
        df = pd.read_sql_query('''
            SELECT stat_date AS 财报季, pub_date AS 发布日,
                   net_profit AS 净利润, cash_flow AS 经营现金流,
                   yoy_pni AS 扣非同增, yoy_profit_growth AS 净利同增,
                   gp_margin AS 毛利率, np_margin AS 净利率,
                   cfo_to_np AS 净利现金含量, 
                   inv_turn_days AS 存货周转天数, nr_turn_days AS 应收周转天数,
                   roe_avg AS ROE
            FROM financial_factors
            WHERE code = ?
            ORDER BY stat_date DESC
        ''', conn, params=(code,))
        
        conn.close()
        
        if df.empty:
            print(f"⚠️ 股票 {code} 暂无历史财务数据。")
            return True
            
        # 4. 施加“同花顺视觉滤镜”
        df['净利润'] = df['净利润'].apply(format_money)
        df['经营现金流'] = df['经营现金流'].apply(format_money)
        df['扣非同增'] = df['扣非同增'].apply(format_pct)
        df['净利同增'] = df['净利同增'].apply(format_pct)
        df['毛利率'] = df['毛利率'].apply(format_pct)
        df['净利率'] = df['净利率'].apply(format_pct)
        df['ROE'] = df['ROE'].apply(format_pct)
        
        df['净利现金含量'] = df['净利现金含量'].apply(format_num)
        df['存货周转天数'] = df['存货周转天数'].apply(format_num)
        df['应收周转天数'] = df['应收周转天数'].apply(format_num)
        
        # 让 Pandas 在控制台对齐中文字符串
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print(df.to_string(index=False))
        print("━"*90)
        return True
        
    except Exception as e:
        print(f"❌ 查询异常: {e}")
        return True

def main():
    print("🕵️ 数据中台交叉审计探针已启动...")
    print("您可以直接与手机炒股软件的【F10 -> 财务分析】页面进行对比。")
    
    # 启动时先随机抽查一只
    audit_stock()
    
    while True:
        print("\n💡 操作指引：")
        print(" [回车] 🎲 随机抽取下一只盲盒股票")
        print(" [代码] 🔍 强制查询指定股票 (例如输入: sh.600519)")
        print(" [q]    🚪 退出审计")
        
        user_input = input("👉 请输入指令: ").strip().lower()
        
        if user_input == 'q' or user_input == 'quit':
            print("👋 审计结束，再见！")
            break
        elif user_input == '':
            audit_stock()  # 回车随机
        else:
            # 兼容用户不输入 sh/sz 前缀的情况
            code = user_input
            if len(code) == 6 and code.isdigit():
                code = f"sh.{code}" if code.startswith('6') else f"sz.{code}"
            audit_stock(code)

if __name__ == "__main__":
    main()