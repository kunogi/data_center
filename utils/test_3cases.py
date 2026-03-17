import sqlite3
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime
import time
from config import DB_PATH

# ==========================================
# ⚙️ 配置中心
# ==========================================
LOOKBACK_DAYS = 120 

def fetch_latest_news(stock_code, limit=3):
    """极速抓取个股最新新闻标题"""
    pure_code = stock_code.split('.')[-1]
    try:
        news_df = ak.stock_news_em(symbol=pure_code)
        if news_df.empty: return "暂无近期重大新闻。"
        
        latest_news = news_df.head(limit)
        news_list = []
        for _, row in latest_news.iterrows():
            time_str = str(row['发布时间'])[:16] 
            title = str(row['新闻标题']).replace('\n', '').replace('\r', '')
            news_list.append(f"   [{time_str}] {title}")
            
        return "\n".join(news_list)
    except Exception:
        return "   ⚠️ 舆情抓取失败或接口限流"

def load_matrix_data():
    conn = sqlite3.connect(DB_PATH)
    dates_df = pd.read_sql_query(
        f"SELECT DISTINCT date FROM daily_k_data ORDER BY date DESC LIMIT {LOOKBACK_DAYS}", conn
    )
    if dates_df.empty: return None, None, None
        
    start_date = dates_df['date'].min()
    target_date = dates_df['date'].max()

    query = f"SELECT date, code, close, volume FROM daily_k_data WHERE date >= '{start_date}'"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    closes = df.pivot(index='date', columns='code', values='close').ffill()
    volumes = df.pivot(index='date', columns='code', values='volume').fillna(0)
    
    return closes, volumes, target_date

def get_screener_summary_with_news():
    """
    核心汇报引擎：量化筛选 + 舆情挂载
    """
    closes, volumes, target_date = load_matrix_data()
    if closes is None: return "数据库为空。"

    print("🧠 正在进行全市场多因子计算...")
    mean_20 = closes.rolling(window=20).mean()
    std_20 = closes.rolling(window=20).std()
    mean_60 = closes.rolling(window=60).mean()
    vol_mean_5 = volumes.rolling(window=5).mean()

    current_close = closes.iloc[-1]
    current_vol = volumes.iloc[-1]
    last_vol_mean = vol_mean_5.iloc[-1]
    active_mask = (current_vol > 0) & (current_close.notna())

    # 1. Z-Score (取前 5)
    z_score = (closes - mean_20) / std_20
    current_z = z_score.iloc[-1]
    z_candidates = current_z[active_mask & (current_z < -2.0) & (current_vol > last_vol_mean)].sort_values(ascending=True).dropna().head(5)

    # 2. RS (取前 5)
    ret_60 = closes.pct_change(periods=60)
    current_ret_60 = ret_60.iloc[-1]
    rs_rank = current_ret_60[active_mask].rank(pct=True) * 100
    current_mean_20 = mean_20.iloc[-1]
    rs_candidates = rs_rank[(rs_rank > 95.0) & (current_close > current_mean_20)].sort_values(ascending=False).dropna().head(5)

    # 3. VCP (取前 5)
    bbw = (4 * std_20) / mean_20
    current_bbw = bbw.iloc[-1]
    current_mean_60 = mean_60.iloc[-1]
    vcp_rank = current_bbw[active_mask].rank(pct=True, ascending=True) * 100
    vcp_candidates = vcp_rank[(vcp_rank < 5.0) & (current_close > current_mean_60)].sort_values(ascending=True).dropna().head(5)

    print("📡 量化初筛完成，正在接入东方财富提取最新舆情情报...")
    
    report = f"### A股多因子量化初筛报告 (数据日期: {target_date})\n\n"
    
    # 辅助构建报告块的闭包函数
    def build_section(title, logic, candidates, metric_name):
        nonlocal report
        report += f"#### {title}\n> 逻辑：{logic}\n\n"
        if candidates.empty:
            report += "暂无符合条件标的。\n\n"
            return
            
        for code, val in candidates.items():
            print(f"   抓取 {code} 舆情中...")
            news_text = fetch_latest_news(code)
            report += f"* **{code}** ({metric_name}: {val:.2f})\n"
            report += f"{news_text}\n\n"
            time.sleep(0.3) # 极其关键的防封印休眠

    build_section("1. 【Z-Score 极寒恐慌反转池】", "价格跌破20日均线向下2个标准差且放量。寻找被大盘错杀的黄金坑，若新闻显示为基本面实质暴雷则必须剔除。", z_candidates, "Z-Score")
    build_section("2. 【RS 相对强度主升浪池】", "过去60天收益率击败全市场95%的股票。市场绝对核心龙头，需结合新闻判断题材炒作是否有后续空间。", rs_candidates, "全市场击败率%")
    build_section("3. 【VCP 波动率冰点潜伏池】", "多头趋势下布林带极度收敛(全市场最窄前5%)。面临变盘，需通过新闻预判其所属行业是否即将迎来风口。", vcp_candidates, "收敛宽度排位%")

    return report

if __name__ == "__main__":
    final_report = get_screener_summary_with_news()
    print("\n=======================================================")
    print("📋 生成的供 AI 审判官阅读的 Markdown 战报预览：\n")
    print(final_report)