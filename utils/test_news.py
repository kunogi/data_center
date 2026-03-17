import akshare as ak
import pandas as pd

def fetch_latest_news(stock_code, limit=3):
    """
    极速抓取个股最新新闻标题
    :param stock_code: 股票代码 (如 'sh600651' 或 '000651')
    """
    # 处理代码格式，akshare 东财接口通常需要纯数字代码
    pure_code = stock_code.split('.')[-1] if '.' in stock_code else stock_code
    
    try:
        print(f"📡 正在连接东方财富舆情接口抓取 {pure_code} ...")
        # 调用 akshare 的东方财富个股新闻接口
        news_df = ak.stock_news_em(symbol=pure_code)
        
        if news_df.empty:
            return "暂无最新重大新闻。"
            
        # 按照时间倒序，提取前 limit 条
        latest_news = news_df.head(limit)
        
        news_list = []
        for _, row in latest_news.iterrows():
            # 提取新闻发布时间和标题进行极简压缩
            time_str = str(row['发布时间'])[:16] # 只保留到分钟
            title = str(row['新闻标题'])
            news_list.append(f"[{time_str}] {title}")
            
        return "\n".join(news_list)
        
    except Exception as e:
        return f"⚠️ 舆情抓取失败: {e}"

if __name__ == "__main__":
    # 我们拿刚才 Z-Score 筛出来的几个票试试水
    test_stocks = ['sz.000651', 'sh.601601', 'sz.001331']
    
    for code in test_stocks:
        print(f"\n================ {code} ================")
        news_text = fetch_latest_news(code)
        print(news_text)