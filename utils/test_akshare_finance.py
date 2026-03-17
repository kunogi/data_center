import akshare as ak
import pandas as pd
import time

print("=== akshare 备选财务接口测试 ===")
print(f"当前 akshare 版本: {ak.__version__}")

test_symbols = ["600000", "000001", "600519"]  # 浦发、平安、茅台

for symbol in test_symbols:
    start_time = time.time()
    print(f"\n--- 测试股票: {symbol} ---")
    
    try:
        # 备选1: stock_individual_info_em (个股基本信息，含 ROE 等)
        print("查询 stock_individual_info_em...")
        df_info = ak.stock_individual_info_em(symbol=symbol)
        if df_info.empty:
            print("  → 空 DataFrame")
        else:
            print("  返回数据（部分字段）：")
            print(df_info.head(10))  # 打印前10行，看 ROE 在哪
            # 常见字段：总市值、市盈率、ROE 等（根据版本）
        
        elapsed = time.time() - start_time
        print(f"  耗时: {elapsed:.2f} 秒")
        
        time.sleep(2)
        
        # 备选2: stock_a_lg_indicator (乐咕乐股财务指标，ROE 叫 roe_ttm)
        print("查询 stock_a_lg_indicator...")
        df_lg = ak.stock_a_lg_indicator(symbol=symbol)
        if df_lg.empty:
            print("  → 空 DataFrame")
        else:
            latest = df_lg.sort_values("date", ascending=False).iloc[0]
            roe_ttm = latest.get("roe_ttm", "未找到 roe_ttm")
            profit_yoy = latest.get("net_profit_yoy", "未找到 net_profit_yoy")
            print(f"  ROE_TTM: {roe_ttm}")
            print(f"  净利润同比增长: {profit_yoy}")
        
        elapsed2 = time.time() - start_time - elapsed
        print(f"  备选2 耗时: {elapsed2:.2f} 秒")
        
    except Exception as e:
        print(f"  异常: {str(e)}")
    
    print("=" * 60)

print("\n测试结束。如果有 ROE 值 → 可用！")