import baostock as bs
import pandas as pd

def test_baostock_fields(code="sh.600519", year="2024", quarter="3"):
    """
    测试 Baostock 财务接口，直接打印底层 DataFrame 看真实字段
    注：为了保证测试100%有数据，这里默认测试 2024 年三季报(Q3)
    """
    print("🔄 正在连接 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return

    print(f"\n🎯 正在获取 {code} {year}年Q{quarter} 的财务数据...\n")

    # 1. 利润表 (取净利润)
    profit_rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
    profit_df = profit_rs.get_data()
    
    # 2. 资产负债表 (取负债率)
    balance_rs = bs.query_balance_data(code=code, year=year, quarter=quarter)
    balance_df = balance_rs.get_data()
    
    # 3. 现金流量表 (取现金流比例)
    cash_rs = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
    cash_df = cash_rs.get_data()

    print("="*50)
    print("📑 1. 资产负债表 (Balance Sheet) 原始字段：")
    if not balance_df.empty:
        print("所有列名:", balance_df.columns.tolist())
        print("实际数据:\n", balance_df.iloc[0].to_dict())
        # 尝试获取负债率
        print(f"👉 提取到的负债率 (liabilityToAsset): {balance_df.iloc[0].get('liabilityToAsset', '不存在')}")
    else:
        print("⚠️ 资产负债表无数据")

    print("\n" + "="*50)
    print("📑 2. 现金流量表 (Cash Flow) 原始字段：")
    if not cash_df.empty:
        print("所有列名:", cash_df.columns.tolist())
        print("实际数据:\n", cash_df.iloc[0].to_dict())
        # 尝试获取经营现金流/净利润比
        print(f"👉 提取到的现金流净利润比 (CFOToNP): {cash_df.iloc[0].get('CFOToNP', '不存在')}")
    else:
        print("⚠️ 现金流量表无数据")

    print("\n" + "="*50)
    print("📑 3. 关联计算测试：")
    if not profit_df.empty and not cash_df.empty:
        net_profit = float(profit_df.iloc[0].get('netProfit', 0) or 0)
        cfo_ratio = float(cash_df.iloc[0].get('CFOToNP', 0) or 0)
        cfo_absolute = net_profit * cfo_ratio
        print(f"👉 净利润: {net_profit / 1e8:.2f} 亿")
        print(f"👉 现金流比例: {cfo_ratio:.4f}")
        print(f"👉 推算出的经营现金流绝对值: {cfo_absolute / 1e8:.2f} 亿")

    bs.logout()
    print("✅ 测试结束。")

if __name__ == '__main__':
    # 你可以换成你截图里那些全为0的股票代码之一进行测试，比如 sh.600197
    test_baostock_fields(code="sh.600197", year="2024", quarter="3")