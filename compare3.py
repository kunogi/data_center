import sqlite3
import akshare as ak
import pandas as pd
import random
from typing import Dict, Optional

# ==========================================
# 1. 配置区域
# ==========================================
TARGET_STAT_DATE = "2025-12-31"

# ==========================================
# 2. 辅助函数：读取本地数据
# ==========================================
def get_local_data(db_path: str) -> Optional[Dict]:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        
        # 找两个表都有的报告期
        target_dates = [TARGET_STAT_DATE, "2026-03-31", "2025-09-30"]
        final_stat_date = None
        common_codes = []
        
        for date in target_dates:
            cursor.execute("SELECT DISTINCT code FROM financial_factors WHERE stat_date = ?", (date,))
            codes_bs = set([row[0] for row in cursor.fetchall()])
            cursor.execute("SELECT DISTINCT code FROM financial_factors_ak WHERE stat_date = ?", (date,))
            codes_ak = set([row[0] for row in cursor.fetchall()])
            common = codes_bs & codes_ak
            if common:
                final_stat_date = date
                common_codes = list(common)
                break
        
        if not final_stat_date:
            print(f"❌ 未找到重叠数据")
            return None
        
        print(f"✅ 共同报告期：{final_stat_date}，重叠股票数：{len(common_codes)}")
        
        # 随机选股
        target_code = random.choice(common_codes)
        print(f"🎲 选中股票：{target_code}")
        
        # 读取Baostock
        cursor.execute("SELECT net_profit,cash_flow,eps_ttm,gp_margin,roe_avg,cfo_to_np FROM financial_factors WHERE code = ? AND stat_date = ?", (target_code, final_stat_date))
        row_bs = cursor.fetchone()
        cols_bs = [desc[0] for desc in cursor.description]
        data_bs = dict(zip(cols_bs, row_bs))
        
        # 读取Ak东财
        cursor.execute("SELECT net_profit,cash_flow,eps_ttm,gp_margin,roe_avg,cfo_to_np FROM financial_factors_ak WHERE code = ? AND stat_date = ?", (target_code, final_stat_date))
        row_ak = cursor.fetchone()
        cols_ak = [desc[0] for desc in cursor.description]
        data_ak = dict(zip(cols_ak, row_ak))
        
        conn.close()
        return {"code": target_code, "stat_date": final_stat_date, "bs": data_bs, "dc": data_ak}
    
    except Exception as e:
        print(f"❌ 读取失败：{str(e)}")
        return None

# ==========================================
# 3. 辅助函数：拉取新浪数据（【最终修正】取对字段）
# ==========================================
def get_sina_data(code_bs: str, stat_date: str) -> Optional[Dict]:
    code_ak = f"{code_bs[:2]}{code_bs[3:]}"
    stat_date_num = stat_date.replace("-", "")
    
    try:
        print(f"\n正在拉取新浪 {code_ak} {stat_date} ...")
        
        df_pft = ak.stock_financial_report_sina(stock=code_ak, symbol="利润表")
        df_bal = ak.stock_financial_report_sina(stock=code_ak, symbol="资产负债表")
        df_csh = ak.stock_financial_report_sina(stock=code_ak, symbol="现金流量表")
        
        df_pft["报告日"] = df_pft["报告日"].astype(str)
        df_bal["报告日"] = df_bal["报告日"].astype(str)
        df_csh["报告日"] = df_csh["报告日"].astype(str)
        
        pft = df_pft[df_pft["报告日"] == stat_date_num].iloc[0].to_dict()
        bal = df_bal[df_bal["报告日"] == stat_date_num].iloc[0].to_dict()
        csh = df_csh[df_csh["报告日"] == stat_date_num].iloc[0].to_dict()
        
        print(f"✅ 新浪数据拉取成功")
        
        # 【核心修正】计算字段
        data = {"code": code_bs, "stat_date": stat_date}
        
        # 1. 核心利润：【修正】直接取新浪的"净利润"字段（索引47）
        data["net_profit"] = pft.get("净利润")
        
        # 2. 其他基础数据
        data["mb_revenue"] = pft.get("营业收入")
        data["cash_flow"] = csh.get("经营活动产生的现金流量净额")
        data["eps_ttm"] = pft.get("基本每股收益")
        
        # 3. 计算比率
        revenue = pft.get("营业收入", 0)
        cost = pft.get("营业成本", 0)
        total_net_profit = data["net_profit"]
        
        if revenue and pd.notna(revenue) and revenue != 0:
            if cost and pd.notna(cost):
                data["gp_margin"] = (revenue - cost) / revenue
            if total_net_profit and pd.notna(total_net_profit):
                data["np_margin"] = total_net_profit / revenue * 100
        
        if total_net_profit and pd.notna(total_net_profit) and total_net_profit != 0 and data["cash_flow"]:
            data["cfo_to_np"] = data["cash_flow"] / total_net_profit
        if revenue and pd.notna(revenue) and revenue != 0 and data["cash_flow"]:
            data["cfo_to_gr"] = data["cash_flow"] / revenue
        
        # ROE
        net_assets = bal.get("所有者权益合计")
        parent_net_profit = pft.get("归属于母公司所有者的净利润")
        if net_assets and pd.notna(net_assets) and net_assets != 0 and parent_net_profit:
            data["roe_avg"] = parent_net_profit / net_assets * 100
        
        return data
    
    except Exception as e:
        print(f"❌ 新浪拉取失败：{str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ==========================================
# 4. 辅助函数：三方对比
# ==========================================
def compare_three(local_data: Dict, sina_data: Dict):
    code = local_data["code"]
    date = local_data["stat_date"]
    bs = local_data["bs"]
    dc = local_data["dc"]
    sn = sina_data
    
    print("\n" + "="*140)
    print(f"📊 三方对比 (合并净利润口径) | {code} | {date}")
    print("="*140)
    
    fields = [
        "net_profit", "cash_flow", "eps_ttm", 
        "gp_margin", "roe_avg", 
        "cfo_to_np"
    ]
    #net_profit用sina
    #cash_flow难，勉强用sina（bao会莫名其妙0，负肯定0）
    #eps_ttm
    #gp_margin 3家高度一致
    #roe_avg勉强用东财，sina没数据
    #cfo_to_np难，勉强sina（bao会莫名其妙0）

    
    print(f"{'字段名':<18} | {'Baostock':<22} | {'Ak东财':<22} | {'Ak新浪':<22} | {'状态':<10}")
    print("-"*140)
    
    for field in fields:
        def get_val(data, key):
            v = data.get(key)
            return v if pd.notna(v) else None
        
        v_bs = get_val(bs, field)
        v_dc = get_val(dc, field)
        v_sn = get_val(sn, field) if sn else None
        
        def fmt(v):
            if v is None:
                return "N/A"
            if isinstance(v, float):
                if abs(v) > 1000:
                    return f"{v:.2f}"
                else:
                    return f"{v:.8f}"
            return str(v)
        
        s_bs, s_dc, s_sn = fmt(v_bs), fmt(v_dc), fmt(v_sn)
        
        status = ""
        if v_bs is not None:
            def is_equal(a, b):
                if a is None or b is None:
                    return False
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return abs(a - b) < 1e-6 or (abs(a) > 1e-8 and abs((a - b) / a) < 1e-4)
                return str(a) == str(b)
            
            eq_dc = is_equal(v_bs, v_dc)
            eq_sn = is_equal(v_bs, v_sn)
            
            if eq_dc and eq_sn:
                status = "✅ 三方一致"
            elif eq_sn:
                status = "🟡 新浪一致"
            elif eq_dc:
                status = "🟡 东财一致"
            else:
                status = "🔴 均不一致"
        
        print(f"{field:<18} | {s_bs:<22} | {s_dc:<22} | {s_sn:<22} | {status:<10}")
    
    print("="*140)

# ==========================================
# 5. 主程序
# ==========================================
if __name__ == "__main__":
    print("="*140)
    print("三方对比 (合并净利润口径 - 最终修正版)")
    print("="*140)
    
    db_path = 'quant_data.db'#input("请输入数据库路径：").strip()
    if not db_path:
        exit()
    
    local_result = get_local_data(db_path)
    if not local_result:
        exit()
    
    sina_result = get_sina_data(local_result["code"], local_result["stat_date"])
    compare_three(local_result, sina_result)