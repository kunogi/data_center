import sqlite3
import random
import requests
import math
import pandas as pd
from datetime import datetime
from config import DB_PATH 

# 💥 1. 新增安全转换器：专门对付 Baostock 的空字符串和奇葩数据
def safe_float(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0

def fetch_latest_local_data(codes):
    """从本地数据库获取指定股票代码的最新一条 K 线数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        max_date_query = "SELECT MAX(date) FROM daily_k_data"
        cursor = conn.cursor()
        cursor.execute(max_date_query)
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            print("⚠️ 本地数据库为空！")
            return {}

        print(f"📂 锁定本地数据库最新日期为: {latest_date}")
        
        placeholders = ','.join(['?'] * len(codes))
        query = f"""
            SELECT code, open, high, low, close, volume, amount 
            FROM daily_k_data 
            WHERE date = ? AND code IN ({placeholders})
        """
        params = [latest_date] + codes
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        local_data = {}
        for _, row in df.iterrows():
            # 💥 2. 换上防弹转换器，遇到停牌股的空字符串自动填 0.0
            local_data[row['code']] = {
                'open': safe_float(row['open']),
                'high': safe_float(row['high']),
                'low': safe_float(row['low']),
                'close': safe_float(row['close']),
                'volume': safe_float(row['volume']),
                'amount': safe_float(row['amount'])
            }
        return local_data
    except Exception as e:
        print(f"❌ 提取本地数据失败: {e}")
        return {}

def fetch_sina_realtime_data(codes, chunk_size=80):
    """分批次从新浪获取高开低收量额"""
    sina_data = {}
    code_mapping = {c.replace('.', ''): c for c in codes}
    sina_codes_list = list(code_mapping.keys())
    
    chunks = [sina_codes_list[i:i + chunk_size] for i in range(0, len(sina_codes_list), chunk_size)]
    headers = {'Referer': 'https://finance.sina.com.cn'}
    
    for idx, chunk in enumerate(chunks):
        url = f"https://hq.sinajs.cn/list={','.join(chunk)}"
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            resp.encoding = 'gbk'
            lines = resp.text.strip().split('\n')
            
            for line in lines:
                if '="' not in line: continue
                sina_code = line.split('=')[0].replace('var hq_str_', '')
                original_code = code_mapping.get(sina_code)
                if not original_code: continue
                
                data_str = line.split('="')[1].strip('";')
                parts = data_str.split(',')
                
                if len(parts) >= 10:
                    sina_data[original_code] = {
                        'name': parts[0],
                        'open': safe_float(parts[1]),
                        'close': safe_float(parts[3]), 
                        'high': safe_float(parts[4]),
                        'low': safe_float(parts[5]),
                        'volume': safe_float(parts[8]), 
                        'amount': safe_float(parts[9])  
                    }
        except Exception as e:
            print(f"⚠️ 新浪 API 第 {idx+1} 批次请求失败: {e}\n{lines}")
            
    return sina_data

def compare_data(sample_size=100):
    print(f"🔍 开始数据对齐校验 (抽样数量: {sample_size}只) ...\n")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM daily_k_data")
    all_codes = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not all_codes:
        print("❌ 本地数据库中没有股票代码！")
        return
        
    sample_size = min(sample_size, len(all_codes))
    sampled_codes = random.sample(all_codes, sample_size)
    print(f"🎲 已抽取 {sample_size} 只股票进行对比。")
    
    local_data = fetch_latest_local_data(sampled_codes)
    
    # 💥 3. 阻断假阳性：如果提取失败，直接停止运行！绝不掩耳盗铃！
    if not local_data:
        print("\n🚨 致命错误：本地数据提取为空，校验强制中止！请检查上方报错原因。")
        return

    sina_data = fetch_sina_realtime_data(sampled_codes, chunk_size=80)
    
    diff_records = []
    PRICE_TOLERANCE = 0.011 
    
    for code in sampled_codes:
        if code not in local_data:
            continue
        if code not in sina_data:
            # print(f"⚠️ {code} 新浪无返回数据 (可能退市或停牌)")
            continue
            
        loc = local_data[code]
        sin = sina_data[code]
        
        # 过滤停牌股 (开盘价或成交量为0)
        if sin['open'] == 0.0 or sin['volume'] == 0.0 or loc['volume'] == 0.0:
            continue
            
        discrepancies = []
        
        for field in ['open', 'high', 'low', 'close']:
            if not math.isclose(loc[field], sin[field], abs_tol=PRICE_TOLERANCE):
                discrepancies.append(
                    f"{field.upper()}: 本地={loc[field]:.2f} | 新浪={sin[field]:.2f} (差 {loc[field]-sin[field]:.2f})"
                )
                
        if loc['volume'] > 0 and sin['volume'] > 0:
            vol_ratio = loc['volume'] / sin['volume']
            if not (0.99 < vol_ratio < 1.01 or 99 < vol_ratio < 101):
                diff_pct = abs(loc['volume'] - sin['volume']) / sin['volume'] * 100
                discrepancies.append(f"量(VOL)差异: 本地={loc['volume']:.0f} vs 新浪={sin['volume']:.0f} (差 {diff_pct:.2f}%)")

        if loc['amount'] > 0 and sin['amount'] > 0:
            amt_ratio = loc['amount'] / sin['amount']
            if not (0.99 < amt_ratio < 1.01 or 9900 < amt_ratio < 10100):
                diff_pct = abs(loc['amount'] - sin['amount']) / sin['amount'] * 100
                discrepancies.append(f"额(AMT)差异: 本地={loc['amount']:.0f} vs 新浪={sin['amount']:.0f} (差 {diff_pct:.2f}%)")

        if discrepancies:
            diff_records.append({
                'code': code,
                'name': sin['name'],
                'diffs': discrepancies
            })
            
    print("\n" + "="*50)
    if not diff_records:
        print(f"✅ 完美！抽查的高开低收与量额数据完全对齐，无异常！")
    else:
        print(f"❌ 发现 {len(diff_records)} 只股票存在数据差异：\n")
        for rec in diff_records:
            print(f"🔸 {rec['name']} ({rec['code']}):")
            for d in rec['diffs']:
                print(f"    ↳ {d}")
    print("="*50)

if __name__ == "__main__":
    compare_data(sample_size=99999)