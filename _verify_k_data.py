import sqlite3
import random
import requests
import math
import pandas as pd
from datetime import datetime
from config import DB_PATH  # 确保这里能引到你的数据库路径

def fetch_latest_local_data(codes):
    """从本地数据库获取指定股票代码的最新一条 K 线数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        # 为了高效，先找出数据库中最近的一个交易日
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
        
        # 转为字典格式方便对比: { 'sh.600000': {'open': 10.0, ...} }
        local_data = {}
        for _, row in df.iterrows():
            local_data[row['code']] = {
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row['volume']),
                'amount': float(row['amount'])
            }
        return local_data
    except Exception as e:
        print(f"❌ 提取本地数据失败: {e}")
        return {}

def fetch_sina_realtime_data(codes, chunk_size=80):
    """分批次从新浪获取高开低收量额"""
    sina_data = {}
    
    # 转换代码格式: sh.600000 -> sh600000
    code_mapping = {c.replace('.', ''): c for c in codes}
    sina_codes_list = list(code_mapping.keys())
    
    # 分批次 (Chunking)
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
                # 解析新浪返回的字符串
                sina_code = line.split('=')[0].replace('var hq_str_', '')
                original_code = code_mapping.get(sina_code)
                if not original_code: continue
                
                data_str = line.split('="')[1].strip('";')
                parts = data_str.split(',')
                
                # 新浪返回字段长度校验 (正常股票长度大于30)
                if len(parts) >= 10:
                    sina_data[original_code] = {
                        'name': parts[0],
                        'open': float(parts[1]),
                        'close': float(parts[3]), # 现价即收盘价
                        'high': float(parts[4]),
                        'low': float(parts[5]),
                        'volume': float(parts[8]), # 成交量 (股)
                        'amount': float(parts[9])  # 成交额 (元)
                    }
        except Exception as e:
            print(f"⚠️ 新浪 API 第 {idx+1} 批次请求失败: {e}")
            
    return sina_data

def compare_data(sample_size=100):
    print(f"🔍 开始数据对齐校验 (抽样数量: {sample_size}只) ...\n")
    
    # 1. 获取本地数据库全量股票池
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM daily_k_data")
    all_codes = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not all_codes:
        print("❌ 本地数据库中没有股票代码！")
        return
        
    # 2. 随机抽样
    sample_size = min(sample_size, len(all_codes))
    sampled_codes = random.sample(all_codes, sample_size)
    print(f"🎲 已随机抽取 {sample_size} 只股票进行对比。")
    
    # 3. 获取双方数据
    local_data = fetch_latest_local_data(sampled_codes)
    sina_data = fetch_sina_realtime_data(sampled_codes, chunk_size=80)
    
    # 4. 对比逻辑
    diff_records = []
    
    # 价格容忍度 (1分钱)
    PRICE_TOLERANCE = 0.011 
    
    for code in sampled_codes:
        if code not in local_data:
            continue
        if code not in sina_data:
            print(f"⚠️ {code} 新浪无返回数据 (可能退市或停牌)")
            continue
            
        loc = local_data[code]
        sin = sina_data[code]
        
        # 过滤停牌股 (开盘价为0)
        if sin['open'] == 0.0:
            continue
            
        discrepancies = []
        
        # 4.1 对比价格 (高、开、低、收)
        for field in ['open', 'high', 'low', 'close']:
            if not math.isclose(loc[field], sin[field], abs_tol=PRICE_TOLERANCE):
                discrepancies.append(
                    f"{field.upper()}: 本地={loc[field]:.2f} | 新浪={sin[field]:.2f} (差 {loc[field]-sin[field]:.2f})"
                )
                
        # 4.2 对比量额 (因为不同数据源可能有“股/手”或“元/万元”的单位差异，这里用比例差异对比)
        # 容忍度设为 1% (0.01)，因为部分数据源的成交量可能包含盘后交易或存在轻微四舍五入
        VOL_TOLERANCE = 0.01 
        
        # 修正可能存在的单位不一致 (通常新浪是 股，数据库如果是 手，会差100倍)
        # 我们用科学计数法或百分比差异来看
        if loc['volume'] > 0 and sin['volume'] > 0:
            vol_ratio = loc['volume'] / sin['volume']
            # 如果不是 1 倍 (±1%)，也不是 100 倍 (±1%) 或 10000 倍，说明数据真有差异
            if not (0.99 < vol_ratio < 1.01 or 99 < vol_ratio < 101):
                diff_pct = abs(loc['volume'] - sin['volume']) / sin['volume'] * 100
                discrepancies.append(f"量(VOL)差异: 本地={loc['volume']:.0f} vs 新浪={sin['volume']:.0f} (差 {diff_pct:.2f}%)")

        if loc['amount'] > 0 and sin['amount'] > 0:
            amt_ratio = loc['amount'] / sin['amount']
            # 同理，如果单位不是1倍、10000倍(万元)等
            if not (0.99 < amt_ratio < 1.01 or 9900 < amt_ratio < 10100):
                diff_pct = abs(loc['amount'] - sin['amount']) / sin['amount'] * 100
                discrepancies.append(f"额(AMT)差异: 本地={loc['amount']:.0f} vs 新浪={sin['amount']:.0f} (差 {diff_pct:.2f}%)")

        if discrepancies:
            diff_records.append({
                'code': code,
                'name': sin['name'],
                'diffs': discrepancies
            })
            
    # 5. 打印报告
    print("\n" + "="*50)
    if not diff_records:
        print(f"✅ 完美！抽查的 {sample_size} 只股票，高开低收与量额数据完全对齐，无异常！")
    else:
        print(f"❌ 发现 {len(diff_records)} 只股票存在数据差异：\n")
        for rec in diff_records:
            print(f"🔸 {rec['name']} ({rec['code']}):")
            for d in rec['diffs']:
                print(f"    ↳ {d}")
    print("="*50)

if __name__ == "__main__":
    # 随机抽样 200 只股票进行核对
    compare_data(sample_size=7000)