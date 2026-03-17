import sqlite3
import random
import requests
import time
from config import DB_PATH

def validate_k_data(stock_sample_size=5, history_sample_size=3):
    print(f"🕵️ 启动多维数据质检 (抽取 {stock_sample_size} 只股票，每只校验最新 1 天 + 随机 {history_sample_size} 天)...\n")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 随机抽取 5 只股票代码
    cursor.execute(f"SELECT DISTINCT code FROM daily_k_data ORDER BY RANDOM() LIMIT {stock_sample_size}")
    sampled_codes = [row[0] for row in cursor.fetchall()]
    
    if not sampled_codes:
        print("⚠️ 数据库为空，请先运行 data_sync.py。")
        conn.close()
        return

    total_checks = 0
    error_count = 0

    for code in sampled_codes:
        print(f"🔍 正在核对标的: {code}")
        
        # 2. 获取该股票在本地的所有历史数据
        cursor.execute("SELECT date, close FROM daily_k_data WHERE code=?", (code,))
        local_rows = cursor.fetchall()
        if not local_rows:
            continue
            
        local_data_map = {row[0]: float(row[1]) for row in local_rows}
        
        # 3. 确定这只股票的“最新一天”和“随机三天”
        sorted_dates = sorted(local_data_map.keys())
        latest_date = sorted_dates[-1]
        
        historical_dates = sorted_dates[:-1]
        random_dates = random.sample(historical_dates, min(history_sample_size, len(historical_dates)))
        
        dates_to_check = [latest_date] + random_dates
        
        # 4. 请求腾讯 API 获取前复权历史 K 线 
        tx_code = code.replace('.', '')
        # 🌟 修复 1：降低单次拉取数量到 660 天，避免触发腾讯接口的参数风控
        # 🌟 修复 3：切换为最稳定的经典复权接口 fqkline，并将天数稳定在 640 天（约 2.5 年）
        url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={tx_code},day,,,640,qfq"
        
        # 🌟 修复 4：增加浏览器伪装，防止被 WAF (Web防火墙) 直接拦截
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            resp = requests.get(url, headers=headers, timeout=5)
        
            data = resp.json()
            
            # 🌟 修复 2：安全防御，防止 API 报错时返回 {"data": ""} 导致的连环报错
            data_body = data.get('data')
            if not isinstance(data_body, dict):
                print(f"   ➖ 验证跳过: 腾讯接口拒绝请求或未返回有效字典 (响应: {str(data)}...)")
                continue
                
            stock_data = data_body.get(tx_code)
            if not isinstance(stock_data, dict):
                print(f"   ➖ 验证跳过: 腾讯接口无此标的明细数据")
                continue
            
            # 解析腾讯数据结构
            k_list = stock_data.get('qfqday', [])
            if not k_list:
                k_list = stock_data.get('day', [])
                
            # 将腾讯的 List 转为 Dict 加速查询
            tx_data_map = {}
            for day_data in k_list:
                tx_data_map[day_data[0]] = float(day_data[2])
                
            # 5. 开始逐日比对
            for check_date in dates_to_check:
                local_close = local_data_map[check_date]
                tx_close = tx_data_map.get(check_date)
                
                label = "【最新】" if check_date == latest_date else "【历史】"
                
                if tx_close is None:
                    # 如果随机抽到了 660 天以前的旧数据，腾讯接口里自然找不到，正常跳过
                    print(f"   ➖ {label} {check_date} | 验证跳过: 腾讯接口本次无此日数据 (可能超出660天范围)")
                    continue
                
                total_checks += 1
                diff = abs(local_close - tx_close)
                
                # 容差设定为 0.02 元
                if diff <= 0.02:
                    print(f"   ✅ {label} {check_date} | 数据吻合 (本地: {local_close:.2f} == 腾讯: {tx_close:.2f})")
                else:
                    error_count += 1
                    diff_pct = (diff / tx_close) * 100
                    print(f"   ❌ {label} {check_date} | 数据异常 (本地: {local_close:.2f} != 腾讯: {tx_close:.2f} | 误差: {diff_pct:.2f}%)")
                    
        except Exception as e:
            print(f"   ⚠️ 请求/解析第三方 API 异常: {e}")
            
        time.sleep(0.5)
        print("-" * 40)

    conn.close()
    
    # 总结报告
    print("\n" + "=" * 40)
    print(f"🎯 交叉验证完成！共实际比对 {total_checks} 个数据点。")
    if total_checks == 0:
        print("⚠️ 警告：未能完成任何比对，可能是网络问题或接口全线拦截。")
    elif error_count == 0:
        print("🟢 完美！抽样比对显示本地数据库底层非常健康，复权处理精准无误。")
    else:
        print(f"🔴 警告：发现 {error_count} 处异常，请关注。")
    print("=" * 40 + "\n")

if __name__ == "__main__":
    validate_k_data()