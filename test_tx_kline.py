import requests
import json

def get_tencent_qfq_close(code, target_date):
    """
    🔌 腾讯财经前复权 K 线获取探针
    code: 'sh.600685' 或 'sz.159890'
    target_date: '2026-04-17'
    """
    # 格式转换: 'sh.600685' -> 'sh600685'
    tx_code = code.replace('.', '')
    
    # 腾讯官方接口: qfq 代表前复权，10 代表获取近10天数据（留余量防节假日）
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={tx_code},day,{target_date},{target_date},10,qfq"
    
    # 💥 物理封锁代理：强制直连，防止 WSL 镜像网络把国内请求扔进黑洞
    proxies = {
        "http": None,
        "https": None
    }
    
    try:
        resp = requests.get(url, proxies=proxies, timeout=5)
        data = resp.json()
        
        if data.get('code') == 0:
            stock_data = data['data'].get(tx_code, {})
            if not stock_data:
                return None, "找不到该标的"
                
            # 腾讯的数据结构：如果有复权数据，在 'qfqday' 里；如果是刚上市/无复权，在 'day' 里
            kline_list = stock_data.get('qfqday') or stock_data.get('day')
            
            if kline_list:
                for k in kline_list:
                    # k 的结构: [日期, 开, 收, 高, 低, 成交量]
                    if k[0] == target_date:
                        return float(k[2]), "成功"
                        
            return None, f"无 {target_date} 当日数据(可能停牌或非交易日)"
            
        return None, f"接口返回异常: {data.get('msg')}"
        
    except Exception as e:
        return None, f"网络请求崩溃: {str(e)}"

# ==========================================
# 🚀 探针测试单元
# ==========================================
if __name__ == "__main__":
    test_cases = [
        ("sh.600685", "2026-04-17", "正常股票"),
        ("sz.159890", "2026-04-13", "刚才报错的 ETF"),
        ("sh.000858", "2026-04-17", "刚才代理超时的标的")
    ]
    
    print("📡 启动腾讯财经直连探针测试...")
    print("-" * 50)
    for code, date, desc in test_cases:
        price, msg = get_tencent_qfq_close(code, date)
        if price is not None:
            print(f"✅ {code} ({desc}) @ {date} -> 收盘价: {price:.2f}")
        else:
            print(f"❌ {code} ({desc}) @ {date} -> 失败原因: {msg}")