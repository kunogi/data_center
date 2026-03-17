import requests

def check_sina_status():
    
    raw_list = [
        "","zjlxn_sz300308"
    ]
#http://hq.sinajs.cn/list=zjlx_sh600000 （含集合）
#http://hq.sinajs.cn/list=zjlxn_sh000001



    # 拼接 Sina 接口 URL
    base_url = "https://hq.sinajs.cn/list=" + ",".join(raw_list)
    headers = {'Referer': 'https://finance.sina.com.cn'}

    print(f"正在穿透探测 {len(raw_list)} 只标的的实时状态...\n")
    
    try:
        print(f"请求 URL: {base_url}\n")
        resp = requests.get(base_url, headers=headers, timeout=10)
        resp.encoding = 'gbk'
        lines = resp.text.strip().split('\n')
        
        dead_count = 0
        for line in lines:
            if '="' not in line: continue
            stock_code = line.split('=')[0].split('_')[-1]
            data = line.split('="')[1].strip('";')
            
            # 如果返回数据为空，或者长度极短，基本确定摘牌
            if not data or len(data.split(',')) < 5:
                print(f"❌ {stock_code}: [无数据] -> 确认摘牌或退市")
                dead_count += 1
            else:
                print(data)
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    check_sina_status()