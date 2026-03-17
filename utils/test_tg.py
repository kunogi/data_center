import os
import requests
from dotenv import load_dotenv

# 加载 .env 变量
load_dotenv()

def test_tg_push():
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    proxy_url = os.getenv("HTTPS_PROXY") # 或者用 HTTP_PROXY

    print("--- Telegram 推送专项测试 ---")
    print(f"1. 正在检查配置...")
    print(f"   - Bot Token: {token[:10]}******")
    print(f"   - Chat ID: {chat_id}")
    print(f"   - 代理地址: {proxy_url}")

    if not token or not chat_id:
        print("❌ 错误：.env 文件中缺少 Token 或 Chat ID")
        return

    # 构造代理字典
    # 注意：如果你的代理是 socks5，这里需要改成 socks5://...
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🚀 这是一个来自量化系统测试消息！\n当前时间: " + os.popen('date').read(),
        "parse_mode": "Markdown"
    }

    print(f"\n2. 正在发起网络请求 (超时限制 15s)...")
    try:
        # 我们这里增加 verify=False 排除证书干扰（仅用于测试）
        response = requests.post(url, json=payload, proxies=proxies, timeout=15)
        
        print(f"3. 请求返回状态码: {response.status_code}")
        if response.status_code == 200:
            print("✅ 成功！请检查你的手机 Telegram。")
        else:
            print(f"❌ 失败！服务器返回: {response.text}")

    except requests.exceptions.ProxyError as e:
        print(f"❌ 代理错误：无法连接到你的本地代理软件 (127.0.0.1:10808)。请检查 Clash/V2Ray 是否开启了'系统代理'或'允许局域网连接'。")
    except requests.exceptions.SSLError as e:
        print(f"❌ SSL 错误：这通常是代理协议不匹配。尝试在 .env 中把 http:// 改成 socks5:// 试试？")
        print(f"   具体报错: {e}")
    except Exception as e:
        print(f"❌ 其他异常: {e}")

if __name__ == "__main__":
    test_tg_push()