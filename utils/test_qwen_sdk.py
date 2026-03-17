import os
import httpx
from openai import OpenAI
from dotenv import load_dotenv

# 屏蔽 httpx 忽略证书时的警告输出
import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

load_dotenv()

API_KEY = os.getenv("QWEN_API_KEY")
BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL = "qwen-plus"

print("="*60)
print("🚀 Qwen (DashScope) SDK 底层网络连通性对比测试")
print("="*60)

if not API_KEY:
    print("❌ 错误: 未找到 QWEN_API_KEY 环境变量，请检查 .env 文件。")
    exit()

# ---------------------------------------------------------
# [测试 1] 原生默认客户端 (易受 WSL 代理环境变量污染)
# ---------------------------------------------------------
print("\n▶️ [测试 1]: 原生默认客户端 (模拟当前系统行为)...")
try:
    # 设置 10 秒超时，避免让你等 150 秒
    client_default = OpenAI(
        api_key=API_KEY,
        base_url=BASE_URL,
        timeout=10.0 
    )
    response = client_default.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "1+1等于几？请只回答数字。"}],
        max_tokens=10
    )
    print(f"✅ [测试 1] 成功！Qwen 存活，返回结果: {response.choices[0].message.content}")
except Exception as e:
    print(f"❌ [测试 1] 失败: {e}")
    print("   (👆 这应该就是你刚才遇到的 timeout 报错)")

# ---------------------------------------------------------
# [测试 2] 注入强制直连客户端 (我们的修复方案)
# ---------------------------------------------------------
print("\n▶️ [测试 2]: 物理切断代理的定制客户端 (强制直连)...")
try:
    # 核心：proxies=None 忽略所有代理变量，verify=False 忽略 WSL 证书问题
    custom_http_client = httpx.Client(proxies=None, verify=False)
    
    client_custom = OpenAI(
        api_key=API_KEY,
        base_url=BASE_URL,
        http_client=custom_http_client,
        timeout=10.0
    )
    response = client_custom.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "2+2等于几？请只回答数字。"}],
        max_tokens=10
    )
    print(f"✅ [测试 2] 成功！Qwen 存活，返回结果: {response.choices[0].message.content}")
    print("\n🎉 结论: 物理切断代理方案完全有效，网络已打通！")
except Exception as e:
    print(f"❌ [测试 2] 失败: {e}")
    print("   (⚠️ 如果这里也失败，说明不是代理问题，而是 WSL 本身失去了基础外网连接)")

print("\n" + "="*60)