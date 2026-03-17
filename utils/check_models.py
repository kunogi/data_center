import os
from dotenv import load_dotenv
from google import genai

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")

if not api_key:
    print("❌ 未找到 GEMINI_API_KEY，请检查 .env 文件！")
    exit()

print(">> 📡 正在向 Google 服务器请求当前 API Key 的可用模型列表...\n")

try:
    client = genai.Client(api_key=api_key)
    models = client.models.list()
    
    count = 0
    for model in models:
        # 直接打印纯净的模型名称，去掉 models/ 前缀
        pure_name = model.name.replace("models/", "")
        print(f"🔥 {pure_name} | {model.display_name}")
        count += 1
            
    print(f"\n✅ 扫描完毕，共拉取到 {count} 个模型权限。")

except Exception as e:
    print(f"❌ 获取模型列表失败: {e}")