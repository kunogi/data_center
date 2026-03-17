import os
import requests
from dotenv import load_dotenv

load_dotenv()

print("1. 测试 Qwen (阿里云) 连通性...")
try:
    resp = requests.get("https://dashscope.aliyuncs.com", timeout=10)
    print(f"✅ Qwen 握手成功！状态码: {resp.status_code}")
except Exception as e:
    print(f"❌ Qwen 连接失败: {e}")

print("\n2. 测试 Gemini (Google) 连通性...")
try:
    resp = requests.get("https://generativelanguage.googleapis.com", timeout=10)
    print(f"✅ Gemini 握手成功！状态码: {resp.status_code}")
except Exception as e:
    print(f"❌ Gemini 连接失败: {e}")