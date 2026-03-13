import os

# ==========================================
# ⚙️ 配置中心 (改为绝对路径)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'quant_data.db')
BLACKLIST_FILE = os.path.join(BASE_DIR, 'blacklist.txt')
COMPLETED_FILE = os.path.join(BASE_DIR, 'completed_financial_codes.txt')
DEFAULT_START_DATE = '2020-01-01'
# K线数据获取天数
DAILY_K_DAYS = 365

# 💥 【全新架构参数】连续获取过去几个季度的财报 
# 12个季度 = 3年，用于后续计算“连续3年ROE”、“连续3年盈利质量”等护城河指标
FINANCIAL_QUARTERS = 12

# 写死我们永远需要跟踪的核心指数 (保底名单)
CORE_INDICES = ['sh.000001', 'sz.399001', 'sz.399107', 'sh.000300', 'sz.399006', 'sh.000905', 'sh.000852', 'bj.899050']