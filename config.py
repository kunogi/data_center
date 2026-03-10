import os

# ==========================================
# ⚙️ 配置中心 (改为绝对路径)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'quant_data.db')
BLACKLIST_FILE = os.path.join(BASE_DIR, 'blacklist.txt')
COMPLETED_FILE = os.path.join(BASE_DIR, 'completed_financial_codes.txt')
DEFAULT_START_DATE = '2020-01-01'

# 写死我们永远需要跟踪的核心指数 (保底名单)
CORE_INDICES = ['sh.000001', 'sz.399001', 'sz.399107', 'sh.000300', 'sz.399006', 'sh.000905', 'sh.000852', 'bj.899050']