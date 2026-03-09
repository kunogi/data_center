import os

# ==========================================
# ⚙️ 配置中心 (改为绝对路径)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'quant_data.db')
BLACKLIST_FILE = os.path.join(BASE_DIR, 'blacklist.txt')
COMPLETED_FILE = os.path.join(BASE_DIR, 'completed_financial_codes.txt')
DEFAULT_START_DATE = '2020-01-01'