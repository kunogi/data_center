import os
import sys
import sqlite3
import json
import re
import random
import pandas as pd
from datetime import datetime

# 动态添加上级目录到 sys.path，以便导入 config
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import DB_PATH, MEMORY_DB_PATH

def extract_json_from_text(text: str) -> str:
    try:
        json.loads(text)
        return text
    except:
        pass
    match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
    if match:
        return match.group(1)
    return text

def get_prefix(ticker):
    """补齐股票代码前缀"""
    ticker = str(ticker)
    if ticker.startswith(('6', '68', '51', '56', '58')): return "sh"
    elif ticker.startswith(('00', '30', '15', '16', '12')): return "sz"
    elif ticker.startswith(('8', '4', '9')): return "bj"
    return "sh" if ticker.startswith('6') else "sz"

def evaluate_performance():
    # 1. 获取所有的真实交易日历
    print("📡 正在连接底层数据湖获取日历与记忆...")
    if not os.path.exists(DB_PATH) or not os.path.exists(MEMORY_DB_PATH):
        print(f"❌ 数据库不存在！请检查路径：\n- {DB_PATH}\n- {MEMORY_DB_PATH}")
        return

    conn_quant = sqlite3.connect(DB_PATH)
    dates_df = pd.read_sql_query("SELECT DISTINCT date FROM daily_k_data ORDER BY date", conn_quant)
    trading_days = dates_df['date'].tolist()

    # 2. 提取 AI 的历史推演记忆
    conn_mem = sqlite3.connect(MEMORY_DB_PATH)
    cursor = conn_mem.cursor()
    cursor.execute("SELECT timestamp, run_phase, decision_json FROM ai_journal ORDER BY timestamp")
    rows = cursor.fetchall()
    conn_mem.close()

    results = []

    for row in rows:
        timestamp_str, run_phase, decision_json = row
        ts_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        ts_date_str = ts_dt.strftime('%Y-%m-%d')

        clean_json = extract_json_from_text(decision_json)
        if not clean_json: continue
        try:
            data = json.loads(clean_json)
        except:
            continue

        # 遍历当年推荐的新标的
        new_targets = data.get('new_targets', [])
        for target in new_targets:
            ticker = target.get('ticker')
            name = target.get('name', '未知')
            action = target.get('action', '观察')
            
            if not ticker: continue

            # --- 核心逻辑 1：精确定位买入日 ---
            buy_date = None
            if run_phase == 'INTRADAY':
                # 盘中推荐：当天直接买
                if ts_date_str in trading_days:
                    buy_date = ts_date_str
            elif run_phase in ('PRE_MARKET', 'WEEKEND'):
                # 盘前或周末推荐：下一个即将到来的交易日买
                future_days = [d for d in trading_days if d >= ts_date_str]
                if future_days: buy_date = future_days[0]
            elif run_phase == 'POST_MARKET':
                # 盘后推荐：明天的交易日买
                future_days = [d for d in trading_days if d > ts_date_str]
                if future_days: buy_date = future_days[0]

            if not buy_date:
                continue # 数据没更新到那天，直接跳过

            full_ticker = f"{get_prefix(ticker)}.{ticker}"
            
            # 读取该股票从买入日至今的 K 线
            k_df = pd.read_sql_query(
                f"SELECT date, open, high, low, close FROM daily_k_data WHERE code='{full_ticker}' AND date >= '{buy_date}' ORDER BY date",
                conn_quant
            )
            
            if k_df.empty:
                continue
            
            # --- 核心逻辑 2：地狱级模拟买入价 (防滑点偏见) ---
            # 锁定随机种子，保证每次回测此记录的价格恒定，不漂移
            rnd = random.Random(f"{ticker}_{buy_date}")
            
            buy_row = k_df.iloc[0]
            # 取出当日价格（兜底：如果是涨停一字板可能高低收同价）
            open_p = float(buy_row['open'])
            high_p = float(buy_row['high'])
            close_p = float(buy_row['close'])
            
            if run_phase == 'INTRADAY':
                # 盘中：在最高价和收盘价之间追高买入
                buy_price = rnd.uniform(min(close_p, high_p), max(close_p, high_p))
            else:
                # 盘前/盘后：在次日开盘价与最高价之间冲刺建仓
                buy_price = rnd.uniform(min(open_p, high_p), max(open_p, high_p))

            # --- 核心逻辑 3：严格计算 T+N 收益 ---
            try:
                buy_idx = trading_days.index(buy_date)
            except ValueError:
                continue

            def get_price_for_offset(offset):
                """获取 T+N 交易日的收盘价，如遇停牌取前值"""
                target_idx = buy_idx + offset
                if target_idx >= len(trading_days):
                    return None
                target_date = trading_days[target_idx]
                past_data = k_df[k_df['date'] <= target_date]
                if past_data.empty: return None
                return float(past_data.iloc[-1]['close'])

            def calc_ret(price):
                if not price or buy_price <= 0: return None
                return (price - buy_price) / buy_price * 100

            p1 = get_price_for_offset(1)
            p2 = get_price_for_offset(2)
            p3 = get_price_for_offset(3)
            
            latest_price = float(k_df.iloc[-1]['close'])
            
            results.append({
                '推荐时间': timestamp_str,
                '阶段': run_phase,
                '代码': ticker,
                '名称': name,
                '动作': action,
                '模拟买入价': buy_price,
                'T+1收益': calc_ret(p1),
                'T+2收益': calc_ret(p2),
                'T+3收益': calc_ret(p3),
                '最新收益': calc_ret(latest_price)
            })

    conn_quant.close()

    # 3. 打印极客级报告表格
    print("\n" + "="*110)
    print(f"🎯 AI 选股引擎历史回测质检报告 (地狱防滑点模式)")
    print("="*110)
    
    header = f"{'指令下达时间':<18} | {'场景':<11} | {'名称':<6} | {'建议':<4} | {'模拟成交价':<10} | {'T+1':<8} | {'T+2':<8} | {'T+3':<8} | {'至今累计':<10}"
    print(header)
    print("-" * 110)
    
    for r in results:
        # 格式化收益率，如果还没到那天就显示 "-"
        def fmt_ret(val): return f"{val:>7.2f}%" if val is not None else "      - "
            
        ts_short = r['推荐时间'][:16]
        icon = "🔥" if r['动作'] == '买入' else "👀"
        action_str = f"{icon}{r['动作'][:2]}"
        
        # 兼容对齐 (针对部分终端的中文字符宽度)
        print(f"{ts_short:<18} | {r['阶段']:<11} | {r['名称']:<6} | {action_str:<5} | {r['模拟买入价']:<10.2f} | {fmt_ret(r['T+1收益'])} | {fmt_ret(r['T+2收益'])} | {fmt_ret(r['T+3收益'])} | {fmt_ret(r['最新收益'])}")

    print("="*110)
    print("💡 质检惩罚说明：")
    print("1. [盘中推荐]：强制使用当天的【最高价 ~ 收盘价】作为买入成本，模拟追高滑点。")
    print("2. [盘前/盘后]：强制使用次日的【开盘价 ~ 最高价】作为买入成本，绝不以纸面收盘价欺骗自己。")
    print("3. [防漂移机制]：基于标的与日期的哈希种子已锁定，同等数据下每次测评的随机买价恒定不变。\n")

if __name__ == "__main__":
    evaluate_performance()