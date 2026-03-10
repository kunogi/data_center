import os
import baostock as bs
import pandas as pd
import time
import sqlite3
from datetime import datetime, timedelta
from config import DB_PATH, BLACKLIST_FILE, DEFAULT_START_DATE, CORE_INDICES

def get_db_conn():
    return sqlite3.connect(DB_PATH)

def init_database():
    """初始化核心数据库表结构 (仅保留 K 线数据)"""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # K 线表：核心计算引擎的基石
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_k_data (
            date TEXT,
            code TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            pctChg REAL,
            turn REAL,
            PRIMARY KEY (date, code)
        )
    """)
    
    conn.commit()
    conn.close()

def load_blacklist():
    if not os.path.exists(BLACKLIST_FILE):
        return set()
    blacklist = set()
    with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            code = line.split('#')[0].strip()
            if code:
                blacklist.add(code)
    return blacklist

def get_real_target_date():
    """通过 Baostock 获取最近一个合法的交易日"""
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败：{lg.error_msg}")
        return datetime.now().strftime("%Y-%m-%d")
        
    start_lookback = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    end_lookback = datetime.now().strftime("%Y-%m-%d")
    
    rs = bs.query_trade_dates(start_date=start_lookback, end_date=end_lookback)
    dates_df = rs.get_data()
    bs.logout()
    
    valid_dates = dates_df[dates_df['is_trading_day'] == '1']['calendar_date'].values
    if len(valid_dates) > 0:
        last_trade_date = valid_dates[-1]
        # 如果今天是交易日但还未收盘（16点前），则同步到前一个交易日
        if last_trade_date == datetime.now().strftime("%Y-%m-%d") and datetime.now().hour < 16:
            return valid_dates[-2] if len(valid_dates) > 1 else last_trade_date
        return last_trade_date
    return datetime.now().strftime("%Y-%m-%d")

def get_todo_list(target_date, blacklist):
    """【真·全市场动态对齐】通过花名册 Diff 机制，自动捕捉新股并彻底过滤垃圾指数"""
    conn = get_db_conn()
    
    # 1. 获取本地数据库的更新进度字典 {code: max_date}
    sql = "SELECT code, MAX(date) as max_date FROM daily_k_data GROUP BY code"
    df_db = pd.read_sql_query(sql, conn)
    db_progress = dict(zip(df_db['code'], df_db['max_date']))
    conn.close()

    print(f"📡 正在向交易所请求 {target_date} 的全市场动态花名册...")
    
    bs.login() 
    rs = bs.query_all_stock(day=target_date)
    all_active_codes = []
    
    # 💥 核心白名单：只放行沪深纯正A股
    valid_prefixes = ('sh.6', 'sz.00', 'sz.30')
    
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(valid_prefixes) or code in CORE_INDICES:
            all_active_codes.append(code)
            
    bs.logout()
    
    if not all_active_codes:
        print("⚠️ 花名册请求失败，降级为本地存量更新模式...")
        all_active_codes = [c for c in db_progress.keys() if c.startswith(valid_prefixes)] + CORE_INDICES

    # 3. Diff 对比：生成最终待办清单
    todo_list = []
    for code in all_active_codes:
        # 💥 救命补丁：如果是 bj 或 399，必须检查它是不是 VIP 指数。只有不是 VIP 的才杀！
        if (code.startswith('bj.') or code.startswith('sz.399')) and (code not in CORE_INDICES):
            continue
            
        if code in blacklist:
            continue
            
        last_date = db_progress.get(code)
        
        # 逻辑：如果本地没记录 (刚才被你删了)，或者记录过期了，就加入下载队列
        if last_date is None or last_date < target_date:
            todo_list.append(code)
            
    # 保底：无论如何，VIP 指数必须检查
    for idx in CORE_INDICES:
        if idx not in todo_list and idx not in blacklist:
            last_date = db_progress.get(idx)
            if last_date is None or last_date < target_date:
                todo_list.append(idx)

    return list(set(todo_list))

def interactive_filter(todo_list):
    if not todo_list: return []
    if len(todo_list) > 20:
        print(f"🚀 待更新标的较多 ({len(todo_list)} 只)，直接进入全量同步队列...")
        return todo_list
        
    print(f"\n👀 发现少量数据落后标的 ({len(todo_list)} 只)，启动人工确认模式：")
    final_list = []
    auto_action = None 
    
    for code in todo_list:
        if auto_action == 'allY':
            final_list.append(code)
            continue
        elif auto_action == 'allN':
            continue
            
        while True:
            # 🌟 优化：提示信息中加入默认操作说明
            raw_ans = input(f"   发现 {code} 数据落后，是否联网同步？[y/n/allY/allN] [默认 allN]: ").strip()
            
            # 🌟 核心逻辑：处理直接回车的情况
            if not raw_ans:
                ans = 'alln'
            else:
                ans = raw_ans.lower()
                
            if ans == 'y':
                final_list.append(code)
                break
            elif ans == 'n':
                break
            elif ans == 'ally':
                auto_action = 'allY'
                final_list.append(code)
                print("   👉 已选择 allY，后续标的自动加入队列。")
                break
            elif ans == 'alln':
                auto_action = 'allN'
                print("   👉 已选择 allN，后续标的全部跳过。")
                break
            else:
                print("   ⚠️ 输入无效，请准确输入 [y/n/allY/allN]，或直接回车使用默认值 allN。")
    return final_list

def sync_single_stock(code, target_date, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT date, close FROM daily_k_data WHERE code=? ORDER BY date DESC LIMIT 1", (code,))
    row = cursor.fetchone()
    
    need_full_reload = False
    last_date = None
    db_close = None
    
    if row:
        last_date, db_close = row[0], float(row[1])
        fetch_start = last_date
    else:
        need_full_reload = True
        fetch_start = DEFAULT_START_DATE

    rs = bs.query_history_k_data_plus(
        code, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=fetch_start, end_date=target_date,
        frequency="d", adjustflag="3"
    )
    
    data_list = []
    while (rs.error_code == '0') and rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return False, "无新数据 (可能停牌)"

    df = pd.DataFrame(data_list, columns=rs.fields)
    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']]
    for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if not need_full_reload and last_date is not None:
        overlap_row = df[df['date'] == last_date]
        if not overlap_row.empty:
            fetched_close = float(overlap_row.iloc[0]['close'])
            # 校验收盘价，防止除权导致的均线漂移
            if abs(fetched_close - db_close) > 0.01:
                need_full_reload = True
                print(f"\n   🔄 触发自愈：{code} 发生除权，执行全量重载...")
            else:
                df = df[df['date'] > last_date]
        else:
            df = df[df['date'] > last_date]

    if need_full_reload and last_date is not None:
        rs = bs.query_history_k_data_plus(
            code, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date=DEFAULT_START_DATE, end_date=target_date,
            frequency="d", adjustflag="3"
        )
        data_list = []
        while (rs.error_code == '0') and rs.next():
            data_list.append(rs.get_row_data())
            
        if not data_list:
            return False, "全量自愈拉取时无数据"
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']]
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        cursor.execute("DELETE FROM daily_k_data WHERE code=?", (code,))

    if not df.empty:
        df.to_sql('daily_k_data', conn, if_exists='append', index=False)
        conn.commit()
        msg = "自愈重载完成" if need_full_reload else "增量更新成功"
        return True, msg
    else:
        return False, "无增量数据"

# ==========================================
# 🚀 主流程 (纯净 K 线同步模式)
# ==========================================

def run_main_sync():
    init_database()
    
    target_date = get_real_target_date()
    blacklist = load_blacklist()
    
    print(f"\n>>> 开始量化数据同步 (目标交易日：{target_date})")
    if blacklist:
        print(f"🛡️ 已加载黑名单，主动屏蔽 {len(blacklist)} 只标的。")
    
    todo_list = get_todo_list(target_date, blacklist)
    
    if len(todo_list) == 0:
        print("✅ 所有活跃标的数据已对齐，无需更新 K 线。")
        return

    final_sync_list = interactive_filter(todo_list)
    
    if len(final_sync_list) == 0:
        print("✅ 用户取消同步，操作结束。")
        return

    print(f"\n🚀 开始顺序同步 {len(final_sync_list)} 只标的...")
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败：{lg.error_msg}")
        return

    conn = get_db_conn()
    success_count = 0
    total_tasks = len(final_sync_list)
    
    # 💥 1. 记录总开始时间
    start_time = time.time()
    
    for i, code in enumerate(final_sync_list):
        current_idx = i + 1
        try:
            status, msg = sync_single_stock(code, target_date, conn)
            
            # 💥 2. 动态计算 ETA
            elapsed_time = time.time() - start_time
            # 计算平均每个股票耗时，乘以剩余股票数，得出预计剩余时间
            eta_seconds = int((elapsed_time / current_idx) * (total_tasks - current_idx))
            eta_str = str(timedelta(seconds=eta_seconds))
            
            if status is True:
                success_count += 1
                print(f"   [{current_idx}/{total_tasks}] ✅ {code}: {msg} | ETA: {eta_str}")
            else:
                print(f"   [{current_idx}/{total_tasks}] ⏸️ {code}: {msg} | ETA: {eta_str}")
                
        except Exception as e:
            print(f"   [{current_idx}/{total_tasks}] ⚠️ {code} 异常：{e}")
            
    conn.close()
    bs.logout()
    
    # 💥 3. 结束时顺便打印总耗时
    total_elapsed = str(timedelta(seconds=int(time.time() - start_time)))
    print(f"\n✨ 数据对齐完成！成功更新：{success_count} 只标的。总耗时: {total_elapsed}")

if __name__ == "__main__":
    run_main_sync()