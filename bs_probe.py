import baostock as bs
import traceback

def run_baostock_probe(test_targets):
    """
    🕵️ 轻量级 Baostock 财报更新探针
    功能：单线程静默探测，避免并发轰炸导致 IP 被封禁。
    """
    print("🕵️ 启动 Baostock 财报更新轻量级探针...")
    print("=" * 60)
    
    # 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"⚠️ 登录 Baostock 失败: {lg.error_msg}")
        return
        
    try:
        hit_count = 0
        for code, year, quarter in test_targets:
            # 选用 query_profit_data (季频盈利能力) 作为探针
            # 只要利润表接口吐出了当期数据，说明 Baostock 后台已经解析完该财报
            rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
            
            if rs.error_code == '0':
                result_df = rs.get_data()
                if not result_df.empty:
                    print(f"  ✅ [命中] {code} 的 {year} 年 Q{quarter} 财报已在 Baostock 上线！")
                    hit_count += 1
                else:
                    print(f"  ⏳ [等待] {code} 的 {year} 年 Q{quarter} 尚未更新。")
            else:
                print(f"  ❌ [请求异常] 探测 {code} 失败: {rs.error_msg}")
                
        print("=" * 60)
        if hit_count > 0:
            print(f"🎯 结论：探针侦测到 {hit_count} 份新财报！")
            print("👉 现在可以安全地拉起 factor_sync.py 进行全量火力同步了。")
        else:
            print("💤 结论：官方依然在沉睡，目标财报全军覆没。")
            print("🛑 请勿启动全量同步，保护 IP 额度，建议明天再探。")
            
    except Exception as e:
        print(f"Exception: 探针运行期间发生严重崩溃: {e}")
        traceback.print_exc()
    finally:
        # 退出系统，释放资源，防止由于没有正常退出导致 Socket 未关闭 (V0.8.9 遗留防范)
        bs.logout()
        print("🔌 探针 Socket 已安全断开。")

if __name__ == "__main__":
    # ==========================================
    # 🎯 探针测试目标配置区
    # 请填入你确切知道“已经发布了财报”的股票代码
    # ==========================================
    targets = [
        ('sh.688252', 2026, 1),  # 代码, 年份, 季度 (1=一季报, 2=中报, 3=三季报, 4=年报)
        ('sz.300442', 2026, 1),
        ('sz.000400', 2026, 1)
    ]
    
    run_baostock_probe(targets)