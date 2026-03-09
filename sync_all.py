import data_sync
import factor_sync
import sys

def main():
    print("==============================================")
    print(" 🏭 A股量化数据中台 (Data Center) - 统一同步启动")
    print("==============================================")
    
    try:
        # 第一步：先对齐全市场 K 线
        print("\n[1/2] 正在执行全市场 K 线数据对齐...")
        data_sync.run_main_sync()
        
        # 第二步：再拉取活跃股票的财务因子
        print("\n[2/2] 正在执行活跃股票财务因子对齐...")
        factor_sync.run_factor_sync()
        
        print("\n✅✅✅ 数据中台全量同步完成！所有模型现可安全读取。")
    except KeyboardInterrupt:
        print("\n👋 用户主动中断了同步进程。")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 同步过程中发生严重错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()