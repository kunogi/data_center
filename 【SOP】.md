python sync_all.py

    它其实会先跑财务数据factor_sync.py（会读取completed_financial_codes.txt配合config里的EXPIRE_DAYS定期检查财务数据，如果需要天天刷新财务数据，可以删txt或者把EXPIRE_DAYS改成0）

    再跑k线数据data_sync.py

    自己手工跑也建议按照这个顺序：因为k线数据可能会回退为使用stock_basic表里的股票代码去一个个拉数据，而stock_basic是在factor_sync里维护的

python ak_staging_sync.py

    用东财更快更新的财务数据先造个临时表，用来个screener防雷（注意定期维护里面的财报抓取日期）

python _latest_k_data_verify.py
    
    k线数据同步完成后，使用sina行情串匹配全部今日入库数据有无差异

python _kline_audit.py

    复查近几十天k线数据有无巨大落差，可能为复权数据错误，需要手动重新抓取对应股票
    并且会随机和东财数据比对有无误差
