#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare Plus 高级用法示例

本示例展示了 Tushare Plus 客户端的高级功能，包括：
1. 禁用频率限制
2. 自定义并发设置
3. 错误处理和重试机制
4. 大数据量处理策略
"""

import os
import time
import pandas as pd
from tushare_plus import TushareAPI

# 从环境变量获取 token，或者使用默认值
TOKEN = os.environ.get("TUSHARE_TOKEN", "your_token_here")

def disable_rate_limit_example():
    """
    禁用频率限制示例
    
    当您需要快速获取大量数据且不担心触发 API 调用限制时，
    可以禁用频率限制以提高数据获取速度。
    注意：请确保您的账户有足够的积分，否则可能会被限制访问。
    """
    print("\n=== 禁用频率限制示例 ===")
    
    # 初始化客户端，禁用频率限制
    client = TushareAPI(token=TOKEN, enable_rate_limit=False)
    
    start_time = time.time()
    
    # 获取股票列表
    df_stocks = client.get_data(
        api_name="stock_basic",
        fields="ts_code,name,industry,area",
        list_status="L"
    )
    
    # 获取前5只股票的日线数据
    sample_stocks = df_stocks['ts_code'].head(5).tolist()
    ts_code = ",".join(sample_stocks)
    
    df_daily = client.get_data(
        api_name="daily",
        fields="ts_code,trade_date,open,high,low,close,vol",
        ts_code=ts_code,
        start_date="20200101",
        end_date="20201231"
    )
    
    end_time = time.time()
    print(f"禁用频率限制获取 {len(df_daily)} 条数据耗时: {end_time - start_time:.2f} 秒")
    print(f"获取的股票: {', '.join(sample_stocks)}")
    print(df_daily.head())

def custom_concurrency_example():
    """
    自定义并发设置示例
    
    通过调整并发工作线程数量，可以根据网络环境和计算资源优化数据获取性能。
    """
    print("\n=== 自定义并发设置示例 ===")
    
    # 使用不同的并发设置进行对比
    concurrency_settings = [1, 3, 5, 10]
    results = []
    
    for max_workers in concurrency_settings:
        # 初始化客户端，设置自定义并发数
        client = TushareAPI(token=TOKEN, max_workers=max_workers)
        
        print(f"\n使用 {max_workers} 个工作线程:")
        start_time = time.time()
        
        # 获取沪深300成分股的日线数据（这里仅使用前10只股票作为示例）
        # 实际使用时可以通过 index_weight 接口获取成分股
        sample_stocks = [
            "600000.SH", "600036.SH", "601318.SH", "600519.SH", "601166.SH",
            "000001.SZ", "000333.SZ", "000651.SZ", "000858.SZ", "002415.SZ"
        ]
        ts_code = ",".join(sample_stocks)
        
        df = client.get_data(
            api_name="daily",
            fields="ts_code,trade_date,open,high,low,close,vol",
            ts_code=ts_code,
            start_date="20200101",
            end_date="20201231",
            concurrent=True  # 启用并发模式
        )
        
        end_time = time.time()
        elapsed = end_time - start_time
        results.append((max_workers, len(df), elapsed))
        print(f"获取 {len(df)} 条数据耗时: {elapsed:.2f} 秒")
    
    # 显示性能对比
    print("\n并发性能对比:")
    print("-" * 50)
    print(f"{'工作线程数':^12} | {'数据条数':^12} | {'耗时(秒)':^12} | {'每秒数据量':^12}")
    print("-" * 50)
    for workers, count, time_spent in results:
        throughput = count / time_spent if time_spent > 0 else 0
        print(f"{workers:^12} | {count:^12} | {time_spent:^12.2f} | {throughput:^12.2f}")

def error_handling_example():
    """
    错误处理和重试机制示例
    
    展示如何处理API调用中的错误，以及自定义重试策略。
    """
    print("\n=== 错误处理和重试机制示例 ===")
    
    # 初始化客户端，设置自定义重试参数
    client = TushareAPI(
        token=TOKEN,
        max_retries=5,       # 最大重试次数
        retry_delay=2        # 重试间隔秒数
    )
    
    try:
        # 故意使用错误的参数
        df = client.get_data(
            api_name="daily",
            fields="ts_code,trade_date,open,high,low,close,vol",
            ts_code="INVALID_CODE",  # 使用无效的股票代码
            start_date="20200101",
            end_date="20201231"
        )
    except Exception as e:
        print(f"预期内的错误被捕获: {str(e)}")
        print("在实际应用中，您可以根据错误类型采取不同的恢复策略")

def large_data_processing():
    """
    大数据量处理策略示例
    
    展示如何高效处理大量数据，包括分批获取和流式处理。
    """
    print("\n=== 大数据量处理策略示例 ===")
    
    # 初始化客户端
    client = TushareAPI(token=TOKEN)
    
    print("获取全市场股票列表...")
    df_stocks = client.get_data(
        api_name="stock_basic",
        fields="ts_code,name,industry,area",
        list_status="L"
    )
    
    # 仅使用前10只股票作为示例
    sample_stocks = df_stocks['ts_code'].head(10).tolist()
    
    print(f"\n分批处理 {len(sample_stocks)} 只股票的数据:")
    all_data = []
    
    # 每批处理的股票数量
    batch_size = 3
    
    for i in range(0, len(sample_stocks), batch_size):
        batch = sample_stocks[i:i+batch_size]
        print(f"处理第 {i//batch_size + 1} 批: {', '.join(batch)}")
        
        ts_code = ",".join(batch)
        df_batch = client.get_data(
            api_name="daily",
            fields="ts_code,trade_date,open,high,low,close,vol",
            ts_code=ts_code,
            start_date="20210101",
            end_date="20210331",
            concurrent=True
        )
        
        print(f"  获取到 {len(df_batch)} 条数据")
        all_data.append(df_batch)
    
    # 合并所有批次的数据
    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        print(f"\n总共获取到 {len(df_all)} 条数据")
        print("数据示例:")
        print(df_all.head())
        
        # 按股票代码分组统计
        stats = df_all.groupby('ts_code').size().reset_index(name='count')
        print("\n每只股票的数据量:")
        print(stats)

def main():
    print("Tushare Plus 高级用法示例")
    print("=" * 50)
    
    # 运行各个示例
    disable_rate_limit_example()
    custom_concurrency_example()
    error_handling_example()
    large_data_processing()
    
    print("\n所有示例运行完成!")

if __name__ == "__main__":
    main()