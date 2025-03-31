#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare Plus 基本使用示例

本示例展示了 Tushare Plus 的基本用法，包括：
1. 初始化客户端
2. 获取股票基本信息
3. 获取大量日线数据（自动处理分页）
4. 使用并发模式获取数据
"""

import pandas as pd
from tushare_plus import TushareAPI

# 替换为您的 Tushare token
TOKEN = "your_token_here"

def main():
    # 初始化客户端
    print("初始化 Tushare Plus 客户端...")
    client = TushareAPI(token=TOKEN)
    
    # 获取股票基本信息
    print("\n获取股票基本信息...")
    df_basic = client.get_data(
        api_name="stock_basic",
        fields="ts_code,name,industry,area",
        list_status="L",
        limit=100  # 只获取前100条数据作为示例
    )
    print(f"获取到 {len(df_basic)} 条股票基本信息")
    print("示例数据:")
    print(df_basic.head())
    
    # 获取日线数据（自动处理分页）
    print("\n获取日线数据（自动处理分页）...")
    df_daily = client.get_data(
        api_name="daily",
        fields="ts_code,trade_date,open,high,low,close,vol",
        ts_code="000001.SZ",  # 只获取平安银行的数据作为示例
        start_date="20200101",
        end_date="20201231"
    )
    print(f"获取到 {len(df_daily)} 条日线数据")
    print("示例数据:")
    print(df_daily.head())
    
    # 使用并发模式获取多只股票的日线数据
    print("\n使用并发模式获取多只股票的日线数据...")
    # 获取上证50成分股的代码（实际使用时可以通过index_weight接口获取）
    sample_stocks = ["600000.SH", "600036.SH", "601318.SH", "600519.SH", "601166.SH"]
    
    # 构建查询条件
    ts_code = ",".join(sample_stocks)
    
    df_concurrent = client.get_data(
        api_name="daily",
        fields="ts_code,trade_date,open,high,low,close,vol",
        ts_code=ts_code,
        start_date="20200101",
        end_date="20201231",
        concurrent=True  # 启用并发模式
    )
    print(f"并发模式获取到 {len(df_concurrent)} 条日线数据")
    print("示例数据:")
    print(df_concurrent.head())
    
    # 保存数据到CSV文件
    print("\n保存数据到CSV文件...")
    df_basic.to_csv("stock_basic.csv", index=False, encoding="utf-8-sig")
    df_daily.to_csv("daily_000001.csv", index=False)
    df_concurrent.to_csv("daily_concurrent.csv", index=False)
    print("数据已保存到CSV文件")

if __name__ == "__main__":
    main()