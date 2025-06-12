#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
集成测试模块：测试异步优化版本的完整流程
"""

import os
import sys
import json
import time
import logging
import asyncio
import argparse
from datetime import datetime
import pandas as pd
import numpy as np

# 导入自定义模块
from async_data_fetcher import AsyncStockDataFetcher
from async_kdj_calculator import AsyncKDJCalculator
from async_stock_selector import AsyncStockSelector
from async_email_sender import AsyncEmailSender
from async_main import AsyncStockKDJApp

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('integration_test')

async def test_data_fetcher():
    """测试异步数据获取器"""
    logger.info("=== 测试异步数据获取器 ===")
    
    # 创建异步数据获取器（不使用代理）
    fetcher1 = AsyncStockDataFetcher(
        request_delay=(0.5, 1.0),
        use_proxy=False,
        max_workers=8
    )
    
    # 获取股票列表
    stock_list = fetcher1.get_stock_list()
    logger.info(f"获取到 {len(stock_list)} 支股票")
    
    # 测试获取单只股票数据
    test_code = "000001"
    daily_data = await fetcher1.get_stock_data_async(test_code, period='daily')
    logger.info(f"股票 {test_code} 日线数据: {len(daily_data)} 条记录")
    
    # 测试批量获取（仅获取前5支股票用于测试）
    test_codes = stock_list['code'].tolist()[:5]
    batch_data = await fetcher1.get_batch_stock_data_async(
        test_codes,
        max_stocks=5,
        batch_size=2,
        concurrency_limit=3
    )
    logger.info(f"批量获取结果，成功获取 {len(batch_data)} 支股票的数据")
    
    # 测试多周期数据获取
    multi_period_data = await fetcher1.get_multi_period_data_async(
        test_codes[:3],
        max_stocks=3,
        batch_size=2,
        concurrency_limit=3
    )
    logger.info("多周期数据获取结果:")
    for period, period_data in multi_period_data.items():
        logger.info(f"{period} 周期: 获取 {len(period_data)} 支股票的数据")
    
    # 输出性能指标
    metrics = fetcher1.get_performance_metrics()
    logger.info(f"性能指标: 总请求数 {metrics['total_requests']}, 缓存命中 {metrics['cache_hits']}, 平均请求时间 {metrics.get('avg_request_time', 0):.2f}秒")
    
    # 创建异步数据获取器（使用代理）
    fetcher2 = AsyncStockDataFetcher(
        request_delay=(0.5, 1.0),
        use_proxy=True,
        proxy_list=["http://127.0.0.1:8080"],  # 测试代理，实际使用时替换为真实代理
        max_workers=8
    )
    
    # 测试代理池
    logger.info("\n=== 测试代理池 ===")
    proxy_stats = fetcher2.get_proxy_stats()
    if proxy_stats:
        logger.info(f"代理池大小: {len(proxy_stats)}")
    else:
        logger.info("代理池未启用或为空")
    
    return True

async def test_kdj_calculator(stock_data_dict):
    """测试异步KDJ计算器"""
    logger.info("\n=== 测试异步KDJ计算器 ===")
    
    # 创建KDJ计算器（多线程版）
    calculator1 = AsyncKDJCalculator(use_multiprocessing=False)
    
    # 测试单只股票KDJ计算
    test_code = next(iter(stock_data_dict))
    test_data = stock_data_dict[test_code]
    kdj_data = calculator1.calculate_kdj(test_data)
    logger.info(f"股票 {test_code} KDJ计算结果: {len(kdj_data)} 条记录")
    
    # 测试批量KDJ计算（多线程）
    start_time = time.time()
    kdj_results1 = calculator1.calculate_batch_kdj(stock_data_dict)
    threaded_time = time.time() - start_time
    logger.info(f"多线程KDJ计算完成，处理 {len(kdj_results1)}/{len(stock_data_dict)} 支股票，耗时: {threaded_time:.2f}秒")
    
    # 创建KDJ计算器（多进程版）
    calculator2 = AsyncKDJCalculator(use_multiprocessing=True)
    
    # 测试批量KDJ计算（多进程）
    start_time = time.time()
    kdj_results2 = calculator2.calculate_batch_kdj(stock_data_dict)
    multiprocess_time = time.time() - start_time
    logger.info(f"多进程KDJ计算完成，处理 {len(kdj_results2)}/{len(stock_data_dict)} 支股票，耗时: {multiprocess_time:.2f}秒")
    
    # 比较结果
    logger.info(f"性能比较: 多线程 {threaded_time:.2f}秒 vs 多进程 {multiprocess_time:.2f}秒")
    
    return kdj_results1

async def test_stock_selector(stock_list, kdj_results):
    """测试异步股票选择器"""
    logger.info("\n=== 测试异步股票选择器 ===")
    
    # 创建股票选择器
    selector = AsyncStockSelector(stock_list)
    
    # 测试单一维度筛选
    start_time = time.time()
    daily_result = selector.select_lowest_j_stocks(kdj_results, 'daily', top_n=20)
    logger.info(f"单一维度筛选耗时: {time.time() - start_time:.4f}秒")
    logger.info(f"筛选结果前5条:\n{daily_result.head().to_string()}")
    
    # 测试多维度筛选
    multi_results = {
        'daily': kdj_results,
        'weekly': kdj_results,  # 使用相同数据模拟不同周期
        'monthly': kdj_results  # 使用相同数据模拟不同周期
    }
    
    start_time = time.time()
    results = selector.select_multi_dimension_lowest_j(multi_results, top_n=20)
    logger.info(f"多维度筛选耗时: {time.time() - start_time:.4f}秒")
    
    # 输出性能指标
    metrics = selector.get_performance_metrics()
    logger.info(f"性能指标: 总筛选次数 {metrics['total_selections']}, 平均筛选时间 {metrics.get('avg_selection_time', 0):.4f}秒")
    
    return results

async def test_email_sender(results_dict):
    """测试异步邮件发送器"""
    logger.info("\n=== 测试异步邮件发送器 ===")
    
    # 创建测试配置
    test_config = {
        'smtp_server': 'smtp.example.com',
        'smtp_port': 465,
        'sender': 'test@example.com',
        'password': 'test_password',
        'use_ssl': True
    }
    
    # 创建邮件发送器
    sender = AsyncEmailSender(test_config)
    
    # 测试生成邮件内容
    html_content = await sender._generate_email_content_async(results_dict)
    logger.info(f"生成的HTML内容长度: {len(html_content)} 字符")
    
    # 测试生成CSV附件
    attachments = await sender._generate_csv_attachment_async(results_dict)
    logger.info(f"生成的附件数量: {len(attachments)}")
    
    # 注意：不实际发送邮件，避免测试时发送真实邮件
    logger.info("邮件发送测试已跳过")
    
    return True

async def test_full_workflow():
    """测试完整工作流程"""
    logger.info("\n=== 测试完整工作流程 ===")
    
    # 创建测试配置
    test_config = {
        "email": {
            "smtp_server": "smtp.example.com",
            "smtp_port": 465,
            "sender": "test@example.com",
            "password": "test_password",
            "receiver": None,  # 设为None避免实际发送邮件
            "use_ssl": True
        },
        "data_fetcher": {
            "cache_dir": "./test_cache",
            "request_delay": [0.5, 1.0],
            "retry_times": 2,
            "retry_delay": 2,
            "use_proxy": False,
            "max_workers": 8,
            "use_multiprocessing": True
        },
        "stock": {
            "batch_size": 5,
            "use_cache": True,
            "top_n": 10,
            "periods": ["daily"],  # 仅测试日线数据
            "kdj_params": {
                "n": 9,
                "m1": 3,
                "m2": 3
            }
        }
    }
    
    # 创建临时配置文件
    with open('test_config.json', 'w', encoding='utf-8') as f:
        json.dump(test_config, f, ensure_ascii=False, indent=2)
    
    # 创建应用
    app = AsyncStockKDJApp('test_config.json')
    
    # 运行应用
    start_time = time.time()
    success = await app.run_async()
    total_time = time.time() - start_time
    
    logger.info(f"完整工作流程测试{'成功' if success else '失败'}，总耗时: {total_time:.2f}秒")
    
    # 清理临时文件
    if os.path.exists('test_config.json'):
        os.remove('test_config.json')
    
    return success

async def test_proxy_integration():
    """测试代理池集成"""
    logger.info("\n=== 测试代理池集成 ===")
    
    # 创建测试配置（启用代理池）
    test_config = {
        "data_fetcher": {
            "cache_dir": "./test_cache",
            "request_delay": [0.5, 1.0],
            "retry_times": 2,
            "retry_delay": 2,
            "use_proxy": True,
            "proxy_list": ["http://127.0.0.1:8080"],  # 测试代理，实际使用时替换为真实代理
            "max_workers": 8
        },
        "stock": {
            "batch_size": 3,
            "use_cache": True
        }
    }
    
    # 创建数据获取器
    fetcher = AsyncStockDataFetcher(
        cache_dir=test_config["data_fetcher"]["cache_dir"],
        request_delay=test_config["data_fetcher"]["request_delay"],
        use_proxy=test_config["data_fetcher"]["use_proxy"],
        proxy_list=test_config["data_fetcher"]["proxy_list"],
        max_workers=test_config["data_fetcher"]["max_workers"]
    )
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    logger.info(f"获取到 {len(stock_list)} 支股票")
    
    # 测试批量获取（仅获取前3支股票用于测试）
    test_codes = stock_list['code'].tolist()[:3]
    
    # 测试代理池状态
    proxy_stats_before = fetcher.get_proxy_stats()
    logger.info(f"代理池初始状态: {proxy_stats_before}")
    
    # 使用代理获取数据
    batch_data = await fetcher.get_batch_stock_data_async(
        test_codes,
        max_stocks=3,
        batch_size=1,
        concurrency_limit=2
    )
    logger.info(f"使用代理批量获取结果，成功获取 {len(batch_data)} 支股票的数据")
    
    # 测试代理池状态变化
    proxy_stats_after = fetcher.get_proxy_stats()
    logger.info(f"代理池最终状态: {proxy_stats_after}")
    
    return True

async def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行集成测试...")
    start_time = time.time()
    
    try:
        # 测试数据获取器
        await test_data_fetcher()
        
        # 获取一些测试数据用于后续测试
        fetcher = AsyncStockDataFetcher(request_delay=(0.1, 0.2), use_proxy=False)
        stock_list = fetcher.get_stock_list()
        test_codes = stock_list['code'].tolist()[:10]
        stock_data_dict = await fetcher.get_batch_stock_data_async(test_codes, max_stocks=10, batch_size=5)
        
        # 测试KDJ计算器
        kdj_results = await test_kdj_calculator(stock_data_dict)
        
        # 测试股票选择器
        selection_results = await test_stock_selector(stock_list, kdj_results)
        
        # 测试邮件发送器
        await test_email_sender(selection_results)
        
        # 测试代理池集成
        await test_proxy_integration()
        
        # 测试完整工作流程
        await test_full_workflow()
        
        total_time = time.time() - start_time
        logger.info(f"所有测试完成，总耗时: {total_time:.2f}秒")
        return True
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """主函数"""
    # 运行所有测试
    loop = asyncio.get_event_loop()
    success = loop.run_until_complete(run_all_tests())
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
