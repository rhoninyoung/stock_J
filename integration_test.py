#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试脚本：验证优化后的数据获取与J值筛选功能
"""

import logging
import os
import pandas as pd
from datetime import datetime

# 导入自定义模块
from data_fetcher import StockDataFetcher
from kdj_calculator import KDJCalculator
from stock_selector import StockSelector

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('integration_test')

def test_optimized_flow():
    """测试优化后的数据获取与J值筛选流程"""
    logger.info("=== 测试优化后的数据获取与J值筛选流程 ===")
    
    # 创建优化后的数据获取器（使用较短的请求延迟以加快测试）
    fetcher = StockDataFetcher(request_delay=(0.5, 1.0))
    calculator = KDJCalculator()
    
    # 获取股票列表（使用缓存加速）
    logger.info("获取A股股票列表...")
    stock_list = fetcher.get_stock_list(use_cache=True)
    if stock_list.empty:
        logger.error("获取股票列表失败")
        return False
    logger.info(f"成功获取股票列表，共{len(stock_list)}支股票")
    
    # 创建股票筛选器
    selector = StockSelector(stock_list)
    
    # 仅使用少量股票进行测试
    test_size = 10
    test_codes = stock_list['code'].tolist()[:test_size]
    logger.info(f"使用前{test_size}支股票进行测试")
    
    # 测试多周期数据获取与J值筛选
    periods = ['daily', 'weekly', 'monthly']
    results = {}
    
    for period in periods:
        logger.info(f"测试{period}周期数据获取与J值筛选...")
        
        # 获取历史数据（使用缓存和分批处理）
        period_data = fetcher.get_batch_stock_data(
            test_codes, 
            period=period, 
            batch_size=5,  # 每批5支股票
            use_cache=True
        )
        
        if not period_data:
            logger.error(f"获取{period}周期数据失败")
            continue
            
        logger.info(f"成功获取{len(period_data)}支股票的{period}周期数据")
        
        # 计算KDJ指标
        kdj_data = calculator.calculate_batch_kdj(period_data)
        logger.info(f"成功计算{len(kdj_data)}支股票的KDJ指标")
        
        # 筛选J值最低的股票
        top_n = 5  # 测试时仅筛选前5支
        result = selector.select_lowest_j_stocks(kdj_data, time_dimension=period, top_n=top_n)
        
        if result.empty:
            logger.error(f"{period}周期J值筛选失败")
            continue
            
        logger.info(f"成功筛选出{len(result)}支{period}周期J值最低的股票")
        logger.info(f"{period}周期筛选结果:\n{result}")
        
        results[period] = result
    
    # 合并三个时间维度的结果
    if len(results) > 0:
        combined_results = selector.combine_dimension_results(
            results.get('daily', pd.DataFrame()),
            results.get('weekly', pd.DataFrame()),
            results.get('monthly', pd.DataFrame())
        )
        
        logger.info("合并后的结果包含以下维度:")
        for dimension, result in combined_results.items():
            if not result.empty:
                logger.info(f"- {dimension}: {len(result)}支股票")
        
        return True
    else:
        logger.error("所有周期的数据获取或筛选均失败")
        return False

def test_cache_mechanism():
    """测试缓存机制"""
    logger.info("=== 测试缓存机制 ===")
    
    # 创建数据获取器
    fetcher = StockDataFetcher(request_delay=(0.5, 1.0))
    
    # 清理过期缓存
    fetcher._clean_expired_cache()
    
    # 首次获取数据（不使用缓存）
    logger.info("首次获取数据（不使用缓存）...")
    start_time = datetime.now()
    test_code = "000001"
    data1 = fetcher.get_stock_data(test_code, period='daily', use_cache=False)
    end_time = datetime.now()
    time_cost1 = (end_time - start_time).total_seconds()
    logger.info(f"首次获取耗时: {time_cost1:.2f}秒")
    
    # 再次获取数据（使用缓存）
    logger.info("再次获取数据（使用缓存）...")
    start_time = datetime.now()
    data2 = fetcher.get_stock_data(test_code, period='daily', use_cache=True)
    end_time = datetime.now()
    time_cost2 = (end_time - start_time).total_seconds()
    logger.info(f"使用缓存获取耗时: {time_cost2:.2f}秒")
    
    # 验证数据一致性
    if data1.equals(data2):
        logger.info("缓存数据与原始数据一致")
    else:
        logger.error("缓存数据与原始数据不一致")
        return False
    
    # 验证缓存加速效果
    if time_cost2 < time_cost1:
        logger.info(f"缓存加速效果明显，提速{time_cost1/time_cost2:.2f}倍")
        return True
    else:
        logger.warning("缓存未能有效加速")
        return False

def test_batch_processing():
    """测试分批处理功能"""
    logger.info("=== 测试分批处理功能 ===")
    
    # 创建数据获取器
    fetcher = StockDataFetcher(request_delay=(0.5, 1.0))
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list(use_cache=True)
    
    # 测试不同批次大小
    test_size = 10
    test_codes = stock_list['code'].tolist()[:test_size]
    
    batch_sizes = [2, 5, 10]
    for batch_size in batch_sizes:
        logger.info(f"测试批次大小: {batch_size}")
        
        start_time = datetime.now()
        data = fetcher.get_batch_stock_data(
            test_codes, 
            period='daily', 
            batch_size=batch_size,
            use_cache=True
        )
        end_time = datetime.now()
        
        time_cost = (end_time - start_time).total_seconds()
        logger.info(f"批次大小{batch_size}，处理{len(data)}/{test_size}支股票，耗时: {time_cost:.2f}秒")
    
    return True

def test_retry_mechanism():
    """测试重试机制"""
    logger.info("=== 测试重试机制 ===")
    
    # 创建一个模拟失败的函数
    from functools import wraps
    
    failure_count = [0]  # 使用列表以便在闭包中修改
    
    def simulate_failure(max_failures=2):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if failure_count[0] < max_failures:
                    failure_count[0] += 1
                    raise Exception(f"模拟失败 {failure_count[0]}/{max_failures}")
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    # 创建测试类
    class RetryTester:
        @retry_decorator(max_retries=3, delay_base=1)
        @simulate_failure(max_failures=2)
        def test_function(self):
            return "成功"
    
    # 测试重试机制
    tester = RetryTester()
    try:
        result = tester.test_function()
        logger.info(f"重试后结果: {result}")
        logger.info("重试机制测试通过")
        return True
    except Exception as e:
        logger.error(f"重试机制测试失败: {str(e)}")
        return False

def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行所有测试...")
    
    tests = [
        test_cache_mechanism,
        test_batch_processing,
        test_retry_mechanism,
        test_optimized_flow
    ]
    
    results = []
    for test in tests:
        try:
            logger.info(f"\n开始测试: {test.__name__}")
            result = test()
            results.append(result)
            logger.info(f"测试 {test.__name__} {'通过' if result else '失败'}")
        except Exception as e:
            logger.error(f"测试 {test.__name__} 出错: {str(e)}")
            results.append(False)
    
    success_count = sum(1 for r in results if r)
    total_count = len(results)
    
    logger.info(f"\n测试完成: {success_count}/{total_count} 通过")
    
    return all(results)

if __name__ == "__main__":
    from data_fetcher import retry_decorator  # 导入重试装饰器用于测试
    
    success = run_all_tests()
    
    if success:
        logger.info("所有测试通过，优化后的数据获取与J值筛选功能正常")
    else:
        logger.error("部分测试失败，请检查日志了解详情")
