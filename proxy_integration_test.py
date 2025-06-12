#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
集成测试脚本：验证代理池优化后的数据获取与J值筛选功能
"""

import logging
import os
import pandas as pd
from datetime import datetime
import time

# 导入自定义模块
from data_fetcher_with_proxy import StockDataFetcher
from kdj_calculator import KDJCalculator
from stock_selector import StockSelector
from proxy_pool import ProxyPool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('proxy_integration_test')

def test_proxy_mode():
    """测试代理模式下的数据获取与J值筛选流程"""
    logger.info("=== 测试代理模式下的数据获取与J值筛选流程 ===")
    
    # 创建测试用的代理列表（实际使用时需替换为真实代理）
    test_proxies = [
        "http://127.0.0.1:8080",  # 示例代理，实际使用时需替换
        "http://127.0.0.1:8081"
    ]
    
    # 创建代理模式下的数据获取器
    fetcher = StockDataFetcher(
        request_delay=(0.5, 1.0),
        use_proxy=True,
        proxy_list=test_proxies
    )
    
    calculator = KDJCalculator()
    
    try:
        # 获取股票列表（使用缓存加速测试）
        logger.info("获取A股股票列表...")
        stock_list = fetcher.get_stock_list(use_cache=True)
        if stock_list.empty:
            logger.error("获取股票列表失败")
            return False
        logger.info(f"成功获取股票列表，共{len(stock_list)}支股票")
        
        # 创建股票筛选器
        selector = StockSelector(stock_list)
        
        # 仅使用少量股票进行测试
        test_size = 5
        test_codes = stock_list['code'].tolist()[:test_size]
        logger.info(f"使用前{test_size}支股票进行测试")
        
        # 测试日维度数据获取与J值筛选
        logger.info("测试日维度数据获取与J值筛选...")
        
        # 获取历史数据（使用缓存和分批处理）
        daily_data = fetcher.get_batch_stock_data(
            test_codes, 
            period='daily', 
            batch_size=2,  # 每批2支股票
            use_cache=True
        )
        
        if not daily_data:
            logger.error("获取日维度数据失败")
            return False
            
        logger.info(f"成功获取{len(daily_data)}支股票的日维度数据")
        
        # 计算KDJ指标
        kdj_data = calculator.calculate_batch_kdj(daily_data)
        logger.info(f"成功计算{len(kdj_data)}支股票的KDJ指标")
        
        # 筛选J值最低的股票
        top_n = 3  # 测试时仅筛选前3支
        result = selector.select_lowest_j_stocks(kdj_data, time_dimension='daily', top_n=top_n)
        
        if result.empty:
            logger.error("日维度J值筛选失败")
            return False
            
        logger.info(f"成功筛选出{len(result)}支日维度J值最低的股票")
        logger.info(f"日维度筛选结果:\n{result}")
        
        # 查看代理统计信息
        proxy_stats = fetcher.get_proxy_stats()
        if proxy_stats:
            logger.info("代理统计信息:")
            for proxy, stat in proxy_stats.items():
                if proxy:  # 跳过None
                    logger.info(f"{proxy}: 成功 {stat['success']}，失败 {stat['failure']}，成功率 {stat.get('success_rate', 0):.2f}")
        
        return True
    except Exception as e:
        logger.error(f"代理模式测试失败: {str(e)}")
        return False

def test_non_proxy_mode():
    """测试非代理模式下的数据获取与J值筛选流程"""
    logger.info("=== 测试非代理模式下的数据获取与J值筛选流程 ===")
    
    # 创建非代理模式下的数据获取器
    fetcher = StockDataFetcher(
        request_delay=(0.5, 1.0),
        use_proxy=False
    )
    
    calculator = KDJCalculator()
    
    # 获取股票列表（使用缓存加速测试）
    logger.info("获取A股股票列表...")
    stock_list = fetcher.get_stock_list(use_cache=True)
    if stock_list.empty:
        logger.error("获取股票列表失败")
        return False
    logger.info(f"成功获取股票列表，共{len(stock_list)}支股票")
    
    # 创建股票筛选器
    selector = StockSelector(stock_list)
    
    # 仅使用少量股票进行测试
    test_size = 5
    test_codes = stock_list['code'].tolist()[:test_size]
    logger.info(f"使用前{test_size}支股票进行测试")
    
    # 测试日维度数据获取与J值筛选
    logger.info("测试日维度数据获取与J值筛选...")
    
    # 获取历史数据（使用缓存和分批处理）
    daily_data = fetcher.get_batch_stock_data(
        test_codes, 
        period='daily', 
        batch_size=2,  # 每批2支股票
        use_cache=True
    )
    
    if not daily_data:
        logger.error("获取日维度数据失败")
        return False
        
    logger.info(f"成功获取{len(daily_data)}支股票的日维度数据")
    
    # 计算KDJ指标
    kdj_data = calculator.calculate_batch_kdj(daily_data)
    logger.info(f"成功计算{len(kdj_data)}支股票的KDJ指标")
    
    # 筛选J值最低的股票
    top_n = 3  # 测试时仅筛选前3支
    result = selector.select_lowest_j_stocks(kdj_data, time_dimension='daily', top_n=top_n)
    
    if result.empty:
        logger.error("日维度J值筛选失败")
        return False
        
    logger.info(f"成功筛选出{len(result)}支日维度J值最低的股票")
    logger.info(f"日维度筛选结果:\n{result}")
    
    return True

def test_proxy_fallback():
    """测试代理失败后的自动切换功能"""
    logger.info("=== 测试代理失败后的自动切换功能 ===")
    
    # 创建一个包含无效代理的列表，测试自动切换到直连
    invalid_proxies = [
        "http://invalid.proxy:8080",
        "http://another.invalid:8081"
    ]
    
    # 创建代理池
    proxy_pool = ProxyPool(proxies=invalid_proxies)
    
    # 检查代理可用性
    available_proxies = proxy_pool.check_proxies()
    logger.info(f"可用代理数: {len(available_proxies)}")
    
    # 如果所有代理都不可用，应该会自动切换到直连（None）
    if None in available_proxies:
        logger.info("所有代理不可用，已自动切换到直连模式")
        return True
    else:
        logger.error("代理失败后未能自动切换到直连模式")
        return False

def test_config_integration():
    """测试配置文件集成"""
    logger.info("=== 测试配置文件集成 ===")
    
    # 创建测试配置文件
    config_file = "/tmp/test_proxy_config.json"
    config_content = """
    {
        "data_fetcher": {
            "use_proxy": true,
            "proxy_list": ["http://127.0.0.1:8080", "http://127.0.0.1:8081"],
            "proxy_file": null,
            "cache_dir": "/tmp/stock_cache",
            "request_delay": [0.5, 1.0]
        }
    }
    """
    
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    # 从配置文件加载
    import json
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # 验证配置是否正确加载
    data_fetcher_config = config.get('data_fetcher', {})
    use_proxy = data_fetcher_config.get('use_proxy', False)
    proxy_list = data_fetcher_config.get('proxy_list', [])
    
    logger.info(f"从配置文件加载: use_proxy={use_proxy}, proxy_list={proxy_list}")
    
    # 使用配置创建数据获取器
    fetcher = StockDataFetcher(
        cache_dir=data_fetcher_config.get('cache_dir'),
        request_delay=tuple(data_fetcher_config.get('request_delay', [1, 3])),
        use_proxy=use_proxy,
        proxy_list=proxy_list
    )
    
    # 验证代理池是否正确初始化
    if fetcher.use_proxy and fetcher.proxy_pool is not None:
        logger.info("代理池成功初始化")
        return True
    else:
        logger.error("代理池初始化失败")
        return False

def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行所有测试...")
    
    tests = [
        test_non_proxy_mode,
        test_proxy_fallback,
        test_config_integration
        # test_proxy_mode  # 注释掉实际代理测试，因为需要真实代理
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
    success = run_all_tests()
    
    if success:
        logger.info("所有测试通过，代理池优化后的数据获取与J值筛选功能正常")
    else:
        logger.error("部分测试失败，请检查日志了解详情")
