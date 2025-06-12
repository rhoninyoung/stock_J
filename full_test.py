#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
整体功能测试脚本：验证代理池优化后的完整流程，包括邮件推送功能
"""

import logging
import os
import pandas as pd
from datetime import datetime
import time
import json
import argparse

# 导入自定义模块
from data_fetcher_with_proxy import StockDataFetcher
from kdj_calculator import KDJCalculator
from stock_selector import StockSelector
from email_sender import EmailSender
from main_with_proxy import StockKDJApp, Config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('full_test')

def test_email_template_generation():
    """测试邮件模板生成功能"""
    logger.info("=== 测试邮件模板生成功能 ===")
    
    # 创建测试数据
    test_data = {
        'daily': pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['平安银行', '万科A', '中国石化'],
            'j_value': [10.5, 15.2, 18.7],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['daily', 'daily', 'daily']
        }),
        'weekly': pd.DataFrame({
            'code': ['000001', '000004', '000005'],
            'name': ['平安银行', '国农科技', '世纪星源'],
            'j_value': [12.3, 16.8, 19.2],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['weekly', 'weekly', 'weekly']
        }),
        'monthly': pd.DataFrame({
            'code': ['000002', '000006', '000007'],
            'name': ['万科A', '深振业A', '全新好'],
            'j_value': [11.8, 17.5, 20.1],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['monthly', 'monthly', 'monthly']
        })
    }
    
    # 创建邮件发送器（使用测试配置）
    test_config = {
        'smtp_server': 'smtp.example.com',
        'smtp_port': 465,
        'sender': 'test@example.com',
        'password': 'test_password',
        'use_ssl': True
    }
    sender = EmailSender(test_config)
    
    # 生成邮件内容
    html_content = sender._generate_email_content(test_data)
    logger.info("成功生成HTML邮件内容")
    
    # 生成CSV附件
    attachments = sender._generate_csv_attachment(test_data)
    logger.info(f"成功生成CSV附件: {list(attachments.keys())}")
    
    return True

def test_config_loading():
    """测试配置加载功能"""
    logger.info("=== 测试配置加载功能 ===")
    
    # 创建测试配置文件
    test_config_file = "/tmp/test_config.json"
    test_config = {
        "email": {
            "smtp_server": "test.smtp.com",
            "receiver": "test@example.com"
        },
        "data_fetcher": {
            "use_proxy": True,
            "proxy_list": ["http://test.proxy:8080"]
        }
    }
    
    with open(test_config_file, 'w') as f:
        json.dump(test_config, f)
    
    # 加载配置
    config = Config(test_config_file)
    
    # 验证配置是否正确加载
    smtp_server = config.get('email', 'smtp_server')
    use_proxy = config.get('data_fetcher', 'use_proxy')
    proxy_list = config.get('data_fetcher', 'proxy_list')
    
    logger.info(f"加载配置: smtp_server={smtp_server}, use_proxy={use_proxy}, proxy_list={proxy_list}")
    
    # 验证配置覆盖是否正确
    if smtp_server == "test.smtp.com" and use_proxy == True and "http://test.proxy:8080" in proxy_list:
        logger.info("配置加载正确")
        return True
    else:
        logger.error("配置加载错误")
        return False

def test_main_app_initialization():
    """测试主应用初始化"""
    logger.info("=== 测试主应用初始化 ===")
    
    # 创建测试配置文件
    test_config_file = "/tmp/test_main_config.json"
    test_config = {
        "email": {
            "smtp_server": "test.smtp.com",
            "receiver": "test@example.com"
        },
        "data_fetcher": {
            "use_proxy": False,
            "cache_dir": "/tmp/stock_cache"
        },
        "stock": {
            "max_stocks": 10
        }
    }
    
    with open(test_config_file, 'w') as f:
        json.dump(test_config, f)
    
    # 初始化主应用
    app = StockKDJApp(test_config_file)
    
    # 验证各组件是否正确初始化
    if (app.fetcher is not None and 
        app.calculator is not None and 
        app.email_sender is not None):
        logger.info("主应用初始化成功")
        return True
    else:
        logger.error("主应用初始化失败")
        return False

def test_full_flow_with_mock_data():
    """使用模拟数据测试完整流程"""
    logger.info("=== 使用模拟数据测试完整流程 ===")
    
    # 创建测试配置文件
    test_config_file = "/tmp/test_full_flow_config.json"
    test_config = {
        "email": {
            "smtp_server": "test.smtp.com",
            "smtp_port": 465,
            "sender": "test@example.com",
            "password": "test_password",
            "use_ssl": True,
            "receiver": "test@example.com"
        },
        "stock": {
            "max_stocks": 5,
            "batch_size": 2
        },
        "selector": {
            "top_n": 3
        },
        "data_fetcher": {
            "use_proxy": False,
            "cache_dir": "/tmp/stock_cache",
            "request_delay": [0.1, 0.2]
        }
    }
    
    with open(test_config_file, 'w') as f:
        json.dump(test_config, f)
    
    # 创建主应用
    app = StockKDJApp(test_config_file)
    
    # 模拟股票列表
    mock_stock_list = pd.DataFrame({
        'code': ['000001', '000002', '000003', '000004', '000005'],
        'name': ['平安银行', '万科A', '中国石化', '国农科技', '世纪星源']
    })
    
    # 替换获取股票列表的方法
    original_get_stock_list = app.fetcher.get_stock_list
    app.fetcher.get_stock_list = lambda use_cache=True: mock_stock_list
    
    # 模拟获取股票数据的方法
    def mock_get_batch_stock_data(stock_codes, period='daily', start_date=None, end_date=None, 
                                max_stocks=None, batch_size=20, use_cache=True):
        result = {}
        for code in stock_codes:
            # 创建模拟数据
            dates = pd.date_range(end='2025-06-10', periods=30)
            data = pd.DataFrame({
                '日期': dates.strftime('%Y-%m-%d'),
                '开盘': [random.uniform(10, 20) for _ in range(30)],
                '收盘': [random.uniform(10, 20) for _ in range(30)],
                '最高': [random.uniform(10, 20) for _ in range(30)],
                '最低': [random.uniform(10, 20) for _ in range(30)],
                '成交量': [random.uniform(1000, 5000) for _ in range(30)]
            })
            result[code] = data
        return result
    
    # 替换获取股票数据的方法
    import random
    original_get_batch_stock_data = app.fetcher.get_batch_stock_data
    app.fetcher.get_batch_stock_data = mock_get_batch_stock_data
    
    # 模拟邮件发送方法
    def mock_send_email(receiver, subject, results_dict, include_csv=True):
        logger.info(f"模拟发送邮件到 {receiver}")
        logger.info(f"邮件主题: {subject}")
        logger.info(f"邮件内容包含 {len(results_dict)} 个维度的数据")
        return True
    
    # 替换邮件发送方法
    original_send_email = app.email_sender.send_email
    app.email_sender.send_email = mock_send_email
    
    try:
        # 运行应用
        success = app.run()
        
        if success:
            logger.info("完整流程测试成功")
            return True
        else:
            logger.error("完整流程测试失败")
            return False
    finally:
        # 恢复原始方法
        app.fetcher.get_stock_list = original_get_stock_list
        app.fetcher.get_batch_stock_data = original_get_batch_stock_data
        app.email_sender.send_email = original_send_email

def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行所有测试...")
    
    tests = [
        test_email_template_generation,
        test_config_loading,
        test_main_app_initialization,
        test_full_flow_with_mock_data
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
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='A股KDJ指标J值筛选系统整体功能测试')
    parser.add_argument('--config', type=str, help='配置文件路径')
    args = parser.parse_args()
    
    # 运行测试
    success = run_all_tests()
    
    if success:
        logger.info("所有测试通过，系统功能正常")
    else:
        logger.error("部分测试失败，请检查日志了解详情")
