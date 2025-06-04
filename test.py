#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试脚本：验证A股KDJ指标J值筛选与邮件推送系统的功能
"""

import logging
import os
import pandas as pd
from datetime import datetime

# 导入自定义模块
from data_fetcher import StockDataFetcher
from kdj_calculator import KDJCalculator
from stock_selector import StockSelector
from email_sender import EmailSender

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test')

def test_data_fetcher():
    """测试数据获取模块"""
    logger.info("=== 测试数据获取模块 ===")
    
    fetcher = StockDataFetcher()
    
    # 测试获取股票列表
    logger.info("测试获取股票列表...")
    stock_list = fetcher.get_stock_list()
    if stock_list.empty:
        logger.error("获取股票列表失败")
        return False
    logger.info(f"成功获取股票列表，共{len(stock_list)}支股票")
    
    # 测试获取单只股票数据
    test_code = "000001"
    logger.info(f"测试获取股票 {test_code} 的日线数据...")
    daily_data = fetcher.get_stock_data(test_code, period='daily')
    if daily_data.empty:
        logger.error(f"获取股票 {test_code} 的日线数据失败")
        return False
    logger.info(f"成功获取股票 {test_code} 的日线数据，共{len(daily_data)}条记录")
    
    # 测试获取单只股票的周线和月线数据
    for period in ['weekly', 'monthly']:
        logger.info(f"测试获取股票 {test_code} 的{period}线数据...")
        period_data = fetcher.get_stock_data(test_code, period=period)
        if period_data.empty:
            logger.error(f"获取股票 {test_code} 的{period}线数据失败")
            return False
        logger.info(f"成功获取股票 {test_code} 的{period}线数据，共{len(period_data)}条记录")
    
    # 测试批量获取股票数据（仅获取前3支股票用于测试）
    test_codes = stock_list['code'].tolist()[:3]
    logger.info(f"测试批量获取股票数据，测试股票: {test_codes}...")
    batch_data = fetcher.get_batch_stock_data(test_codes, max_stocks=3)
    if not batch_data:
        logger.error("批量获取股票数据失败")
        return False
    logger.info(f"成功批量获取股票数据，共{len(batch_data)}支股票")
    
    logger.info("数据获取模块测试通过")
    return True

def test_kdj_calculator():
    """测试KDJ计算模块"""
    logger.info("=== 测试KDJ计算模块 ===")
    
    fetcher = StockDataFetcher()
    calculator = KDJCalculator()
    
    # 获取测试数据
    test_code = "000001"
    daily_data = fetcher.get_stock_data(test_code, period='daily')
    if daily_data.empty:
        logger.error("获取测试数据失败")
        return False
    
    # 测试KDJ计算
    logger.info(f"测试计算股票 {test_code} 的KDJ指标...")
    kdj_data = calculator.calculate_kdj(daily_data)
    if kdj_data.empty:
        logger.error("计算KDJ指标失败")
        return False
    
    # 检查KDJ指标是否包含K、D、J列
    for col in ['K', 'D', 'J']:
        if col not in kdj_data.columns:
            logger.error(f"KDJ指标中缺少{col}列")
            return False
    
    # 检查J值是否在合理范围内
    j_values = kdj_data['J'].dropna()
    if j_values.empty:
        logger.error("J值全为NaN")
        return False
    
    logger.info(f"J值范围: {j_values.min():.2f} - {j_values.max():.2f}")
    logger.info(f"最新J值: {j_values.iloc[-1]:.2f}")
    
    # 测试批量计算KDJ
    stock_list = fetcher.get_stock_list()
    test_codes = stock_list['code'].tolist()[:3]
    batch_data = fetcher.get_batch_stock_data(test_codes, max_stocks=3)
    
    logger.info("测试批量计算KDJ指标...")
    batch_kdj = calculator.calculate_batch_kdj(batch_data)
    if not batch_kdj:
        logger.error("批量计算KDJ指标失败")
        return False
    logger.info(f"成功批量计算KDJ指标，共{len(batch_kdj)}支股票")
    
    logger.info("KDJ计算模块测试通过")
    return True

def test_stock_selector():
    """测试股票筛选模块"""
    logger.info("=== 测试股票筛选模块 ===")
    
    fetcher = StockDataFetcher()
    calculator = KDJCalculator()
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    selector = StockSelector(stock_list)
    
    # 获取测试数据（仅获取前10支股票用于测试）
    test_codes = stock_list['code'].tolist()[:10]
    
    # 测试日维度筛选
    logger.info("测试日维度筛选...")
    daily_data = fetcher.get_batch_stock_data(test_codes, period='daily', max_stocks=10)
    daily_kdj = calculator.calculate_batch_kdj(daily_data)
    daily_result = selector.select_lowest_j_stocks(daily_kdj, time_dimension='daily', top_n=5)
    
    if daily_result.empty:
        logger.error("日维度筛选失败")
        return False
    logger.info(f"日维度筛选结果: {len(daily_result)}支股票")
    logger.info(daily_result)
    
    # 测试周维度筛选
    logger.info("测试周维度筛选...")
    weekly_data = fetcher.get_batch_stock_data(test_codes, period='weekly', max_stocks=10)
    weekly_kdj = calculator.calculate_batch_kdj(weekly_data)
    weekly_result = selector.select_lowest_j_stocks(weekly_kdj, time_dimension='weekly', top_n=5)
    
    if weekly_result.empty:
        logger.error("周维度筛选失败")
        return False
    logger.info(f"周维度筛选结果: {len(weekly_result)}支股票")
    logger.info(weekly_result)
    
    # 测试合并结果
    logger.info("测试合并结果...")
    combined_results = selector.combine_dimension_results(daily_result, weekly_result, pd.DataFrame())
    
    if not combined_results:
        logger.error("合并结果失败")
        return False
    
    for dimension, result in combined_results.items():
        if dimension in ['daily', 'weekly'] and result.empty:
            logger.error(f"{dimension}维度结果为空")
            return False
    
    logger.info("股票筛选模块测试通过")
    return True

def test_email_content_generation():
    """测试邮件内容生成"""
    logger.info("=== 测试邮件内容生成 ===")
    
    # 创建测试数据
    test_data = {
        'daily': pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['平安银行', '万科A', '中国石化'],
            'j_value': [10.5, 15.2, 18.7],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
        }),
        'weekly': pd.DataFrame({
            'code': ['000001', '000004', '000005'],
            'name': ['平安银行', '国农科技', '世纪星源'],
            'j_value': [12.3, 16.8, 19.2],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
        }),
        'monthly': pd.DataFrame({
            'code': ['000002', '000006', '000007'],
            'name': ['万科A', '深振业A', '全新好'],
            'j_value': [11.8, 17.5, 20.1],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
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
    logger.info("测试生成邮件内容...")
    html_content = sender._generate_email_content(test_data)
    if not html_content:
        logger.error("生成邮件内容失败")
        return False
    logger.info("成功生成邮件内容")
    
    # 生成CSV附件
    logger.info("测试生成CSV附件...")
    attachments = sender._generate_csv_attachment(test_data)
    if not attachments:
        logger.error("生成CSV附件失败")
        return False
    logger.info(f"成功生成CSV附件，共{len(attachments)}个附件")
    
    logger.info("邮件内容生成测试通过")
    return True

def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行所有测试...")
    
    tests = [
        test_data_fetcher,
        test_kdj_calculator,
        test_stock_selector,
        test_email_content_generation
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            logger.error(f"测试 {test.__name__} 出错: {str(e)}")
            results.append(False)
    
    success_count = sum(results)
    total_count = len(results)
    
    logger.info(f"测试完成: {success_count}/{total_count} 通过")
    
    return all(results)

if __name__ == "__main__":
    success = run_all_tests()
    
    if success:
        logger.info("所有测试通过")
    else:
        logger.error("部分测试失败")
