#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步主控模块：整合异步数据获取、KDJ计算、股票筛选和邮件发送
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
from typing import Dict, List, Tuple, Optional, Any, Union

# 导入自定义模块
from async_data_fetcher import AsyncStockDataFetcher
from async_kdj_calculator import AsyncKDJCalculator
from async_stock_selector import AsyncStockSelector
from async_email_sender import AsyncEmailSender

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('a_stock_kdj.log')
    ]
)
logger = logging.getLogger('async_main')

class AsyncStockKDJApp:
    """A股KDJ指标J值筛选与邮件推送应用（异步优化版）"""
    
    def __init__(self, config_file=None):
        """
        初始化应用
        
        Args:
            config_file (str): 配置文件路径，默认为None
        """
        # 加载配置
        self.config = self._load_config(config_file)
        
        # 初始化组件
        self._init_components()
        
        # 性能指标
        self.performance_metrics = {
            'start_time': None,
            'end_time': None,
            'total_time': None,
            'component_metrics': {}
        }
    
    def _load_config(self, config_file):
        """
        加载配置文件
        
        Args:
            config_file (str): 配置文件路径
            
        Returns:
            dict: 配置字典
        """
        # 默认配置
        default_config = {
            "email": {
                "smtp_server": "smtp.example.com",
                "smtp_port": 465,
                "sender": "sender@example.com",
                "password": "password",
                "receiver": "receiver@example.com",
                "use_ssl": True
            },
            "data_fetcher": {
                "cache_dir": None,
                "request_delay": [1, 3],
                "retry_times": 3,
                "retry_delay": 5,
                "use_proxy": False,
                "proxy_list": [],
                "proxy_file": None,
                "max_workers": None,
                "use_multiprocessing": False
            },
            "stock": {
                "batch_size": 20,
                "use_cache": True,
                "top_n": 20,
                "periods": ["daily", "weekly", "monthly"],
                "kdj_params": {
                    "n": 9,
                    "m1": 3,
                    "m2": 3
                }
            }
        }
        
        # 如果提供了配置文件，则加载并合并
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                
                # 递归合并配置
                def merge_config(default, user):
                    for key, value in user.items():
                        if key in default and isinstance(default[key], dict) and isinstance(value, dict):
                            merge_config(default[key], value)
                        else:
                            default[key] = value
                
                merge_config(default_config, user_config)
                logger.info(f"已加载配置文件: {config_file}")
            except Exception as e:
                logger.error(f"加载配置文件失败: {str(e)}，将使用默认配置")
        else:
            logger.warning(f"配置文件不存在: {config_file}，将使用默认配置")
        
        return default_config
    
    def _init_components(self):
        """初始化各组件"""
        # 数据获取器
        data_fetcher_config = self.config.get('data_fetcher', {})
        self.data_fetcher = AsyncStockDataFetcher(
            cache_dir=data_fetcher_config.get('cache_dir'),
            request_delay=data_fetcher_config.get('request_delay', (1, 3)),
            use_proxy=data_fetcher_config.get('use_proxy', False),
            proxy_list=data_fetcher_config.get('proxy_list'),
            proxy_file=data_fetcher_config.get('proxy_file'),
            max_workers=data_fetcher_config.get('max_workers'),
            use_multiprocessing=data_fetcher_config.get('use_multiprocessing', False)
        )
        
        # KDJ计算器
        stock_config = self.config.get('stock', {})
        kdj_params = stock_config.get('kdj_params', {})
        self.kdj_calculator = AsyncKDJCalculator(
            n=kdj_params.get('n', 9),
            m1=kdj_params.get('m1', 3),
            m2=kdj_params.get('m2', 3),
            max_workers=data_fetcher_config.get('max_workers'),
            use_multiprocessing=data_fetcher_config.get('use_multiprocessing', False)
        )
        
        # 股票选择器（初始化时需要股票列表，将在运行时设置）
        self.stock_selector = None
        
        # 邮件发送器
        email_config = self.config.get('email', {})
        self.email_sender = AsyncEmailSender(email_config)
    
    async def run_async(self):
        """
        异步运行应用
        
        Returns:
            bool: 是否成功
        """
        self.performance_metrics['start_time'] = time.time()
        
        try:
            # 1. 获取股票列表
            logger.info("步骤1: 获取A股股票列表")
            stock_list = self.data_fetcher.get_stock_list(
                use_cache=self.config['stock'].get('use_cache', True)
            )
            
            if stock_list.empty:
                logger.error("获取股票列表失败")
                return False
            
            logger.info(f"成功获取 {len(stock_list)} 支股票的信息")
            
            # 初始化股票选择器
            self.stock_selector = AsyncStockSelector(
                stock_list,
                max_workers=self.config['data_fetcher'].get('max_workers')
            )
            
            # 2. 获取多个周期的股票数据
            logger.info("步骤2: 获取多个周期的股票数据")
            periods = self.config['stock'].get('periods', ['daily', 'weekly', 'monthly'])
            stock_codes = stock_list['code'].tolist()
            
            multi_period_data = await self.data_fetcher.get_multi_period_data_async(
                stock_codes,
                periods=periods,
                batch_size=self.config['stock'].get('batch_size', 20),
                use_cache=self.config['stock'].get('use_cache', True)
            )
            
            # 检查是否成功获取数据
            data_counts = {period: len(data) for period, data in multi_period_data.items()}
            logger.info(f"获取数据统计: {data_counts}")
            
            if all(count == 0 for count in data_counts.values()):
                logger.error("所有周期的数据获取均失败")
                return False
            
            # 3. 计算KDJ指标
            logger.info("步骤3: 计算KDJ指标")
            kdj_results = {}
            
            for period, period_data in multi_period_data.items():
                logger.info(f"计算 {period} 周期的KDJ指标")
                kdj_data = self.kdj_calculator.calculate_batch_kdj(period_data)
                kdj_results[period] = kdj_data
                logger.info(f"完成 {period} 周期的KDJ计算，共 {len(kdj_data)} 支股票")
            
            # 4. 筛选J值最低的股票
            logger.info("步骤4: 筛选J值最低的股票")
            top_n = self.config['stock'].get('top_n', 20)
            lowest_j_stocks = self.stock_selector.select_multi_dimension_lowest_j(kdj_results, top_n=top_n)
            
            # 检查筛选结果
            for period, df in lowest_j_stocks.items():
                if not df.empty:
                    logger.info(f"{period} 周期筛选出 {len(df)} 支J值最低的股票")
                    logger.info(f"J值范围: {df['j_value'].min():.2f} - {df['j_value'].max():.2f}")
                else:
                    logger.warning(f"{period} 周期没有筛选出股票")
            
            # 5. 发送邮件
            logger.info("步骤5: 发送邮件报告")
            email_config = self.config.get('email', {})
            receiver = email_config.get('receiver')
            
            if not receiver:
                logger.warning("未配置邮件接收者，跳过邮件发送")
            else:
                subject = f"A股KDJ指标J值最低股票筛选结果 - {datetime.now().strftime('%Y-%m-%d')}"
                success = await self.email_sender.send_email_async(receiver, subject, lowest_j_stocks)
                
                if not success:
                    logger.error("邮件发送失败")
                    return False
                
                logger.info(f"邮件已成功发送至 {receiver}")
            
            # 6. 收集性能指标
            self.performance_metrics['end_time'] = time.time()
            self.performance_metrics['total_time'] = self.performance_metrics['end_time'] - self.performance_metrics['start_time']
            
            self.performance_metrics['component_metrics'] = {
                'data_fetcher': self.data_fetcher.get_performance_metrics(),
                'kdj_calculator': self.kdj_calculator.get_performance_metrics(),
                'stock_selector': self.stock_selector.get_performance_metrics(),
                'email_sender': self.email_sender.get_performance_metrics()
            }
            
            logger.info(f"应用运行完成，总耗时: {self.performance_metrics['total_time']:.2f}秒")
            self._log_performance_metrics()
            
            return True
            
        except Exception as e:
            logger.error(f"应用运行出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def run(self):
        """
        运行应用（同步版本，用于兼容现有代码）
        
        Returns:
            bool: 是否成功
        """
        # 创建事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # 运行异步应用
            success = loop.run_until_complete(self.run_async())
            return success
        finally:
            # 关闭事件循环
            loop.close()
    
    def _log_performance_metrics(self):
        """记录性能指标"""
        logger.info("=== 性能指标 ===")
        logger.info(f"总运行时间: {self.performance_metrics['total_time']:.2f}秒")
        
        # 数据获取器指标
        df_metrics = self.performance_metrics['component_metrics'].get('data_fetcher', {})
        logger.info(f"数据获取: 总请求数 {df_metrics.get('total_requests', 0)}, "
                   f"缓存命中 {df_metrics.get('cache_hits', 0)}, "
                   f"平均请求时间 {df_metrics.get('avg_request_time', 0):.4f}秒")
        
        # KDJ计算器指标
        kdj_metrics = self.performance_metrics['component_metrics'].get('kdj_calculator', {})
        logger.info(f"KDJ计算: 总计算次数 {kdj_metrics.get('total_calculations', 0)}, "
                   f"平均计算时间 {kdj_metrics.get('avg_calculation_time', 0):.4f}秒")
        
        # 股票选择器指标
        ss_metrics = self.performance_metrics['component_metrics'].get('stock_selector', {})
        logger.info(f"股票筛选: 总筛选次数 {ss_metrics.get('total_selections', 0)}, "
                   f"平均筛选时间 {ss_metrics.get('avg_selection_time', 0):.4f}秒")
        
        # 邮件发送器指标
        es_metrics = self.performance_metrics['component_metrics'].get('email_sender', {})
        logger.info(f"邮件发送: 总邮件数 {es_metrics.get('total_emails', 0)}, "
                   f"平均发送时间 {es_metrics.get('avg_email_time', 0):.2f}秒")
        
        # 代理池指标
        if self.config['data_fetcher'].get('use_proxy', False):
            proxy_stats = self.data_fetcher.get_proxy_stats()
            if proxy_stats:
                logger.info(f"代理池: {len(proxy_stats)} 个代理")
                for proxy, stats in proxy_stats.items():
                    success_rate = stats.get('success_rate', 0)
                    logger.info(f"代理 {proxy}: 成功率 {success_rate:.2%}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股KDJ指标J值筛选与邮件推送应用')
    parser.add_argument('--config', type=str, default='config.json', help='配置文件路径')
    parser.add_argument('--use-proxy', action='store_true', help='启用代理池')
    args = parser.parse_args()
    
    # 加载配置
    config_file = args.config
    
    # 创建应用
    app = AsyncStockKDJApp(config_file)
    
    # 如果命令行指定了使用代理，则覆盖配置
    if args.use_proxy:
        app.config['data_fetcher']['use_proxy'] = True
    
    # 运行应用
    success = app.run()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
