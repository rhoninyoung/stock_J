#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主控模块：协调各模块工作，实现A股KDJ指标J值筛选与邮件推送
优化版：整合了数据获取优化、缓存机制、分批处理和代理池支持
"""

import os
import logging
import pandas as pd
import json
from datetime import datetime, timedelta
import argparse

# 导入自定义模块
from data_fetcher_with_proxy import StockDataFetcher
from kdj_calculator import KDJCalculator
from stock_selector import StockSelector
from email_sender import EmailSender

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('main')

class Config:
    """配置类，用于管理系统配置"""
    
    def __init__(self, config_file=None):
        """
        初始化配置
        
        Args:
            config_file (str): 配置文件路径
        """
        # 默认配置
        self.default_config = {
            'email': {
                'smtp_server': 'smtp.163.com',
                'smtp_port': 465,
                'sender': 'your_email@163.com',
                'password': 'your_password',
                'use_ssl': True,
                'receiver': 'receiver@example.com'
            },
            'stock': {
                'max_stocks': None,  # 获取全部股票
                'exclude_st': True,  # 排除ST股票
                'exclude_new': True,  # 排除上市不足一年的新股
                'batch_size': 20,    # 每批处理的股票数量
                'use_cache': True    # 是否使用缓存
            },
            'kdj': {
                'n': 9,
                'm1': 3,
                'm2': 3
            },
            'selector': {
                'top_n': 20  # 筛选数量
            },
            'data_fetcher': {
                'cache_dir': None,  # 缓存目录，默认为当前目录下的cache子目录
                'request_delay': [1, 3],  # 请求延迟范围（最小值，最大值），单位为秒
                'retry_times': 3,   # 最大重试次数
                'retry_delay': 5,   # 重试基础延迟时间
                'use_proxy': False,  # 是否使用代理池
                'proxy_list': [],    # 代理列表
                'proxy_file': None   # 代理文件路径
            }
        }
        
        # 加载配置
        self.config = self._load_config(config_file)
    
    def _load_config(self, config_file):
        """
        加载配置
        
        Args:
            config_file (str): 配置文件路径
            
        Returns:
            dict: 配置
        """
        config = self.default_config.copy()
        
        # 尝试从配置文件加载
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                    # 递归更新配置
                    self._update_dict(config, file_config)
            except Exception as e:
                logger.error(f"加载配置文件失败: {str(e)}")
        
        # 尝试从环境变量加载邮件配置
        if os.environ.get('EMAIL_SMTP_SERVER'):
            config['email']['smtp_server'] = os.environ.get('EMAIL_SMTP_SERVER')
        if os.environ.get('EMAIL_SMTP_PORT'):
            config['email']['smtp_port'] = int(os.environ.get('EMAIL_SMTP_PORT'))
        if os.environ.get('EMAIL_SENDER'):
            config['email']['sender'] = os.environ.get('EMAIL_SENDER')
        if os.environ.get('EMAIL_PASSWORD'):
            config['email']['password'] = os.environ.get('EMAIL_PASSWORD')
        if os.environ.get('EMAIL_USE_SSL'):
            config['email']['use_ssl'] = os.environ.get('EMAIL_USE_SSL').lower() == 'true'
        if os.environ.get('EMAIL_RECEIVER'):
            config['email']['receiver'] = os.environ.get('EMAIL_RECEIVER')
        
        # 尝试从环境变量加载代理配置
        if os.environ.get('USE_PROXY'):
            config['data_fetcher']['use_proxy'] = os.environ.get('USE_PROXY').lower() == 'true'
        if os.environ.get('PROXY_FILE'):
            config['data_fetcher']['proxy_file'] = os.environ.get('PROXY_FILE')
        if os.environ.get('CACHE_DIR'):
            config['data_fetcher']['cache_dir'] = os.environ.get('CACHE_DIR')
        
        return config
    
    def _update_dict(self, d, u):
        """
        递归更新字典
        
        Args:
            d (dict): 目标字典
            u (dict): 更新字典
        """
        for k, v in u.items():
            if isinstance(v, dict):
                d[k] = self._update_dict(d.get(k, {}), v)
            else:
                d[k] = v
        return d
    
    def get(self, section, key=None):
        """
        获取配置
        
        Args:
            section (str): 配置节
            key (str): 配置键
            
        Returns:
            任意类型: 配置值
        """
        if key:
            return self.config.get(section, {}).get(key)
        return self.config.get(section, {})


class StockKDJApp:
    """A股KDJ指标J值筛选与邮件推送应用（优化版，支持代理池）"""
    
    def __init__(self, config_file=None):
        """
        初始化应用
        
        Args:
            config_file (str): 配置文件路径
        """
        # 加载配置
        self.config = Config(config_file)
        
        # 创建数据获取器（使用优化配置）
        data_fetcher_config = self.config.get('data_fetcher')
        self.fetcher = StockDataFetcher(
            cache_dir=data_fetcher_config.get('cache_dir'),
            request_delay=tuple(data_fetcher_config.get('request_delay', [1, 3])),
            use_proxy=data_fetcher_config.get('use_proxy', False),
            proxy_list=data_fetcher_config.get('proxy_list', []),
            proxy_file=data_fetcher_config.get('proxy_file')
        )
        
        # 创建KDJ计算器
        kdj_config = self.config.get('kdj')
        self.calculator = KDJCalculator(
            n=kdj_config.get('n', 9),
            m1=kdj_config.get('m1', 3),
            m2=kdj_config.get('m2', 3)
        )
        
        # 股票筛选器在获取股票列表后创建
        self.selector = None
        
        # 创建邮件发送器
        email_config = self.config.get('email')
        self.email_sender = EmailSender(email_config)
    
    def _filter_stock_list(self, stock_list):
        """
        过滤股票列表
        
        Args:
            stock_list (pandas.DataFrame): 股票列表
            
        Returns:
            pandas.DataFrame: 过滤后的股票列表
        """
        stock_config = self.config.get('stock')
        filtered_list = stock_list.copy()
        
        # 排除ST股票
        if stock_config.get('exclude_st', True):
            filtered_list = filtered_list[~filtered_list['name'].str.contains('ST')]
        
        # 限制股票数量（用于测试）
        max_stocks = stock_config.get('max_stocks')
        if max_stocks:
            filtered_list = filtered_list.head(max_stocks)
        
        return filtered_list
    
    def run(self):
        """
        运行应用
        
        Returns:
            bool: 是否成功
        """
        try:
            logger.info("开始运行A股KDJ指标J值筛选与邮件推送应用...")
            
            # 获取股票列表（使用缓存）
            stock_config = self.config.get('stock')
            use_cache = stock_config.get('use_cache', True)
            
            stock_list = self.fetcher.get_stock_list(use_cache=use_cache)
            if stock_list.empty:
                logger.error("获取股票列表失败")
                return False
            
            # 过滤股票列表
            filtered_stock_list = self._filter_stock_list(stock_list)
            logger.info(f"过滤后的股票数量: {len(filtered_stock_list)}")
            
            # 创建股票筛选器
            self.selector = StockSelector(filtered_stock_list)
            
            # 获取股票代码列表
            stock_codes = filtered_stock_list['code'].tolist()
            
            # 获取各时间维度的数据并计算KDJ（使用优化的分批处理）
            dimensions = ['daily', 'weekly', 'monthly']
            kdj_results = {}
            
            batch_size = stock_config.get('batch_size', 20)
            
            for dimension in dimensions:
                # 获取历史数据（使用分批处理和缓存）
                logger.info(f"获取{dimension}维度的历史数据...")
                stock_data = self.fetcher.get_batch_stock_data(
                    stock_codes, 
                    period=dimension,
                    batch_size=batch_size,
                    use_cache=use_cache
                )
                
                # 计算KDJ指标
                logger.info(f"计算{dimension}维度的KDJ指标...")
                kdj_data = self.calculator.calculate_batch_kdj(stock_data)
                
                # 保存结果
                kdj_results[dimension] = kdj_data
            
            # 筛选各时间维度J值最低的股票
            selection_results = {}
            top_n = self.config.get('selector', {}).get('top_n', 20)
            
            for dimension, kdj_data in kdj_results.items():
                logger.info(f"筛选{dimension}维度J值最低的{top_n}支股票...")
                selection = self.selector.select_lowest_j_stocks(kdj_data, dimension, top_n)
                selection_results[dimension] = selection
            
            # 发送邮件
            logger.info("准备发送邮件...")
            receiver = self.config.get('email', {}).get('receiver')
            if not receiver:
                logger.error("未配置邮件接收者")
                return False
            
            subject = f"A股KDJ指标J值最低股票筛选结果 - {datetime.now().strftime('%Y-%m-%d')}"
            success = self.email_sender.send_email(receiver, subject, selection_results)
            
            if success:
                logger.info("邮件发送成功")
                return True
            else:
                logger.error("邮件发送失败")
                return False
            
        except Exception as e:
            logger.error(f"运行应用时出错: {str(e)}")
            return False
    
    def get_proxy_stats(self):
        """
        获取代理统计信息
        
        Returns:
            dict: 代理统计信息，如果未启用代理池则返回None
        """
        if hasattr(self.fetcher, 'get_proxy_stats'):
            return self.fetcher.get_proxy_stats()
        return None


# 云函数入口
def main(req=None):
    """
    云函数入口
    
    Args:
        req: 云函数请求对象
        
    Returns:
        dict: 响应
    """
    # 获取配置文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'config.json')
    
    # 创建并运行应用
    app = StockKDJApp(config_file)
    success = app.run()
    
    # 返回结果
    if success:
        return {"status": "success", "message": "A股KDJ指标J值筛选与邮件推送成功"}
    else:
        return {"status": "error", "message": "A股KDJ指标J值筛选与邮件推送失败"}


# 命令行入口
if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='A股KDJ指标J值筛选与邮件推送')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--use-proxy', action='store_true', help='启用代理池')
    parser.add_argument('--proxy-file', type=str, help='代理文件路径')
    args = parser.parse_args()
    
    # 获取配置文件路径
    config_file = args.config
    if not config_file:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, 'config.json')
    
    # 如果命令行指定了代理选项，设置环境变量
    if args.use_proxy:
        os.environ['USE_PROXY'] = 'true'
    if args.proxy_file:
        os.environ['PROXY_FILE'] = args.proxy_file
    
    # 创建并运行应用
    app = StockKDJApp(config_file)
    success = app.run()
    
    # 输出结果
    if success:
        logger.info("A股KDJ指标J值筛选与邮件推送成功")
        
        # 如果启用了代理池，输出代理统计信息
        proxy_stats = app.get_proxy_stats()
        if proxy_stats:
            logger.info("代理统计信息:")
            for proxy, stat in proxy_stats.items():
                if proxy:  # 跳过None
                    logger.info(f"{proxy}: 成功 {stat['success']}，失败 {stat['failure']}，成功率 {stat.get('success_rate', 0):.2f}")
    else:
        logger.error("A股KDJ指标J值筛选与邮件推送失败")
