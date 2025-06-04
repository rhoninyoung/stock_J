#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主控模块：协调各模块工作，实现A股KDJ指标J值筛选与邮件推送
"""

import os
import logging
import pandas as pd
import json
from datetime import datetime, timedelta
import argparse

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
                'exclude_new': True  # 排除上市不足一年的新股
            },
            'kdj': {
                'n': 9,
                'm1': 3,
                'm2': 3
            },
            'selector': {
                'top_n': 20  # 筛选数量
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
    """A股KDJ指标J值筛选与邮件推送应用"""
    
    def __init__(self, config_file=None):
        """
        初始化应用
        
        Args:
            config_file (str): 配置文件路径
        """
        # 加载配置
        self.config = Config(config_file)
        
        # 创建数据获取器
        self.fetcher = StockDataFetcher()
        
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
            
            # 获取股票列表
            stock_list = self.fetcher.get_stock_list()
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
            
            # 获取各时间维度的数据并计算KDJ
            dimensions = ['daily', 'weekly', 'monthly']
            kdj_results = {}
            
            for dimension in dimensions:
                # 获取历史数据
                logger.info(f"获取{dimension}维度的历史数据...")
                stock_data = self.fetcher.get_batch_stock_data(stock_codes, period=dimension)
                
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
    args = parser.parse_args()
    
    # 获取配置文件路径
    config_file = args.config
    if not config_file:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, 'config.json')
    
    # 创建并运行应用
    app = StockKDJApp(config_file)
    success = app.run()
    
    # 输出结果
    if success:
        logger.info("A股KDJ指标J值筛选与邮件推送成功")
    else:
        logger.error("A股KDJ指标J值筛选与邮件推送失败")
