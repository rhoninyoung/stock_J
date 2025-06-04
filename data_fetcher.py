#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取模块：负责从免费数据源获取A股股票列表和历史K线数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_fetcher')

class StockDataFetcher:
    """A股数据获取类"""
    
    def __init__(self):
        """初始化数据获取器"""
        self.today = datetime.now().strftime('%Y%m%d')
        # 计算一年前的日期作为默认起始日期
        self.default_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    def get_stock_list(self):
        """
        获取所有A股股票列表
        
        Returns:
            pandas.DataFrame: 包含股票代码和名称的DataFrame
        """
        try:
            logger.info("正在获取A股股票列表...")
            stock_info = ak.stock_info_a_code_name()
            logger.info(f"成功获取A股股票列表，共{len(stock_info)}支股票")
            return stock_info
        except Exception as e:
            logger.error(f"获取A股股票列表失败: {str(e)}")
            raise
    
    def get_stock_data(self, stock_code, period='daily', start_date=None, end_date=None):
        """
        获取指定股票的历史K线数据
        
        Args:
            stock_code (str): 股票代码
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD，默认一年前
            end_date (str): 结束日期，格式: YYYYMMDD，默认今天
            
        Returns:
            pandas.DataFrame: 包含历史K线数据的DataFrame
        """
        if start_date is None:
            start_date = self.default_start_date
        if end_date is None:
            end_date = self.today
            
        try:
            logger.info(f"正在获取股票 {stock_code} 的 {period} 数据...")
            
            # 将period转换为akshare接口需要的格式
            period_map = {
                'daily': 'daily',
                'weekly': 'weekly',
                'monthly': 'monthly'
            }
            ak_period = period_map.get(period, 'daily')
            
            # 获取股票历史数据
            stock_data = ak.stock_zh_a_hist(
                symbol=stock_code,
                period=ak_period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            logger.info(f"成功获取股票 {stock_code} 的 {period} 数据，共{len(stock_data)}条记录")
            return stock_data
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 的 {period} 数据失败: {str(e)}")
            # 返回空DataFrame而不是抛出异常，便于批量处理
            return pd.DataFrame()
    
    def get_batch_stock_data(self, stock_codes, period='daily', start_date=None, end_date=None, max_stocks=None):
        """
        批量获取多只股票的历史K线数据
        
        Args:
            stock_codes (list): 股票代码列表
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD，默认一年前
            end_date (str): 结束日期，格式: YYYYMMDD，默认今天
            max_stocks (int): 最大获取股票数量，用于测试
            
        Returns:
            dict: 以股票代码为键，历史K线数据DataFrame为值的字典
        """
        if max_stocks is not None:
            stock_codes = stock_codes[:max_stocks]
            
        result = {}
        total = len(stock_codes)
        
        for i, code in enumerate(stock_codes):
            try:
                logger.info(f"进度: {i+1}/{total} - 获取股票 {code} 的数据")
                data = self.get_stock_data(code, period, start_date, end_date)
                if not data.empty:
                    result[code] = data
            except Exception as e:
                logger.error(f"处理股票 {code} 时出错: {str(e)}")
                continue
                
        logger.info(f"批量获取完成，成功获取 {len(result)}/{total} 支股票的数据")
        return result


# 测试代码
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    print(f"获取到 {len(stock_list)} 支股票")
    print(stock_list.head())
    
    # 测试获取单只股票数据
    test_code = "000001"
    daily_data = fetcher.get_stock_data(test_code, period='daily')
    print(f"\n股票 {test_code} 日线数据:")
    print(daily_data.head())
    
    # 测试批量获取（仅获取前5支股票用于测试）
    test_codes = stock_list['code'].tolist()[:5]
    batch_data = fetcher.get_batch_stock_data(test_codes, max_stocks=5)
    print(f"\n批量获取结果，成功获取 {len(batch_data)} 支股票的数据")
    for code, data in batch_data.items():
        print(f"股票 {code} 数据量: {len(data)} 条")
