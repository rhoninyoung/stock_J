#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KDJ计算模块：负责计算股票的KDJ指标
"""

import pandas as pd
import numpy as np
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kdj_calculator')

class KDJCalculator:
    """KDJ指标计算类"""
    
    def __init__(self, n=9, m1=3, m2=3):
        """
        初始化KDJ计算器
        
        Args:
            n (int): RSV计算周期，默认9
            m1 (int): K值平滑因子，默认3
            m2 (int): D值平滑因子，默认3
        """
        self.n = n
        self.m1 = m1
        self.m2 = m2
        
    def calculate_kdj(self, stock_data):
        """
        计算KDJ指标
        
        Args:
            stock_data (pandas.DataFrame): 股票历史K线数据，必须包含'最高'、'最低'、'收盘'列
            
        Returns:
            pandas.DataFrame: 包含K、D、J值的DataFrame
        """
        try:
            # 确保数据包含必要的列
            required_columns = ['最高', '最低', '收盘']
            # 检查akshare返回的列名是否包含中文
            if '最高' in stock_data.columns and '最低' in stock_data.columns and '收盘' in stock_data.columns:
                high_col, low_col, close_col = '最高', '最低', '收盘'
            # 检查是否使用英文列名
            elif 'high' in stock_data.columns.str.lower() and 'low' in stock_data.columns.str.lower() and 'close' in stock_data.columns.str.lower():
                # 找到对应的列名（不区分大小写）
                high_col = stock_data.columns[stock_data.columns.str.lower() == 'high'][0]
                low_col = stock_data.columns[stock_data.columns.str.lower() == 'low'][0]
                close_col = stock_data.columns[stock_data.columns.str.lower() == 'close'][0]
            else:
                logger.error("数据中缺少必要的列：最高/high、最低/low、收盘/close")
                return pd.DataFrame()
            
            # 复制数据，避免修改原始数据
            df = stock_data.copy()
            
            # 计算N日内的最高价和最低价
            df['highest'] = df[high_col].rolling(window=self.n).max()
            df['lowest'] = df[low_col].rolling(window=self.n).min()
            
            # 计算RSV值：(收盘价 - 最低价) / (最高价 - 最低价) * 100
            df['RSV'] = (df[close_col] - df['lowest']) / (df['highest'] - df['lowest']) * 100
            # 处理可能的除零情况
            df['RSV'] = df['RSV'].replace([np.inf, -np.inf], np.nan).fillna(50)
            
            # 计算K值：当日K值 = 前一日K值 * (m1-1)/m1 + 当日RSV/m1
            df['K'] = df['RSV'].ewm(alpha=1/self.m1, adjust=False).mean()
            
            # 计算D值：当日D值 = 前一日D值 * (m2-1)/m2 + 当日K值/m2
            df['D'] = df['K'].ewm(alpha=1/self.m2, adjust=False).mean()
            
            # 计算J值：J = 3*K - 2*D
            df['J'] = 3 * df['K'] - 2 * df['D']
            
            # 保留原始数据列和KDJ列
            result = df[[high_col, low_col, close_col, 'K', 'D', 'J']]
            
            logger.info(f"成功计算KDJ指标，数据量: {len(result)}")
            return result
        
        except Exception as e:
            logger.error(f"计算KDJ指标失败: {str(e)}")
            return pd.DataFrame()
    
    def calculate_batch_kdj(self, stock_data_dict):
        """
        批量计算多只股票的KDJ指标
        
        Args:
            stock_data_dict (dict): 以股票代码为键，历史K线数据DataFrame为值的字典
            
        Returns:
            dict: 以股票代码为键，包含KDJ指标的DataFrame为值的字典
        """
        result = {}
        total = len(stock_data_dict)
        
        for i, (code, data) in enumerate(stock_data_dict.items()):
            try:
                logger.info(f"进度: {i+1}/{total} - 计算股票 {code} 的KDJ指标")
                kdj_data = self.calculate_kdj(data)
                if not kdj_data.empty:
                    result[code] = kdj_data
            except Exception as e:
                logger.error(f"计算股票 {code} 的KDJ指标时出错: {str(e)}")
                continue
                
        logger.info(f"批量计算完成，成功计算 {len(result)}/{total} 支股票的KDJ指标")
        return result


# 测试代码
if __name__ == "__main__":
    from data_fetcher import StockDataFetcher
    
    # 创建数据获取器和KDJ计算器
    fetcher = StockDataFetcher()
    calculator = KDJCalculator()
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    
    # 测试单只股票的KDJ计算
    test_code = "000001"
    daily_data = fetcher.get_stock_data(test_code, period='daily')
    kdj_data = calculator.calculate_kdj(daily_data)
    print(f"\n股票 {test_code} 的KDJ指标:")
    print(kdj_data.tail())
    
    # 测试批量计算KDJ（仅获取前3支股票用于测试）
    test_codes = stock_list['code'].tolist()[:3]
    batch_data = fetcher.get_batch_stock_data(test_codes, max_stocks=3)
    batch_kdj = calculator.calculate_batch_kdj(batch_data)
    print(f"\n批量计算结果，成功计算 {len(batch_kdj)} 支股票的KDJ指标")
    for code, data in batch_kdj.items():
        print(f"股票 {code} 最新J值: {data['J'].iloc[-1]:.2f}")
