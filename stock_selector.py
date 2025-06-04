#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票筛选模块：按不同时间维度排序J值，筛选出J值最低的股票
"""

import pandas as pd
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('stock_selector')

class StockSelector:
    """股票筛选类"""
    
    def __init__(self, stock_info_df):
        """
        初始化股票筛选器
        
        Args:
            stock_info_df (pandas.DataFrame): 包含股票代码和名称的DataFrame
        """
        self.stock_info = stock_info_df
        self.code_to_name = dict(zip(stock_info_df['code'], stock_info_df['name']))
    
    def select_lowest_j_stocks(self, stock_kdj_dict, time_dimension='daily', top_n=20):
        """
        筛选J值最低的股票
        
        Args:
            stock_kdj_dict (dict): 以股票代码为键，包含KDJ指标的DataFrame为值的字典
            time_dimension (str): 时间维度，可选值: daily, weekly, monthly
            top_n (int): 筛选数量，默认20
            
        Returns:
            pandas.DataFrame: 筛选出的股票列表，包含股票代码、名称、J值等信息
        """
        try:
            logger.info(f"开始筛选{time_dimension}维度J值最低的{top_n}支股票...")
            
            # 收集所有股票的最新J值
            latest_j_values = []
            
            for code, data in stock_kdj_dict.items():
                if data.empty or 'J' not in data.columns:
                    continue
                
                # 获取最新的J值
                latest_j = data['J'].iloc[-1]
                
                # 获取最新日期
                if '日期' in data.columns:
                    latest_date = data['日期'].iloc[-1]
                elif 'date' in data.columns.str.lower():
                    date_col = data.columns[data.columns.str.lower() == 'date'][0]
                    latest_date = data[date_col].iloc[-1]
                else:
                    latest_date = datetime.now().strftime('%Y-%m-%d')
                
                # 获取股票名称
                name = self.code_to_name.get(code, f"未知_{code}")
                
                latest_j_values.append({
                    'code': code,
                    'name': name,
                    'j_value': latest_j,
                    'date': latest_date,
                    'dimension': time_dimension
                })
            
            if not latest_j_values:
                logger.warning(f"没有找到有效的J值数据")
                return pd.DataFrame()
            
            # 转换为DataFrame
            result_df = pd.DataFrame(latest_j_values)
            
            # 按J值升序排序
            result_df = result_df.sort_values(by='j_value')
            
            # 筛选前top_n个
            result_df = result_df.head(top_n)
            
            # 重置索引
            result_df = result_df.reset_index(drop=True)
            
            logger.info(f"成功筛选出{len(result_df)}支J值最低的股票")
            return result_df
        
        except Exception as e:
            logger.error(f"筛选股票时出错: {str(e)}")
            return pd.DataFrame()
    
    def combine_dimension_results(self, daily_result, weekly_result, monthly_result):
        """
        合并三个时间维度的筛选结果
        
        Args:
            daily_result (pandas.DataFrame): 日维度筛选结果
            weekly_result (pandas.DataFrame): 周维度筛选结果
            monthly_result (pandas.DataFrame): 月维度筛选结果
            
        Returns:
            dict: 包含三个时间维度筛选结果的字典
        """
        return {
            'daily': daily_result,
            'weekly': weekly_result,
            'monthly': monthly_result
        }


# 测试代码
if __name__ == "__main__":
    from data_fetcher import StockDataFetcher
    from kdj_calculator import KDJCalculator
    
    # 创建数据获取器、KDJ计算器和股票筛选器
    fetcher = StockDataFetcher()
    calculator = KDJCalculator()
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    selector = StockSelector(stock_list)
    
    # 测试日维度筛选（仅获取前10支股票用于测试）
    test_codes = stock_list['code'].tolist()[:10]
    
    # 获取日线数据
    daily_data = fetcher.get_batch_stock_data(test_codes, period='daily', max_stocks=10)
    daily_kdj = calculator.calculate_batch_kdj(daily_data)
    
    # 筛选日维度J值最低的5支股票
    daily_result = selector.select_lowest_j_stocks(daily_kdj, time_dimension='daily', top_n=5)
    print("\n日维度J值最低的5支股票:")
    print(daily_result)
    
    # 获取周线数据（测试用）
    weekly_data = fetcher.get_batch_stock_data(test_codes, period='weekly', max_stocks=10)
    weekly_kdj = calculator.calculate_batch_kdj(weekly_data)
    
    # 筛选周维度J值最低的5支股票
    weekly_result = selector.select_lowest_j_stocks(weekly_kdj, time_dimension='weekly', top_n=5)
    print("\n周维度J值最低的5支股票:")
    print(weekly_result)
    
    # 合并结果（测试用，月维度略过）
    combined_results = selector.combine_dimension_results(daily_result, weekly_result, pd.DataFrame())
    print("\n合并后的结果字典包含的维度:")
    for dimension, result in combined_results.items():
        if not result.empty:
            print(f"{dimension}: {len(result)}支股票")
