#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步股票选择器模块：使用多线程优化J值筛选
"""

import pandas as pd
import numpy as np
import logging
import time
import os
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('async_stock_selector')

class AsyncStockSelector:
    """A股股票筛选类（异步优化版）"""
    
    def __init__(self, stock_list, max_workers=None):
        """
        初始化股票筛选器
        
        Args:
            stock_list (pandas.DataFrame): 股票列表，必须包含code和name列
            max_workers (int): 最大工作线程数，默认为CPU核心数的2倍
        """
        self.stock_list = stock_list
        
        # 创建股票代码到名称的映射
        self.code_to_name = dict(zip(stock_list['code'], stock_list['name']))
        
        # 设置最大工作线程数
        if max_workers is None:
            max_workers = min(32, os.cpu_count() * 2)  # 最大32个线程，默认为CPU核心数的2倍
        self.max_workers = max_workers
        
        # 创建线程池
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        
        # 性能指标
        self.performance_metrics = {
            'total_selections': 0,
            'selection_times': []
        }
    
    def _process_single_stock(self, args):
        """
        处理单只股票的KDJ数据（用于多线程）
        
        Args:
            args (tuple): (股票代码, KDJ数据, 时间维度)
            
        Returns:
            tuple: (股票代码, 股票名称, J值, 日期, 时间维度)
        """
        code, kdj_data, time_dimension = args
        
        try:
            # 获取最新的J值
            latest_j = kdj_data['J'].iloc[-1]
            latest_date = kdj_data.iloc[-1, 0]  # 第一列是日期
            
            # 获取股票名称
            name = self.code_to_name.get(code, "未知")
            
            return code, name, latest_j, latest_date, time_dimension
        except Exception as e:
            logger.error(f"处理股票 {code} 的J值时出错: {str(e)}")
            return code, "未知", float('inf'), None, time_dimension
    
    def select_lowest_j_stocks(self, kdj_data_dict, time_dimension='daily', top_n=20):
        """
        筛选J值最低的股票（多线程优化版）
        
        Args:
            kdj_data_dict (dict): 以股票代码为键，KDJ指标DataFrame为值的字典
            time_dimension (str): 时间维度，可选值: daily, weekly, monthly
            top_n (int): 筛选数量，默认为20
            
        Returns:
            pandas.DataFrame: 包含J值最低的股票信息
        """
        start_time = time.time()
        logger.info(f"开始筛选{time_dimension}维度J值最低的{top_n}支股票...")
        
        # 准备任务参数
        tasks = [(code, data, time_dimension) for code, data in kdj_data_dict.items() if not data.empty]
        
        # 使用线程池并行处理
        results = []
        with self.thread_pool as executor:
            futures = [executor.submit(self._process_single_stock, args) for args in tasks]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result[2] != float('inf'):  # 排除处理失败的股票
                        results.append(result)
                except Exception as e:
                    logger.error(f"获取线程结果时出错: {str(e)}")
        
        # 创建DataFrame并排序
        if results:
            df = pd.DataFrame(results, columns=['code', 'name', 'j_value', 'date', 'dimension'])
            
            # 按J值升序排序
            df = df.sort_values(by='j_value').reset_index(drop=True)
            
            # 取前top_n个
            if len(df) > top_n:
                df = df.head(top_n)
            
            # 记录性能指标
            selection_time = time.time() - start_time
            self.performance_metrics['selection_times'].append(selection_time)
            self.performance_metrics['total_selections'] += 1
            
            logger.info(f"成功筛选出{len(df)}支J值最低的股票，耗时: {selection_time:.4f}秒")
            return df
        else:
            logger.warning("没有有效的J值数据，无法筛选股票")
            return pd.DataFrame(columns=['code', 'name', 'j_value', 'date', 'dimension'])
    
    def select_multi_dimension_lowest_j(self, kdj_results, top_n=20):
        """
        筛选多个时间维度的J值最低股票
        
        Args:
            kdj_results (dict): 以时间维度为键，KDJ数据字典为值的嵌套字典
            top_n (int): 每个维度筛选的数量，默认为20
            
        Returns:
            dict: 以时间维度为键，筛选结果DataFrame为值的字典
        """
        start_time = time.time()
        logger.info(f"开始筛选多个时间维度的J值最低股票...")
        
        results = {}
        
        # 并行处理多个维度
        futures = {}
        with self.thread_pool as executor:
            for dimension, kdj_data in kdj_results.items():
                future = executor.submit(self.select_lowest_j_stocks, kdj_data, dimension, top_n)
                futures[future] = dimension
        
        # 收集结果
        for future in concurrent.futures.as_completed(futures):
            dimension = futures[future]
            try:
                result = future.result()
                results[dimension] = result
            except Exception as e:
                logger.error(f"处理{dimension}维度时出错: {str(e)}")
                results[dimension] = pd.DataFrame(columns=['code', 'name', 'j_value', 'date', 'dimension'])
        
        total_time = time.time() - start_time
        logger.info(f"多维度筛选完成，共处理 {len(results)} 个维度，总耗时: {total_time:.2f}秒")
        
        return results
    
    def get_performance_metrics(self):
        """
        获取性能指标
        
        Returns:
            dict: 性能指标
        """
        metrics = self.performance_metrics.copy()
        
        # 计算平均筛选时间
        if metrics['selection_times']:
            metrics['avg_selection_time'] = sum(metrics['selection_times']) / len(metrics['selection_times'])
        else:
            metrics['avg_selection_time'] = 0
        
        return metrics

# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    import random
    
    # 生成模拟股票列表
    def generate_test_stock_list(num_stocks=100):
        codes = [f"{i:06d}" for i in range(num_stocks)]
        names = [f"测试股票{i}" for i in range(num_stocks)]
        return pd.DataFrame({'code': codes, 'name': names})
    
    # 生成模拟KDJ数据
    def generate_test_kdj_data(stock_list, days=100):
        kdj_data_dict = {}
        for code in stock_list['code']:
            dates = pd.date_range(end='2025-06-10', periods=days).strftime('%Y-%m-%d')
            data = pd.DataFrame({
                '日期': dates,
                'K': [random.uniform(0, 100) for _ in range(days)],
                'D': [random.uniform(0, 100) for _ in range(days)],
                'J': [random.uniform(0, 100) for _ in range(days)]
            })
            kdj_data_dict[code] = data
        return kdj_data_dict
    
    # 生成测试数据
    test_stock_list = generate_test_stock_list(100)
    test_daily_data = generate_test_kdj_data(test_stock_list)
    test_weekly_data = generate_test_kdj_data(test_stock_list, days=50)
    test_monthly_data = generate_test_kdj_data(test_stock_list, days=24)
    
    # 创建多维度数据
    test_multi_data = {
        'daily': test_daily_data,
        'weekly': test_weekly_data,
        'monthly': test_monthly_data
    }
    
    # 测试股票筛选
    logger.info("=== 测试异步股票筛选 ===")
    selector = AsyncStockSelector(test_stock_list)
    
    # 测试单一维度筛选
    start_time = time.time()
    daily_result = selector.select_lowest_j_stocks(test_daily_data, 'daily', top_n=20)
    logger.info(f"日维度筛选耗时: {time.time() - start_time:.4f}秒")
    logger.info(f"日维度筛选结果前5条:\n{daily_result.head()}")
    
    # 测试多维度筛选
    start_time = time.time()
    multi_results = selector.select_multi_dimension_lowest_j(test_multi_data, top_n=20)
    logger.info(f"多维度筛选耗时: {time.time() - start_time:.4f}秒")
    
    # 输出性能指标
    metrics = selector.get_performance_metrics()
    logger.info(f"性能指标: 总筛选次数 {metrics['total_selections']}, 平均筛选时间 {metrics.get('avg_selection_time', 0):.4f}秒")
