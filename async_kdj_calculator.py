#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步KDJ计算模块：使用多线程和多进程优化KDJ指标计算
"""

import pandas as pd
import numpy as np
import logging
import time
import os
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Any, Union
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('async_kdj_calculator')

class AsyncKDJCalculator:
    """KDJ指标计算类（异步优化版）"""
    
    def __init__(self, n=9, m1=3, m2=3, max_workers=None, use_multiprocessing=False):
        """
        初始化KDJ计算器
        
        Args:
            n (int): 计算RSV的天数，默认为9
            m1 (int): 计算K值的天数，默认为3
            m2 (int): 计算D值的天数，默认为3
            max_workers (int): 最大工作线程数，默认为CPU核心数的2倍
            use_multiprocessing (bool): 是否使用多进程，默认为False
        """
        self.n = n
        self.m1 = m1
        self.m2 = m2
        
        # 设置最大工作线程数
        if max_workers is None:
            max_workers = min(32, os.cpu_count() * 2)  # 最大32个线程，默认为CPU核心数的2倍
        self.max_workers = max_workers
        
        # 是否使用多进程
        self.use_multiprocessing = use_multiprocessing
        
        # 创建线程池和进程池
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        if self.use_multiprocessing:
            self.process_pool = ProcessPoolExecutor(max_workers=min(os.cpu_count(), 8))  # 最大8个进程
        
        # 性能指标
        self.performance_metrics = {
            'total_calculations': 0,
            'calculation_times': []
        }
    
    def calculate_kdj(self, stock_data):
        """
        计算单只股票的KDJ指标
        
        Args:
            stock_data (pandas.DataFrame): 股票历史数据，必须包含日期、最高价、最低价和收盘价列
            
        Returns:
            pandas.DataFrame: 包含KDJ指标的DataFrame
        """
        try:
            # 记录开始时间
            start_time = time.time()
            
            # 确保列名一致
            if '日期' in stock_data.columns:
                date_col = '日期'
            elif 'date' in stock_data.columns:
                date_col = 'date'
            else:
                # 尝试找到日期列
                for col in stock_data.columns:
                    if '日' in col or 'date' in col.lower():
                        date_col = col
                        break
                else:
                    raise ValueError(f"无法找到日期列: {stock_data.columns}")
            
            if '最高' in stock_data.columns:
                high_col = '最高'
            elif 'high' in stock_data.columns:
                high_col = 'high'
            else:
                # 尝试找到最高价列
                for col in stock_data.columns:
                    if '高' in col or 'high' in col.lower():
                        high_col = col
                        break
                else:
                    raise ValueError(f"无法找到最高价列: {stock_data.columns}")
            
            if '最低' in stock_data.columns:
                low_col = '最低'
            elif 'low' in stock_data.columns:
                low_col = 'low'
            else:
                # 尝试找到最低价列
                for col in stock_data.columns:
                    if '低' in col or 'low' in col.lower():
                        low_col = col
                        break
                else:
                    raise ValueError(f"无法找到最低价列: {stock_data.columns}")
            
            if '收盘' in stock_data.columns:
                close_col = '收盘'
            elif 'close' in stock_data.columns:
                close_col = 'close'
            else:
                # 尝试找到收盘价列
                for col in stock_data.columns:
                    if '收' in col or 'close' in col.lower():
                        close_col = col
                        break
                else:
                    raise ValueError(f"无法找到收盘价列: {stock_data.columns}")
            
            # 计算KDJ指标
            df = stock_data.copy()
            
            # 计算N日内的最高价和最低价
            df['HH'] = df[high_col].rolling(window=self.n).max()
            df['LL'] = df[low_col].rolling(window=self.n).min()
            
            # 计算未成熟随机值RSV
            df['RSV'] = 100 * (df[close_col] - df['LL']) / (df['HH'] - df['LL'])
            
            # 计算K值、D值和J值
            df['K'] = df['RSV'].ewm(alpha=1/self.m1, adjust=False).mean()
            df['D'] = df['K'].ewm(alpha=1/self.m2, adjust=False).mean()
            df['J'] = 3 * df['K'] - 2 * df['D']
            
            # 只保留需要的列
            result = df[[date_col, 'K', 'D', 'J']].copy()
            result.dropna(inplace=True)
            
            # 记录计算时间
            calculation_time = time.time() - start_time
            self.performance_metrics['calculation_times'].append(calculation_time)
            self.performance_metrics['total_calculations'] += 1
            
            logger.debug(f"成功计算KDJ指标，数据量: {len(result)}，耗时: {calculation_time:.4f}秒")
            
            return result
        except Exception as e:
            logger.error(f"计算KDJ指标失败: {str(e)}")
            return pd.DataFrame()
    
    def _process_stock_data(self, stock_code, stock_data):
        """
        处理单只股票的数据（用于多线程/多进程）
        
        Args:
            stock_code (str): 股票代码
            stock_data (pandas.DataFrame): 股票历史数据
            
        Returns:
            tuple: (股票代码, KDJ指标DataFrame)
        """
        try:
            kdj_data = self.calculate_kdj(stock_data)
            return stock_code, kdj_data
        except Exception as e:
            logger.error(f"处理股票 {stock_code} 时出错: {str(e)}")
            return stock_code, pd.DataFrame()
    
    def calculate_batch_kdj_threaded(self, stock_data_dict):
        """
        使用多线程计算多只股票的KDJ指标
        
        Args:
            stock_data_dict (dict): 以股票代码为键，历史数据DataFrame为值的字典
            
        Returns:
            dict: 以股票代码为键，KDJ指标DataFrame为值的字典
        """
        result = {}
        total = len(stock_data_dict)
        
        logger.info(f"使用多线程计算 {total} 支股票的KDJ指标...")
        start_time = time.time()
        
        # 使用线程池并行计算
        futures = {}
        for code, data in stock_data_dict.items():
            if not data.empty:
                future = self.thread_pool.submit(self._process_stock_data, code, data)
                futures[future] = code
        
        # 收集结果
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            code = futures[future]
            try:
                code, kdj_data = future.result()
                if not kdj_data.empty:
                    result[code] = kdj_data
                
                completed += 1
                if completed % 10 == 0 or completed == total:
                    logger.info(f"进度: {completed}/{total} - {completed/total*100:.1f}%")
                    
            except Exception as e:
                logger.error(f"处理股票 {code} 时出错: {str(e)}")
        
        total_time = time.time() - start_time
        logger.info(f"多线程计算完成，成功计算 {len(result)}/{total} 支股票的KDJ指标，总耗时: {total_time:.2f}秒")
        
        return result
    
    def calculate_batch_kdj_multiprocess(self, stock_data_dict):
        """
        使用多进程计算多只股票的KDJ指标
        
        Args:
            stock_data_dict (dict): 以股票代码为键，历史数据DataFrame为值的字典
            
        Returns:
            dict: 以股票代码为键，KDJ指标DataFrame为值的字典
        """
        if not self.use_multiprocessing:
            logger.warning("多进程模式未启用，将使用多线程模式")
            return self.calculate_batch_kdj_threaded(stock_data_dict)
        
        result = {}
        total = len(stock_data_dict)
        
        logger.info(f"使用多进程计算 {total} 支股票的KDJ指标...")
        start_time = time.time()
        
        # 准备参数
        kdj_params = {
            'n': self.n,
            'm1': self.m1,
            'm2': self.m2
        }
        
        # 使用进程池并行计算
        tasks = []
        for code, data in stock_data_dict.items():
            if not data.empty:
                tasks.append((code, data, kdj_params))
        
        # 多进程工作函数
        def _process_stock_data_mp(args):
            code, data, params = args
            try:
                # 提取参数
                n = params.get('n', 9)
                m1 = params.get('m1', 3)
                m2 = params.get('m2', 3)
                
                # 确保列名一致
                if '日期' in data.columns:
                    date_col = '日期'
                elif 'date' in data.columns:
                    date_col = 'date'
                else:
                    # 尝试找到日期列
                    for col in data.columns:
                        if '日' in col or 'date' in col.lower():
                            date_col = col
                            break
                    else:
                        raise ValueError(f"无法找到日期列: {data.columns}")
                
                if '最高' in data.columns:
                    high_col = '最高'
                elif 'high' in data.columns:
                    high_col = 'high'
                else:
                    # 尝试找到最高价列
                    for col in data.columns:
                        if '高' in col or 'high' in col.lower():
                            high_col = col
                            break
                    else:
                        raise ValueError(f"无法找到最高价列: {data.columns}")
                
                if '最低' in data.columns:
                    low_col = '最低'
                elif 'low' in data.columns:
                    low_col = 'low'
                else:
                    # 尝试找到最低价列
                    for col in data.columns:
                        if '低' in col or 'low' in col.lower():
                            low_col = col
                            break
                    else:
                        raise ValueError(f"无法找到最低价列: {data.columns}")
                
                if '收盘' in data.columns:
                    close_col = '收盘'
                elif 'close' in data.columns:
                    close_col = 'close'
                else:
                    # 尝试找到收盘价列
                    for col in data.columns:
                        if '收' in col or 'close' in col.lower():
                            close_col = col
                            break
                    else:
                        raise ValueError(f"无法找到收盘价列: {data.columns}")
                
                # 计算KDJ指标
                df = data.copy()
                
                # 计算N日内的最高价和最低价
                df['HH'] = df[high_col].rolling(window=n).max()
                df['LL'] = df[low_col].rolling(window=n).min()
                
                # 计算未成熟随机值RSV
                df['RSV'] = 100 * (df[close_col] - df['LL']) / (df['HH'] - df['LL'])
                
                # 计算K值、D值和J值
                df['K'] = df['RSV'].ewm(alpha=1/m1, adjust=False).mean()
                df['D'] = df['K'].ewm(alpha=1/m2, adjust=False).mean()
                df['J'] = 3 * df['K'] - 2 * df['D']
                
                # 只保留需要的列
                result = df[[date_col, 'K', 'D', 'J']].copy()
                result.dropna(inplace=True)
                
                return code, result
            except Exception as e:
                logger.error(f"处理股票 {code} 时出错: {str(e)}")
                return code, pd.DataFrame()
        
        # 使用进程池执行任务
        with ProcessPoolExecutor(max_workers=min(os.cpu_count(), 8)) as executor:
            results = list(executor.map(_process_stock_data_mp, tasks))
        
        # 处理结果
        for code, kdj_data in results:
            if not kdj_data.empty:
                result[code] = kdj_data
        
        total_time = time.time() - start_time
        logger.info(f"多进程计算完成，成功计算 {len(result)}/{total} 支股票的KDJ指标，总耗时: {total_time:.2f}秒")
        
        return result
    
    def calculate_batch_kdj(self, stock_data_dict):
        """
        计算多只股票的KDJ指标（自动选择多线程或多进程）
        
        Args:
            stock_data_dict (dict): 以股票代码为键，历史数据DataFrame为值的字典
            
        Returns:
            dict: 以股票代码为键，KDJ指标DataFrame为值的字典
        """
        if self.use_multiprocessing and len(stock_data_dict) > 10:
            return self.calculate_batch_kdj_multiprocess(stock_data_dict)
        else:
            return self.calculate_batch_kdj_threaded(stock_data_dict)
    
    def get_performance_metrics(self):
        """
        获取性能指标
        
        Returns:
            dict: 性能指标
        """
        metrics = self.performance_metrics.copy()
        
        # 计算平均计算时间
        if metrics['calculation_times']:
            metrics['avg_calculation_time'] = sum(metrics['calculation_times']) / len(metrics['calculation_times'])
        else:
            metrics['avg_calculation_time'] = 0
        
        return metrics

# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    import random
    
    # 生成模拟股票数据
    def generate_test_data(num_stocks=10, days=100):
        stock_data_dict = {}
        for i in range(num_stocks):
            code = f"00000{i}"
            dates = pd.date_range(end='2025-06-10', periods=days).strftime('%Y-%m-%d')
            data = pd.DataFrame({
                '日期': dates,
                '开盘': [random.uniform(10, 20) for _ in range(days)],
                '收盘': [random.uniform(10, 20) for _ in range(days)],
                '最高': [random.uniform(10, 20) for _ in range(days)],
                '最低': [random.uniform(10, 20) for _ in range(days)],
                '成交量': [random.uniform(1000, 5000) for _ in range(days)]
            })
            stock_data_dict[code] = data
        return stock_data_dict
    
    # 生成测试数据
    test_data = generate_test_data(num_stocks=100, days=250)
    
    # 测试多线程计算
    logger.info("=== 测试多线程KDJ计算 ===")
    calculator1 = AsyncKDJCalculator(use_multiprocessing=False)
    start_time = time.time()
    result1 = calculator1.calculate_batch_kdj(test_data)
    threaded_time = time.time() - start_time
    logger.info(f"多线程计算耗时: {threaded_time:.2f}秒")
    
    # 测试多进程计算
    logger.info("\n=== 测试多进程KDJ计算 ===")
    calculator2 = AsyncKDJCalculator(use_multiprocessing=True)
    start_time = time.time()
    result2 = calculator2.calculate_batch_kdj(test_data)
    multiprocess_time = time.time() - start_time
    logger.info(f"多进程计算耗时: {multiprocess_time:.2f}秒")
    
    # 比较结果
    logger.info(f"\n性能比较: 多线程 {threaded_time:.2f}秒 vs 多进程 {multiprocess_time:.2f}秒")
    if multiprocess_time < threaded_time:
        logger.info(f"多进程快 {threaded_time/multiprocess_time:.2f} 倍")
    else:
        logger.info(f"多线程快 {multiprocess_time/threaded_time:.2f} 倍")
