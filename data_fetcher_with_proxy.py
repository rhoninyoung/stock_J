#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取模块：负责从免费数据源获取A股股票列表和历史K线数据
增强版：添加请求延迟、重试机制、分批处理、数据缓存和代理池支持
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import time
import random
import json
import pickle
from datetime import datetime, timedelta
import logging
import requests
from functools import wraps

# 导入代理池模块
from proxy_pool import ProxyPool, ProxyRequests

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_fetcher')

# 重试装饰器
def retry_decorator(max_retries=3, delay_base=2, delay_randomize=True, exceptions=(Exception,)):
    """
    重试装饰器，用于在遇到异常时自动重试
    
    Args:
        max_retries (int): 最大重试次数
        delay_base (int): 基础延迟时间（秒）
        delay_randomize (bool): 是否随机化延迟时间
        exceptions (tuple): 需要捕获的异常类型
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"达到最大重试次数 {max_retries}，操作失败: {str(e)}")
                        raise
                    
                    # 计算延迟时间（指数退避策略）
                    delay = delay_base * (2 ** (retries - 1))
                    if delay_randomize:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(f"操作失败，{retries}/{max_retries} 次重试，延迟 {delay:.2f} 秒: {str(e)}")
                    time.sleep(delay)
        return wrapper
    return decorator

class StockDataFetcher:
    """A股数据获取类（增强版，支持代理池）"""
    
    def __init__(self, cache_dir=None, request_delay=(1, 3), use_proxy=False, proxy_list=None, proxy_file=None):
        """
        初始化数据获取器
        
        Args:
            cache_dir (str): 缓存目录，默认为当前目录下的cache子目录
            request_delay (tuple): 请求延迟范围（最小值，最大值），单位为秒
            use_proxy (bool): 是否使用代理池
            proxy_list (list): 代理列表，格式为 ["http://ip:port", "http://ip:port", ...]
            proxy_file (str): 代理文件路径，每行一个代理，格式为 "http://ip:port"
        """
        self.today = datetime.now().strftime('%Y%m%d')
        # 计算一年前的日期作为默认起始日期
        self.default_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        # 设置缓存目录
        if cache_dir is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cache_dir = os.path.join(script_dir, 'cache')
        self.cache_dir = cache_dir
        
        # 创建缓存目录（如果不存在）
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        # 请求延迟范围
        self.min_delay, self.max_delay = request_delay
        
        # 初始化缓存索引
        self.cache_index = self._load_cache_index()
        
        # 初始化代理池
        self.use_proxy = use_proxy
        if use_proxy:
            logger.info("初始化代理池...")
            self.proxy_pool = ProxyPool(proxies=proxy_list, proxy_file=proxy_file)
            self.proxy_requests = ProxyRequests(self.proxy_pool)
            
            # 检查代理可用性
            available_proxies = self.proxy_pool.check_proxies()
            logger.info(f"代理池初始化完成，可用代理数: {len(available_proxies)}")
        else:
            logger.info("代理池功能已禁用")
            self.proxy_pool = None
            self.proxy_requests = None
    
    def _load_cache_index(self):
        """
        加载缓存索引
        
        Returns:
            dict: 缓存索引
        """
        index_file = os.path.join(self.cache_dir, 'cache_index.json')
        if os.path.exists(index_file):
            try:
                with open(index_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载缓存索引失败: {str(e)}，将创建新索引")
        return {}
    
    def _save_cache_index(self):
        """保存缓存索引"""
        index_file = os.path.join(self.cache_dir, 'cache_index.json')
        try:
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_index, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存缓存索引失败: {str(e)}")
    
    def _get_cache_key(self, func_name, **kwargs):
        """
        生成缓存键
        
        Args:
            func_name (str): 函数名
            **kwargs: 函数参数
            
        Returns:
            str: 缓存键
        """
        # 将参数转换为排序后的字符串
        param_str = json.dumps(kwargs, sort_keys=True)
        return f"{func_name}_{param_str}"
    
    def _get_cache_path(self, cache_key):
        """
        获取缓存文件路径
        
        Args:
            cache_key (str): 缓存键
            
        Returns:
            str: 缓存文件路径
        """
        # 使用MD5哈希作为文件名，避免文件名过长或包含特殊字符
        import hashlib
        filename = hashlib.md5(cache_key.encode()).hexdigest() + '.pkl'
        return os.path.join(self.cache_dir, filename)
    
    def _cache_data(self, cache_key, data, expire_days=7):
        """
        缓存数据
        
        Args:
            cache_key (str): 缓存键
            data: 要缓存的数据
            expire_days (int): 过期天数
        """
        try:
            cache_path = self._get_cache_path(cache_key)
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            
            # 更新缓存索引
            expire_time = (datetime.now() + timedelta(days=expire_days)).timestamp()
            self.cache_index[cache_key] = {
                'path': cache_path,
                'expire_time': expire_time,
                'created_at': datetime.now().timestamp()
            }
            self._save_cache_index()
            
            logger.debug(f"数据已缓存: {cache_key}")
        except Exception as e:
            logger.warning(f"缓存数据失败: {str(e)}")
    
    def _get_cached_data(self, cache_key):
        """
        获取缓存数据
        
        Args:
            cache_key (str): 缓存键
            
        Returns:
            缓存的数据，如果缓存不存在或已过期则返回None
        """
        # 检查缓存索引
        cache_info = self.cache_index.get(cache_key)
        if not cache_info:
            return None
        
        # 检查是否过期
        if datetime.now().timestamp() > cache_info['expire_time']:
            logger.debug(f"缓存已过期: {cache_key}")
            return None
        
        # 读取缓存
        try:
            cache_path = cache_info['path']
            if os.path.exists(cache_path):
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                logger.debug(f"从缓存加载数据: {cache_key}")
                return data
        except Exception as e:
            logger.warning(f"读取缓存失败: {str(e)}")
        
        return None
    
    def _clean_expired_cache(self):
        """清理过期缓存"""
        now = datetime.now().timestamp()
        expired_keys = []
        
        for key, info in self.cache_index.items():
            if now > info['expire_time']:
                expired_keys.append(key)
                try:
                    if os.path.exists(info['path']):
                        os.remove(info['path'])
                except Exception as e:
                    logger.warning(f"删除过期缓存文件失败: {str(e)}")
        
        # 更新缓存索引
        for key in expired_keys:
            del self.cache_index[key]
        
        if expired_keys:
            self._save_cache_index()
            logger.info(f"已清理 {len(expired_keys)} 个过期缓存")
    
    def _add_request_delay(self):
        """添加请求延迟，避免请求过于频繁"""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"请求延迟 {delay:.2f} 秒")
        time.sleep(delay)
    
    def _make_request_with_proxy(self, url, method="GET", **kwargs):
        """
        使用代理池发送请求
        
        Args:
            url (str): 请求URL
            method (str): 请求方法，默认为GET
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        if not self.use_proxy or self.proxy_requests is None:
            # 不使用代理，直接发送请求
            if method.upper() == "GET":
                return requests.get(url, **kwargs)
            elif method.upper() == "POST":
                return requests.post(url, **kwargs)
            else:
                raise ValueError(f"不支持的请求方法: {method}")
        else:
            # 使用代理池发送请求
            if method.upper() == "GET":
                return self.proxy_requests.get(url, **kwargs)
            elif method.upper() == "POST":
                return self.proxy_requests.post(url, **kwargs)
            else:
                raise ValueError(f"不支持的请求方法: {method}")
    
    # 重写akshare请求方法，使用代理池
    def _patch_akshare_request(self):
        """
        修补akshare的请求方法，使用代理池发送请求
        
        注意：这是一个实验性功能，可能会因akshare版本更新而失效
        """
        if not self.use_proxy:
            return
        
        try:
            # 尝试获取akshare的requests模块
            import akshare.utils.request as ak_request
            
            # 保存原始的request函数
            original_request = ak_request.requests.request
            
            # 定义新的request函数
            def patched_request(method, url, **kwargs):
                logger.debug(f"使用代理池发送请求: {method} {url}")
                return self.proxy_requests.request(method, url, **kwargs)
            
            # 替换request函数
            ak_request.requests.request = patched_request
            logger.info("成功修补akshare请求方法，启用代理池支持")
            
            return original_request
        except Exception as e:
            logger.warning(f"修补akshare请求方法失败: {str(e)}")
            return None
    
    def _restore_akshare_request(self, original_request):
        """
        恢复akshare的原始请求方法
        
        Args:
            original_request: 原始的request函数
        """
        if original_request is None:
            return
        
        try:
            import akshare.utils.request as ak_request
            ak_request.requests.request = original_request
            logger.info("已恢复akshare原始请求方法")
        except Exception as e:
            logger.warning(f"恢复akshare请求方法失败: {str(e)}")
    
    @retry_decorator(max_retries=3, delay_base=5)
    def get_stock_list(self, use_cache=True):
        """
        获取所有A股股票列表
        
        Args:
            use_cache (bool): 是否使用缓存
            
        Returns:
            pandas.DataFrame: 包含股票代码和名称的DataFrame
        """
        # 生成缓存键
        cache_key = self._get_cache_key('get_stock_list')
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self._get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data
        
        try:
            logger.info("正在获取A股股票列表...")
            self._add_request_delay()  # 添加请求延迟
            
            # 如果启用代理池，修补akshare请求方法
            original_request = None
            if self.use_proxy:
                original_request = self._patch_akshare_request()
            
            try:
                stock_info = ak.stock_info_a_code_name()
            finally:
                # 恢复akshare原始请求方法
                if self.use_proxy and original_request:
                    self._restore_akshare_request(original_request)
            
            logger.info(f"成功获取A股股票列表，共{len(stock_info)}支股票")
            
            # 缓存数据（股票列表缓存时间较长，设为30天）
            if use_cache:
                self._cache_data(cache_key, stock_info, expire_days=30)
            
            return stock_info
        except Exception as e:
            logger.error(f"获取A股股票列表失败: {str(e)}")
            raise
    
    @retry_decorator(max_retries=3, delay_base=5)
    def get_stock_data(self, stock_code, period='daily', start_date=None, end_date=None, use_cache=True):
        """
        获取指定股票的历史K线数据
        
        Args:
            stock_code (str): 股票代码
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD，默认一年前
            end_date (str): 结束日期，格式: YYYYMMDD，默认今天
            use_cache (bool): 是否使用缓存
            
        Returns:
            pandas.DataFrame: 包含历史K线数据的DataFrame
        """
        if start_date is None:
            start_date = self.default_start_date
        if end_date is None:
            end_date = self.today
        
        # 生成缓存键
        cache_key = self._get_cache_key(
            'get_stock_data',
            stock_code=stock_code,
            period=period,
            start_date=start_date,
            end_date=end_date
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self._get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data
            
        try:
            logger.info(f"正在获取股票 {stock_code} 的 {period} 数据...")
            
            # 添加请求延迟
            self._add_request_delay()
            
            # 将period转换为akshare接口需要的格式
            period_map = {
                'daily': 'daily',
                'weekly': 'weekly',
                'monthly': 'monthly'
            }
            ak_period = period_map.get(period, 'daily')
            
            # 如果启用代理池，修补akshare请求方法
            original_request = None
            if self.use_proxy:
                original_request = self._patch_akshare_request()
            
            try:
                # 获取股票历史数据
                stock_data = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period=ak_period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"  # 前复权
                )
            finally:
                # 恢复akshare原始请求方法
                if self.use_proxy and original_request:
                    self._restore_akshare_request(original_request)
            
            logger.info(f"成功获取股票 {stock_code} 的 {period} 数据，共{len(stock_data)}条记录")
            
            # 缓存数据
            if use_cache and not stock_data.empty:
                # 根据周期设置不同的缓存过期时间
                expire_days = 1 if period == 'daily' else (7 if period == 'weekly' else 30)
                self._cache_data(cache_key, stock_data, expire_days=expire_days)
            
            return stock_data
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 的 {period} 数据失败: {str(e)}")
            # 返回空DataFrame而不是抛出异常，便于批量处理
            return pd.DataFrame()
    
    def get_batch_stock_data(self, stock_codes, period='daily', start_date=None, end_date=None, 
                            max_stocks=None, batch_size=20, use_cache=True):
        """
        批量获取多只股票的历史K线数据，使用分批处理避免请求过量
        
        Args:
            stock_codes (list): 股票代码列表
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD，默认一年前
            end_date (str): 结束日期，格式: YYYYMMDD，默认今天
            max_stocks (int): 最大获取股票数量，用于测试
            batch_size (int): 每批处理的股票数量
            use_cache (bool): 是否使用缓存
            
        Returns:
            dict: 以股票代码为键，历史K线数据DataFrame为值的字典
        """
        if max_stocks is not None:
            stock_codes = stock_codes[:max_stocks]
            
        result = {}
        total = len(stock_codes)
        
        # 分批处理
        batches = [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]
        batch_count = len(batches)
        
        for batch_idx, batch_codes in enumerate(batches):
            logger.info(f"处理第 {batch_idx+1}/{batch_count} 批，包含 {len(batch_codes)} 支股票")
            
            for i, code in enumerate(batch_codes):
                try:
                    overall_idx = batch_idx * batch_size + i + 1
                    logger.info(f"进度: {overall_idx}/{total} - 获取股票 {code} 的数据")
                    
                    data = self.get_stock_data(code, period, start_date, end_date, use_cache=use_cache)
                    if not data.empty:
                        result[code] = data
                        
                except Exception as e:
                    logger.error(f"处理股票 {code} 时出错: {str(e)}")
                    continue
            
            # 每批处理完后添加额外延迟，避免连续批次请求过快
            if batch_idx < batch_count - 1:  # 不是最后一批
                batch_delay = random.uniform(3, 5)  # 批次间延迟3-5秒
                logger.info(f"批次间延迟 {batch_delay:.2f} 秒")
                time.sleep(batch_delay)
                
        logger.info(f"批量获取完成，成功获取 {len(result)}/{total} 支股票的数据")
        return result
    
    def get_multi_period_data(self, stock_codes, periods=['daily', 'weekly', 'monthly'], 
                             max_stocks=None, batch_size=20, use_cache=True):
        """
        获取多个周期的股票数据
        
        Args:
            stock_codes (list): 股票代码列表
            periods (list): 周期列表，可选值: daily, weekly, monthly
            max_stocks (int): 最大获取股票数量，用于测试
            batch_size (int): 每批处理的股票数量
            use_cache (bool): 是否使用缓存
            
        Returns:
            dict: 以周期为键，股票数据字典为值的嵌套字典
        """
        result = {}
        
        for period in periods:
            logger.info(f"获取 {period} 周期数据...")
            period_data = self.get_batch_stock_data(
                stock_codes, 
                period=period, 
                max_stocks=max_stocks, 
                batch_size=batch_size,
                use_cache=use_cache
            )
            result[period] = period_data
            
            # 不同周期间添加额外延迟
            if period != periods[-1]:  # 不是最后一个周期
                period_delay = random.uniform(5, 10)  # 周期间延迟5-10秒
                logger.info(f"周期间延迟 {period_delay:.2f} 秒")
                time.sleep(period_delay)
        
        return result
    
    def get_proxy_stats(self):
        """
        获取代理统计信息
        
        Returns:
            dict: 代理统计信息，如果未启用代理池则返回None
        """
        if not self.use_proxy or self.proxy_pool is None:
            return None
        
        return self.proxy_pool.get_proxy_stats()


# 测试代码
if __name__ == "__main__":
    # 测试不使用代理池
    logger.info("=== 测试不使用代理池 ===")
    fetcher1 = StockDataFetcher(request_delay=(0.5, 1.5), use_proxy=False)
    
    # 清理过期缓存
    fetcher1._clean_expired_cache()
    
    # 获取股票列表
    stock_list = fetcher1.get_stock_list()
    print(f"获取到 {len(stock_list)} 支股票")
    print(stock_list.head())
    
    # 测试获取单只股票数据
    test_code = "000001"
    daily_data = fetcher1.get_stock_data(test_code, period='daily')
    print(f"\n股票 {test_code} 日线数据:")
    print(daily_data.head())
    
    # 测试使用代理池
    logger.info("\n=== 测试使用代理池 ===")
    # 创建一个包含示例代理的列表（实际使用时需替换为真实代理）
    test_proxies = [
        "http://127.0.0.1:8080",
        "http://127.0.0.1:8081"
    ]
    
    fetcher2 = StockDataFetcher(request_delay=(0.5, 1.5), use_proxy=True, proxy_list=test_proxies)
    
    try:
        # 获取股票列表
        proxy_stock_list = fetcher2.get_stock_list()
        print(f"\n使用代理池获取到 {len(proxy_stock_list)} 支股票")
    except Exception as e:
        print(f"\n使用代理池获取股票列表失败: {str(e)}")
    
    # 查看代理统计信息
    proxy_stats = fetcher2.get_proxy_stats()
    if proxy_stats:
        print("\n代理统计信息:")
        for proxy, stat in proxy_stats.items():
            if proxy:  # 跳过None
                print(f"{proxy}: 成功 {stat['success']}，失败 {stat['failure']}，成功率 {stat.get('success_rate', 0):.2f}")
