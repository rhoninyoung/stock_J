#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步数据获取模块：使用异步IO和多线程优化A股数据获取
"""

import asyncio
import aiohttp
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
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Any, Union
import akshare as ak
import threading
import queue
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('async_data_fetcher')

# 异步重试装饰器
def async_retry_decorator(max_retries=3, delay_base=2, delay_randomize=True, exceptions=(Exception,)):
    """
    异步重试装饰器，用于在遇到异常时自动重试异步函数
    
    Args:
        max_retries (int): 最大重试次数
        delay_base (int): 基础延迟时间（秒）
        delay_randomize (bool): 是否随机化延迟时间
        exceptions (tuple): 需要捕获的异常类型
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
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
                    await asyncio.sleep(delay)
        return wrapper
    return decorator

# 同步重试装饰器
def retry_decorator(max_retries=3, delay_base=2, delay_randomize=True, exceptions=(Exception,)):
    """
    同步重试装饰器，用于在遇到异常时自动重试
    
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

class AsyncProxyPool:
    """异步代理池管理类"""
    
    def __init__(self, proxies=None, proxy_file=None, check_url="http://www.baidu.com/", 
                 timeout=5, max_fails=3, check_interval=300):
        """
        初始化异步代理池
        
        Args:
            proxies (list): 代理列表，格式为 ["http://ip:port", "http://ip:port", ...]
            proxy_file (str): 代理文件路径，每行一个代理，格式为 "http://ip:port"
            check_url (str): 用于检查代理可用性的URL
            timeout (int): 请求超时时间（秒）
            max_fails (int): 最大失败次数，超过此次数的代理将被移除
            check_interval (int): 代理可用性检查间隔（秒）
        """
        self.proxies = []
        self.proxy_stats = {}  # 记录代理成功/失败次数
        self.current_index = 0
        self.check_url = check_url
        self.timeout = timeout
        self.max_fails = max_fails
        self.check_interval = check_interval
        self.lock = threading.RLock()  # 用于线程安全操作
        
        # 添加直连选项（无代理）
        self.proxies.append(None)
        
        # 加载代理
        if proxies:
            self.add_proxies(proxies)
        
        if proxy_file and os.path.exists(proxy_file):
            self.load_proxies_from_file(proxy_file)
            
        logger.info(f"添加了 {len(self.proxies) - 1} 个代理，当前代理池大小: {len(self.proxies)}")
        
        # 启动异步代理检查任务
        self._start_proxy_checker()
    
    def add_proxies(self, proxies):
        """
        添加代理到代理池
        
        Args:
            proxies (list): 代理列表
        """
        with self.lock:
            for proxy in proxies:
                if proxy and proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_stats[proxy] = {"success": 0, "failure": 0}
    
    def load_proxies_from_file(self, file_path):
        """
        从文件加载代理
        
        Args:
            file_path (str): 代理文件路径
        """
        try:
            with open(file_path, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
                self.add_proxies(proxies)
        except Exception as e:
            logger.error(f"从文件加载代理失败: {str(e)}")
    
    def get_proxy(self):
        """
        获取下一个代理
        
        Returns:
            str: 代理地址，如果没有可用代理则返回None
        """
        with self.lock:
            if not self.proxies:
                return None
            
            # 使用轮询策略选择代理
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            return proxy
    
    def mark_success(self, proxy):
        """
        标记代理请求成功
        
        Args:
            proxy (str): 代理地址
        """
        if not proxy:  # 跳过None（直连）
            return
            
        with self.lock:
            if proxy not in self.proxy_stats:
                self.proxy_stats[proxy] = {"success": 0, "failure": 0}
            self.proxy_stats[proxy]["success"] += 1
            
            # 计算成功率
            total = self.proxy_stats[proxy]["success"] + self.proxy_stats[proxy]["failure"]
            if total > 0:
                self.proxy_stats[proxy]["success_rate"] = self.proxy_stats[proxy]["success"] / total
    
    def mark_failure(self, proxy):
        """
        标记代理请求失败
        
        Args:
            proxy (str): 代理地址
        """
        if not proxy:  # 跳过None（直连）
            return
            
        with self.lock:
            if proxy not in self.proxy_stats:
                self.proxy_stats[proxy] = {"success": 0, "failure": 0}
            self.proxy_stats[proxy]["failure"] += 1
            
            # 计算成功率
            total = self.proxy_stats[proxy]["success"] + self.proxy_stats[proxy]["failure"]
            if total > 0:
                self.proxy_stats[proxy]["success_rate"] = self.proxy_stats[proxy]["success"] / total
            
            # 检查是否超过最大失败次数
            if self.proxy_stats[proxy]["failure"] >= self.max_fails:
                if proxy in self.proxies:
                    logger.warning(f"代理 {proxy} 失败次数过多，从代理池中移除")
                    self.proxies.remove(proxy)
    
    def get_proxy_stats(self):
        """
        获取代理统计信息
        
        Returns:
            dict: 代理统计信息
        """
        with self.lock:
            return self.proxy_stats.copy()
    
    async def check_proxy(self, proxy):
        """
        异步检查代理可用性
        
        Args:
            proxy (str): 代理地址
            
        Returns:
            bool: 代理是否可用
        """
        if not proxy:  # None表示直连，始终可用
            return True
            
        try:
            start_time = time.time()
            async with aiohttp.ClientSession() as session:
                async with session.get(self.check_url, proxy=proxy, timeout=self.timeout) as response:
                    if response.status == 200:
                        elapsed = time.time() - start_time
                        logger.debug(f"代理 {proxy} 可用，响应时间: {elapsed:.2f}秒")
                        return True
                    else:
                        logger.warning(f"代理 {proxy} 不可用，状态码: {response.status}")
                        return False
        except Exception as e:
            logger.warning(f"代理 {proxy} 不可用: {str(e)}")
            return False
    
    async def check_proxies(self):
        """
        异步检查所有代理可用性
        
        Returns:
            list: 可用代理列表
        """
        logger.info("开始检查代理可用性...")
        available_proxies = []
        
        # 始终保留None（直连选项）
        available_proxies.append(None)
        
        # 复制代理列表，避免在迭代过程中修改
        with self.lock:
            proxies_to_check = [p for p in self.proxies if p is not None]
        
        # 并发检查所有代理
        tasks = [self.check_proxy(proxy) for proxy in proxies_to_check]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for proxy, result in zip(proxies_to_check, results):
            if isinstance(result, bool) and result:
                available_proxies.append(proxy)
        
        # 更新代理池
        with self.lock:
            self.proxies = available_proxies
            self.current_index = 0
        
        logger.info(f"代理可用性检查完成，可用代理数: {len(available_proxies)}")
        return available_proxies
    
    def _start_proxy_checker(self):
        """启动异步代理检查任务"""
        def run_checker():
            while True:
                try:
                    # 创建新的事件循环
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    # 运行代理检查
                    loop.run_until_complete(self.check_proxies())
                    
                    # 关闭事件循环
                    loop.close()
                except Exception as e:
                    logger.error(f"代理检查任务出错: {str(e)}")
                
                # 等待下一次检查
                time.sleep(self.check_interval)
        
        # 在后台线程中运行代理检查
        checker_thread = threading.Thread(target=run_checker, daemon=True)
        checker_thread.start()

class AsyncRequests:
    """异步HTTP请求类，支持代理池"""
    
    def __init__(self, proxy_pool=None, timeout=10, max_retries=3):
        """
        初始化异步请求类
        
        Args:
            proxy_pool (AsyncProxyPool): 代理池
            timeout (int): 请求超时时间（秒）
            max_retries (int): 最大重试次数
        """
        self.proxy_pool = proxy_pool
        self.timeout = timeout
        self.max_retries = max_retries
    
    @async_retry_decorator(max_retries=3)
    async def request(self, method, url, **kwargs):
        """
        发送异步HTTP请求
        
        Args:
            method (str): 请求方法
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            aiohttp.ClientResponse: 响应对象
        """
        proxy = None
        if self.proxy_pool:
            proxy = self.proxy_pool.get_proxy()
        
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        
        try:
            async with aiohttp.ClientSession() as session:
                if method.upper() == 'GET':
                    async with session.get(url, proxy=proxy, **kwargs) as response:
                        # 读取响应内容，确保连接已关闭
                        content = await response.read()
                        if self.proxy_pool:
                            self.proxy_pool.mark_success(proxy)
                        return response, content
                elif method.upper() == 'POST':
                    async with session.post(url, proxy=proxy, **kwargs) as response:
                        content = await response.read()
                        if self.proxy_pool:
                            self.proxy_pool.mark_success(proxy)
                        return response, content
                else:
                    raise ValueError(f"不支持的请求方法: {method}")
        except Exception as e:
            if self.proxy_pool:
                self.proxy_pool.mark_failure(proxy)
            raise
    
    async def get(self, url, **kwargs):
        """
        发送异步GET请求
        
        Args:
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            tuple: (响应对象, 响应内容)
        """
        return await self.request('GET', url, **kwargs)
    
    async def post(self, url, **kwargs):
        """
        发送异步POST请求
        
        Args:
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            tuple: (响应对象, 响应内容)
        """
        return await self.request('POST', url, **kwargs)

class AsyncStockDataFetcher:
    """异步A股数据获取类（使用异步IO和多线程优化）"""
    
    def __init__(self, cache_dir=None, request_delay=(1, 3), use_proxy=False, proxy_list=None, 
                 proxy_file=None, max_workers=None, use_multiprocessing=False):
        """
        初始化异步数据获取器
        
        Args:
            cache_dir (str): 缓存目录，默认为当前目录下的cache子目录
            request_delay (tuple): 请求延迟范围（最小值，最大值），单位为秒
            use_proxy (bool): 是否使用代理池
            proxy_list (list): 代理列表，格式为 ["http://ip:port", "http://ip:port", ...]
            proxy_file (str): 代理文件路径，每行一个代理，格式为 "http://ip:port"
            max_workers (int): 最大工作线程数，默认为CPU核心数的2倍
            use_multiprocessing (bool): 是否使用多进程，默认为False
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
        
        # 设置最大工作线程数
        if max_workers is None:
            max_workers = min(32, os.cpu_count() * 2)  # 最大32个线程，默认为CPU核心数的2倍
        self.max_workers = max_workers
        
        # 是否使用多进程
        self.use_multiprocessing = use_multiprocessing
        
        # 初始化代理池
        self.use_proxy = use_proxy
        if use_proxy:
            logger.info("初始化异步代理池...")
            self.proxy_pool = AsyncProxyPool(proxies=proxy_list, proxy_file=proxy_file)
            self.async_requests = AsyncRequests(self.proxy_pool)
            
            # 异步检查代理可用性
            loop = asyncio.get_event_loop()
            available_proxies = loop.run_until_complete(self.proxy_pool.check_proxies())
            logger.info(f"代理池初始化完成，可用代理数: {len(available_proxies)}")
        else:
            logger.info("代理池功能已禁用")
            self.proxy_pool = None
            self.async_requests = None
        
        # 创建线程池和进程池
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        if self.use_multiprocessing:
            self.process_pool = ProcessPoolExecutor(max_workers=min(os.cpu_count(), 8))  # 最大8个进程
        
        # 性能指标
        self.performance_metrics = {
            'total_requests': 0,
            'cache_hits': 0,
            'request_times': [],
            'processing_times': []
        }
    
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
                self.performance_metrics['cache_hits'] += 1
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
    
    async def _add_request_delay(self):
        """添加异步请求延迟，避免请求过于频繁"""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"请求延迟 {delay:.2f} 秒")
        await asyncio.sleep(delay)
    
    def _add_request_delay_sync(self):
        """添加同步请求延迟，避免请求过于频繁"""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"请求延迟 {delay:.2f} 秒")
        time.sleep(delay)
    
    @retry_decorator(max_retries=3, delay_base=5)
    def get_stock_list(self, use_cache=True):
        """
        获取所有A股股票列表（同步方法）
        
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
            self._add_request_delay_sync()  # 添加请求延迟
            
            # 记录开始时间
            start_time = time.time()
            
            # 获取股票列表
            stock_info = ak.stock_info_a_code_name()
            
            # 记录请求时间
            request_time = time.time() - start_time
            self.performance_metrics['request_times'].append(request_time)
            self.performance_metrics['total_requests'] += 1
            
            logger.info(f"成功获取A股股票列表，共{len(stock_info)}支股票，耗时: {request_time:.2f}秒")
            
            # 缓存数据（股票列表缓存时间较长，设为30天）
            if use_cache:
                self._cache_data(cache_key, stock_info, expire_days=30)
            
            return stock_info
        except Exception as e:
            logger.error(f"获取A股股票列表失败: {str(e)}")
            raise
    
    async def _fetch_stock_data_async(self, stock_code, period='daily', start_date=None, end_date=None):
        """
        异步获取单只股票的历史K线数据（内部方法）
        
        Args:
            stock_code (str): 股票代码
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD
            end_date (str): 结束日期，格式: YYYYMMDD
            
        Returns:
            pandas.DataFrame: 包含历史K线数据的DataFrame
        """
        if start_date is None:
            start_date = self.default_start_date
        if end_date is None:
            end_date = self.today
        
        try:
            logger.debug(f"异步获取股票 {stock_code} 的 {period} 数据...")
            
            # 添加请求延迟
            await self._add_request_delay()
            
            # 将period转换为akshare接口需要的格式
            period_map = {
                'daily': 'daily',
                'weekly': 'weekly',
                'monthly': 'monthly'
            }
            ak_period = period_map.get(period, 'daily')
            
            # 记录开始时间
            start_time = time.time()
            
            # 由于akshare不支持异步，使用线程池执行同步请求
            loop = asyncio.get_event_loop()
            stock_data = await loop.run_in_executor(
                self.thread_pool,
                lambda: ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period=ak_period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"  # 前复权
                )
            )
            
            # 记录请求时间
            request_time = time.time() - start_time
            self.performance_metrics['request_times'].append(request_time)
            self.performance_metrics['total_requests'] += 1
            
            logger.debug(f"成功获取股票 {stock_code} 的 {period} 数据，共{len(stock_data)}条记录，耗时: {request_time:.2f}秒")
            
            return stock_data
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 的 {period} 数据失败: {str(e)}")
            # 返回空DataFrame而不是抛出异常，便于批量处理
            return pd.DataFrame()
    
    @async_retry_decorator(max_retries=3, delay_base=5)
    async def get_stock_data_async(self, stock_code, period='daily', start_date=None, end_date=None, use_cache=True):
        """
        异步获取指定股票的历史K线数据
        
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
        
        # 获取股票数据
        stock_data = await self._fetch_stock_data_async(stock_code, period, start_date, end_date)
        
        # 缓存数据
        if use_cache and not stock_data.empty:
            # 根据周期设置不同的缓存过期时间
            expire_days = 1 if period == 'daily' else (7 if period == 'weekly' else 30)
            self._cache_data(cache_key, stock_data, expire_days=expire_days)
        
        return stock_data
    
    async def get_batch_stock_data_async(self, stock_codes, period='daily', start_date=None, end_date=None, 
                                        max_stocks=None, batch_size=20, use_cache=True, concurrency_limit=10):
        """
        异步批量获取多只股票的历史K线数据
        
        Args:
            stock_codes (list): 股票代码列表
            period (str): 周期，可选值: daily, weekly, monthly
            start_date (str): 开始日期，格式: YYYYMMDD，默认一年前
            end_date (str): 结束日期，格式: YYYYMMDD，默认今天
            max_stocks (int): 最大获取股票数量，用于测试
            batch_size (int): 每批处理的股票数量
            use_cache (bool): 是否使用缓存
            concurrency_limit (int): 并发请求限制
            
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
            
            # 创建信号量限制并发请求数
            semaphore = asyncio.Semaphore(concurrency_limit)
            
            async def fetch_with_semaphore(code):
                async with semaphore:
                    return code, await self.get_stock_data_async(code, period, start_date, end_date, use_cache=use_cache)
            
            # 并发获取当前批次的所有股票数据
            tasks = [fetch_with_semaphore(code) for code in batch_codes]
            batch_results = await asyncio.gather(*tasks)
            
            # 处理结果
            for code, data in batch_results:
                if not data.empty:
                    result[code] = data
            
            # 每批处理完后添加额外延迟，避免连续批次请求过快
            if batch_idx < batch_count - 1:  # 不是最后一批
                batch_delay = random.uniform(1, 2)  # 批次间延迟1-2秒
                logger.info(f"批次间延迟 {batch_delay:.2f} 秒")
                await asyncio.sleep(batch_delay)
                
        logger.info(f"批量获取完成，成功获取 {len(result)}/{total} 支股票的数据")
        return result
    
    async def get_multi_period_data_async(self, stock_codes, periods=['daily', 'weekly', 'monthly'], 
                                         max_stocks=None, batch_size=20, use_cache=True, concurrency_limit=10):
        """
        异步获取多个周期的股票数据
        
        Args:
            stock_codes (list): 股票代码列表
            periods (list): 周期列表，可选值: daily, weekly, monthly
            max_stocks (int): 最大获取股票数量，用于测试
            batch_size (int): 每批处理的股票数量
            use_cache (bool): 是否使用缓存
            concurrency_limit (int): 并发请求限制
            
        Returns:
            dict: 以周期为键，股票数据字典为值的嵌套字典
        """
        result = {}
        
        for period in periods:
            logger.info(f"获取 {period} 周期数据...")
            period_data = await self.get_batch_stock_data_async(
                stock_codes, 
                period=period, 
                max_stocks=max_stocks, 
                batch_size=batch_size,
                use_cache=use_cache,
                concurrency_limit=concurrency_limit
            )
            result[period] = period_data
            
            # 不同周期间添加额外延迟
            if period != periods[-1]:  # 不是最后一个周期
                period_delay = random.uniform(2, 5)  # 周期间延迟2-5秒
                logger.info(f"周期间延迟 {period_delay:.2f} 秒")
                await asyncio.sleep(period_delay)
        
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
    
    def get_performance_metrics(self):
        """
        获取性能指标
        
        Returns:
            dict: 性能指标
        """
        metrics = self.performance_metrics.copy()
        
        # 计算平均请求时间
        if metrics['request_times']:
            metrics['avg_request_time'] = sum(metrics['request_times']) / len(metrics['request_times'])
        else:
            metrics['avg_request_time'] = 0
        
        # 计算缓存命中率
        if metrics['total_requests'] > 0:
            metrics['cache_hit_rate'] = metrics['cache_hits'] / metrics['total_requests']
        else:
            metrics['cache_hit_rate'] = 0
        
        return metrics

# 多进程工作函数
def _process_stock_data(stock_code, stock_data, kdj_params):
    """
    多进程工作函数：计算单只股票的KDJ指标
    
    Args:
        stock_code (str): 股票代码
        stock_data (pandas.DataFrame): 股票历史数据
        kdj_params (dict): KDJ计算参数
        
    Returns:
        tuple: (股票代码, KDJ指标DataFrame)
    """
    try:
        # 提取参数
        n = kdj_params.get('n', 9)
        m1 = kdj_params.get('m1', 3)
        m2 = kdj_params.get('m2', 3)
        
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
        
        return stock_code, result
    except Exception as e:
        print(f"处理股票 {stock_code} 时出错: {str(e)}")
        return stock_code, pd.DataFrame()

# 测试代码
async def test_async_fetcher():
    """测试异步数据获取器"""
    logger.info("=== 测试异步数据获取器 ===")
    
    # 创建异步数据获取器
    fetcher = AsyncStockDataFetcher(request_delay=(0.5, 1.0), use_proxy=False)
    
    # 清理过期缓存
    fetcher._clean_expired_cache()
    
    # 获取股票列表
    stock_list = fetcher.get_stock_list()
    logger.info(f"获取到 {len(stock_list)} 支股票")
    
    # 测试获取单只股票数据
    test_code = "000001"
    daily_data = await fetcher.get_stock_data_async(test_code, period='daily')
    logger.info(f"股票 {test_code} 日线数据: {len(daily_data)} 条记录")
    
    # 测试批量获取（仅获取前5支股票用于测试）
    test_codes = stock_list['code'].tolist()[:5]
    batch_data = await fetcher.get_batch_stock_data_async(test_codes, max_stocks=5, batch_size=2)
    logger.info(f"批量获取结果，成功获取 {len(batch_data)} 支股票的数据")
    
    # 测试多周期数据获取
    multi_period_data = await fetcher.get_multi_period_data_async(test_codes[:3], max_stocks=3, batch_size=2)
    logger.info("多周期数据获取结果:")
    for period, period_data in multi_period_data.items():
        logger.info(f"{period} 周期: 获取 {len(period_data)} 支股票的数据")
    
    # 输出性能指标
    metrics = fetcher.get_performance_metrics()
    logger.info(f"性能指标: 总请求数 {metrics['total_requests']}, 缓存命中 {metrics['cache_hits']}, 平均请求时间 {metrics.get('avg_request_time', 0):.2f}秒")
    
    return True

if __name__ == "__main__":
    # 运行异步测试
    asyncio.run(test_async_fetcher())
