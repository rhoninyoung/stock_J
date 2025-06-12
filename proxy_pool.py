#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
代理池模块：管理HTTP代理，提供代理轮换、可用性检测和自动切换功能
"""

import requests
import random
import time
import json
import os
import logging
from datetime import datetime, timedelta
from functools import wraps

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('proxy_pool')

class ProxyPool:
    """代理池管理类"""
    
    def __init__(self, proxies=None, proxy_file=None, check_interval=3600):
        """
        初始化代理池
        
        Args:
            proxies (list): 代理列表，格式为 ["http://ip:port", "http://ip:port", ...]
            proxy_file (str): 代理文件路径，每行一个代理，格式为 "http://ip:port"
            check_interval (int): 代理检查间隔，单位为秒
        """
        self.proxies = []
        self.current_index = 0
        self.proxy_stats = {}  # 记录代理成功/失败次数
        self.last_check_time = None
        self.check_interval = check_interval
        
        # 加载代理
        if proxies:
            self.add_proxies(proxies)
        
        if proxy_file:
            self.load_proxies_from_file(proxy_file)
        
        # 如果没有代理，添加一个None表示直连
        if not self.proxies:
            self.proxies = [None]
            logger.warning("没有可用代理，将使用直连")
    
    def add_proxies(self, proxies):
        """
        添加代理到代理池
        
        Args:
            proxies (list): 代理列表
        """
        for proxy in proxies:
            if proxy and proxy not in self.proxies:
                self.proxies.append(proxy)
                self.proxy_stats[proxy] = {"success": 0, "failure": 0, "last_used": None}
        
        logger.info(f"添加了 {len(proxies)} 个代理，当前代理池大小: {len(self.proxies)}")
    
    def load_proxies_from_file(self, file_path):
        """
        从文件加载代理
        
        Args:
            file_path (str): 代理文件路径
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"代理文件不存在: {file_path}")
                return
            
            with open(file_path, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
            
            self.add_proxies(proxies)
            logger.info(f"从文件 {file_path} 加载了 {len(proxies)} 个代理")
        
        except Exception as e:
            logger.error(f"从文件加载代理失败: {str(e)}")
    
    def save_proxies_to_file(self, file_path):
        """
        将代理保存到文件
        
        Args:
            file_path (str): 代理文件路径
        """
        try:
            with open(file_path, 'w') as f:
                for proxy in self.proxies:
                    if proxy:  # 跳过None
                        f.write(f"{proxy}\n")
            
            logger.info(f"将 {len(self.proxies)} 个代理保存到文件 {file_path}")
        
        except Exception as e:
            logger.error(f"保存代理到文件失败: {str(e)}")
    
    def get_proxy(self, strategy="round_robin"):
        """
        获取下一个代理
        
        Args:
            strategy (str): 代理选择策略，可选值: round_robin, random, weighted
            
        Returns:
            str: 代理地址
        """
        if not self.proxies:
            return None
        
        if strategy == "round_robin":
            # 轮询策略
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
        
        elif strategy == "random":
            # 随机策略
            proxy = random.choice(self.proxies)
        
        elif strategy == "weighted":
            # 加权策略，根据成功率选择
            total_weight = 0
            weights = []
            
            for p in self.proxies:
                if p is None:
                    # 直连的权重固定为1
                    weight = 1
                else:
                    stats = self.proxy_stats.get(p, {"success": 0, "failure": 0})
                    success = stats["success"]
                    failure = stats["failure"]
                    
                    # 计算权重，成功率越高，权重越大
                    if success + failure == 0:
                        weight = 1  # 未使用过的代理给予基础权重
                    else:
                        success_rate = success / (success + failure)
                        weight = max(0.1, success_rate)  # 最小权重为0.1
                
                weights.append(weight)
                total_weight += weight
            
            # 根据权重随机选择
            r = random.uniform(0, total_weight)
            cumulative_weight = 0
            
            for i, weight in enumerate(weights):
                cumulative_weight += weight
                if r <= cumulative_weight:
                    proxy = self.proxies[i]
                    break
        
        else:
            # 默认使用轮询策略
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
        
        # 更新最后使用时间
        if proxy in self.proxy_stats:
            self.proxy_stats[proxy]["last_used"] = datetime.now()
        
        return proxy
    
    def mark_success(self, proxy):
        """
        标记代理请求成功
        
        Args:
            proxy (str): 代理地址
        """
        if proxy is None:
            return
        
        if proxy not in self.proxy_stats:
            self.proxy_stats[proxy] = {"success": 0, "failure": 0, "last_used": datetime.now()}
        
        self.proxy_stats[proxy]["success"] += 1
    
    def mark_failure(self, proxy):
        """
        标记代理请求失败
        
        Args:
            proxy (str): 代理地址
        """
        if proxy is None:
            return
        
        if proxy not in self.proxy_stats:
            self.proxy_stats[proxy] = {"success": 0, "failure": 0, "last_used": datetime.now()}
        
        self.proxy_stats[proxy]["failure"] += 1
        
        # 如果失败次数过多，考虑移除该代理
        if self.proxy_stats[proxy]["failure"] > 5 and self.proxy_stats[proxy]["success"] == 0:
            if len(self.proxies) > 1:  # 确保至少有一个代理
                logger.warning(f"代理 {proxy} 失败次数过多，已移除")
                self.remove_proxy(proxy)
    
    def remove_proxy(self, proxy):
        """
        从代理池中移除代理
        
        Args:
            proxy (str): 代理地址
        """
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            
            if proxy in self.proxy_stats:
                del self.proxy_stats[proxy]
            
            # 调整当前索引
            if self.current_index >= len(self.proxies) and len(self.proxies) > 0:
                self.current_index = 0
    
    def check_proxies(self, test_url="http://www.baidu.com", timeout=5):
        """
        检查代理可用性
        
        Args:
            test_url (str): 测试URL
            timeout (int): 超时时间，单位为秒
            
        Returns:
            list: 可用代理列表
        """
        now = datetime.now()
        
        # 如果距离上次检查时间不足检查间隔，则跳过
        if self.last_check_time and (now - self.last_check_time).total_seconds() < self.check_interval:
            return self.proxies
        
        logger.info("开始检查代理可用性...")
        self.last_check_time = now
        
        available_proxies = []
        
        for proxy in self.proxies:
            if proxy is None:
                # None表示直连，始终可用
                available_proxies.append(None)
                continue
            
            try:
                # 构建代理字典
                proxies = {
                    "http": proxy,
                    "https": proxy
                }
                
                # 发送测试请求
                response = requests.get(test_url, proxies=proxies, timeout=timeout)
                
                if response.status_code == 200:
                    logger.info(f"代理 {proxy} 可用")
                    available_proxies.append(proxy)
                    self.mark_success(proxy)
                else:
                    logger.warning(f"代理 {proxy} 返回状态码 {response.status_code}")
                    self.mark_failure(proxy)
            
            except Exception as e:
                logger.warning(f"代理 {proxy} 不可用: {str(e)}")
                self.mark_failure(proxy)
        
        # 更新代理池
        self.proxies = available_proxies if available_proxies else [None]
        
        logger.info(f"代理可用性检查完成，可用代理数: {len(self.proxies)}")
        return self.proxies
    
    def get_proxy_stats(self):
        """
        获取代理统计信息
        
        Returns:
            dict: 代理统计信息
        """
        stats = {}
        
        for proxy, proxy_stat in self.proxy_stats.items():
            success = proxy_stat["success"]
            failure = proxy_stat["failure"]
            total = success + failure
            
            if total > 0:
                success_rate = success / total
            else:
                success_rate = 0
            
            stats[proxy] = {
                "success": success,
                "failure": failure,
                "total": total,
                "success_rate": success_rate,
                "last_used": proxy_stat.get("last_used")
            }
        
        return stats
    
    def get_best_proxies(self, count=3):
        """
        获取最佳代理
        
        Args:
            count (int): 返回代理数量
            
        Returns:
            list: 最佳代理列表
        """
        if not self.proxy_stats:
            return self.proxies[:count]
        
        # 计算每个代理的得分
        proxy_scores = []
        
        for proxy in self.proxies:
            if proxy is None:
                # 直连的得分固定为0.5
                proxy_scores.append((proxy, 0.5))
                continue
            
            stats = self.proxy_stats.get(proxy, {"success": 0, "failure": 0})
            success = stats["success"]
            failure = stats["failure"]
            
            # 计算得分，成功率越高，得分越高
            if success + failure == 0:
                score = 0.5  # 未使用过的代理给予中等得分
            else:
                success_rate = success / (success + failure)
                score = success_rate
            
            proxy_scores.append((proxy, score))
        
        # 按得分降序排序
        proxy_scores.sort(key=lambda x: x[1], reverse=True)
        
        # 返回得分最高的count个代理
        return [proxy for proxy, _ in proxy_scores[:count]]


class ProxyRequests:
    """代理请求类，封装requests库，自动使用代理池中的代理"""
    
    def __init__(self, proxy_pool, max_retries=3, retry_delay=2):
        """
        初始化代理请求
        
        Args:
            proxy_pool (ProxyPool): 代理池
            max_retries (int): 最大重试次数
            retry_delay (int): 重试延迟，单位为秒
        """
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
    
    def request(self, method, url, **kwargs):
        """
        发送请求
        
        Args:
            method (str): 请求方法，如 "GET", "POST" 等
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        retries = 0
        last_error = None
        
        while retries <= self.max_retries:
            # 获取代理
            proxy = self.proxy_pool.get_proxy()
            
            try:
                # 设置代理
                if proxy:
                    kwargs["proxies"] = {
                        "http": proxy,
                        "https": proxy
                    }
                
                # 发送请求
                response = self.session.request(method, url, **kwargs)
                
                # 检查响应状态
                if response.status_code == 200:
                    # 请求成功，标记代理成功
                    self.proxy_pool.mark_success(proxy)
                    return response
                elif response.status_code == 403 or response.status_code == 429:
                    # 请求被拒绝，可能是IP被限制，标记代理失败
                    logger.warning(f"请求被拒绝，状态码: {response.status_code}，尝试切换代理")
                    self.proxy_pool.mark_failure(proxy)
                    retries += 1
                else:
                    # 其他错误，标记代理失败
                    logger.warning(f"请求失败，状态码: {response.status_code}")
                    self.proxy_pool.mark_failure(proxy)
                    retries += 1
            
            except Exception as e:
                # 请求异常，标记代理失败
                logger.warning(f"请求异常: {str(e)}")
                self.proxy_pool.mark_failure(proxy)
                last_error = e
                retries += 1
            
            # 重试前等待
            if retries <= self.max_retries:
                delay = self.retry_delay * (2 ** (retries - 1))  # 指数退避
                time.sleep(delay)
        
        # 达到最大重试次数，抛出异常
        if last_error:
            raise last_error
        else:
            raise Exception(f"请求失败，达到最大重试次数 {self.max_retries}")
    
    def get(self, url, **kwargs):
        """
        发送GET请求
        
        Args:
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.request("GET", url, **kwargs)
    
    def post(self, url, **kwargs):
        """
        发送POST请求
        
        Args:
            url (str): 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.request("POST", url, **kwargs)


# 测试代码
if __name__ == "__main__":
    # 创建代理池
    pool = ProxyPool([
        "http://127.0.0.1:8080",  # 示例代理，实际使用时需替换为真实代理
        "http://127.0.0.1:8081"
    ])
    
    # 检查代理可用性
    pool.check_proxies()
    
    # 创建代理请求
    proxy_requests = ProxyRequests(pool)
    
    # 测试请求
    try:
        response = proxy_requests.get("http://www.baidu.com", timeout=5)
        print(f"请求成功，状态码: {response.status_code}")
    except Exception as e:
        print(f"请求失败: {str(e)}")
    
    # 查看代理统计信息
    stats = pool.get_proxy_stats()
    print("\n代理统计信息:")
    for proxy, stat in stats.items():
        print(f"{proxy}: 成功 {stat['success']}，失败 {stat['failure']}，成功率 {stat['success_rate']:.2f}")
