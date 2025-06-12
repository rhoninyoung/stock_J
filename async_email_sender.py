#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步邮件发送模块：使用异步IO优化邮件发送
"""

import smtplib
import pandas as pd
import os
import time
import logging
import asyncio
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import concurrent.futures
import io

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('async_email_sender')

class AsyncEmailSender:
    """异步邮件发送类"""
    
    def __init__(self, config):
        """
        初始化邮件发送器
        
        Args:
            config (dict): 邮件配置，包含smtp_server, smtp_port, sender, password, use_ssl等
        """
        self.smtp_server = config.get('smtp_server')
        self.smtp_port = config.get('smtp_port', 465)
        self.sender = config.get('sender')
        self.password = config.get('password')
        self.use_ssl = config.get('use_ssl', True)
        
        # 性能指标
        self.performance_metrics = {
            'total_emails': 0,
            'email_times': []
        }
    
    async def _generate_email_content_async(self, results_dict):
        """
        异步生成邮件HTML内容
        
        Args:
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            
        Returns:
            str: HTML格式的邮件内容
        """
        # 使用线程池执行HTML生成（CPU密集型任务）
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            html_content = await loop.run_in_executor(
                pool, self._generate_email_content, results_dict
            )
        return html_content
    
    def _generate_email_content(self, results_dict):
        """
        生成邮件HTML内容
        
        Args:
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            
        Returns:
            str: HTML格式的邮件内容
        """
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                h2 {{ color: #333366; }}
                .date {{ color: #666; font-size: 0.9em; }}
            </style>
        </head>
        <body>
            <h1>A股KDJ指标J值最低股票筛选结果</h1>
            <p class="date">生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
        
        # 添加各时间维度的表格
        dimensions = {
            'daily': '日线',
            'weekly': '周线',
            'monthly': '月线'
        }
        
        for dimension, df in results_dict.items():
            if df.empty:
                continue
                
            html += f"""
            <h2>{dimensions.get(dimension, dimension)}数据 - J值最低的{len(df)}支股票</h2>
            <table>
                <tr>
                    <th>排名</th>
                    <th>股票代码</th>
                    <th>股票名称</th>
                    <th>J值</th>
                    <th>日期</th>
                </tr>
            """
            
            for i, (_, row) in enumerate(df.iterrows()):
                html += f"""
                <tr>
                    <td>{i+1}</td>
                    <td>{row['code']}</td>
                    <td>{row['name']}</td>
                    <td>{row['j_value']:.2f}</td>
                    <td>{row['date']}</td>
                </tr>
                """
            
            html += "</table>"
        
        html += """
            <p>注意：本邮件由系统自动生成，请勿直接回复。</p>
            <p>免责声明：本邮件内容仅供参考，不构成任何投资建议。投资有风险，入市需谨慎。</p>
        </body>
        </html>
        """
        
        return html
    
    async def _generate_csv_attachment_async(self, results_dict):
        """
        异步生成CSV附件
        
        Args:
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            
        Returns:
            dict: 以文件名为键，文件内容为值的字典
        """
        # 使用线程池执行CSV生成（CPU密集型任务）
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            attachments = await loop.run_in_executor(
                pool, self._generate_csv_attachment, results_dict
            )
        return attachments
    
    def _generate_csv_attachment(self, results_dict):
        """
        生成CSV附件
        
        Args:
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            
        Returns:
            dict: 以文件名为键，文件内容为值的字典
        """
        attachments = {}
        
        dimensions = {
            'daily': '日线',
            'weekly': '周线',
            'monthly': '月线'
        }
        
        for dimension, df in results_dict.items():
            if df.empty:
                continue
                
            # 创建CSV内容
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_content = csv_buffer.getvalue()
            
            # 添加到附件字典
            filename = f"A股KDJ指标J值最低股票_{dimensions.get(dimension, dimension)}_{datetime.now().strftime('%Y%m%d')}.csv"
            attachments[filename] = csv_content
        
        return attachments
    
    async def send_email_async(self, receiver, subject, results_dict, include_csv=True):
        """
        异步发送邮件
        
        Args:
            receiver (str): 接收者邮箱
            subject (str): 邮件主题
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            include_csv (bool): 是否包含CSV附件，默认为True
            
        Returns:
            bool: 是否发送成功
        """
        start_time = time.time()
        
        try:
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = receiver
            msg['Subject'] = subject
            
            # 异步生成HTML内容
            html_content = await self._generate_email_content_async(results_dict)
            msg.attach(MIMEText(html_content, 'html'))
            
            # 异步生成CSV附件
            if include_csv:
                attachments = await self._generate_csv_attachment_async(results_dict)
                
                for filename, content in attachments.items():
                    attachment = MIMEApplication(content.encode('utf-8-sig'))
                    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
                    msg.attach(attachment)
            
            # 使用线程池执行SMTP发送（IO密集型任务）
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as pool:
                success = await loop.run_in_executor(
                    pool, self._send_smtp, msg, receiver
                )
            
            # 记录性能指标
            email_time = time.time() - start_time
            self.performance_metrics['email_times'].append(email_time)
            self.performance_metrics['total_emails'] += 1
            
            logger.info(f"邮件发送{'成功' if success else '失败'}，耗时: {email_time:.2f}秒")
            return success
            
        except Exception as e:
            logger.error(f"发送邮件时出错: {str(e)}")
            return False
    
    def _send_smtp(self, msg, receiver):
        """
        通过SMTP发送邮件
        
        Args:
            msg (MIMEMultipart): 邮件对象
            receiver (str): 接收者邮箱
            
        Returns:
            bool: 是否发送成功
        """
        try:
            # 连接SMTP服务器
            if self.use_ssl:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.starttls()
            
            # 登录
            server.login(self.sender, self.password)
            
            # 发送邮件
            server.sendmail(self.sender, receiver, msg.as_string())
            
            # 关闭连接
            server.quit()
            
            logger.info(f"邮件已发送至 {receiver}")
            return True
            
        except Exception as e:
            logger.error(f"SMTP发送邮件时出错: {str(e)}")
            return False
    
    def send_email(self, receiver, subject, results_dict, include_csv=True):
        """
        发送邮件（同步版本，用于兼容现有代码）
        
        Args:
            receiver (str): 接收者邮箱
            subject (str): 邮件主题
            results_dict (dict): 以时间维度为键，筛选结果DataFrame为值的字典
            include_csv (bool): 是否包含CSV附件，默认为True
            
        Returns:
            bool: 是否发送成功
        """
        # 创建事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # 运行异步发送邮件
            success = loop.run_until_complete(
                self.send_email_async(receiver, subject, results_dict, include_csv)
            )
            return success
        finally:
            # 关闭事件循环
            loop.close()
    
    def get_performance_metrics(self):
        """
        获取性能指标
        
        Returns:
            dict: 性能指标
        """
        metrics = self.performance_metrics.copy()
        
        # 计算平均邮件发送时间
        if metrics['email_times']:
            metrics['avg_email_time'] = sum(metrics['email_times']) / len(metrics['email_times'])
        else:
            metrics['avg_email_time'] = 0
        
        return metrics

# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    test_results = {
        'daily': pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['平安银行', '万科A', '中国石化'],
            'j_value': [10.5, 15.2, 18.7],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['daily', 'daily', 'daily']
        }),
        'weekly': pd.DataFrame({
            'code': ['000001', '000004', '000005'],
            'name': ['平安银行', '国农科技', '世纪星源'],
            'j_value': [12.3, 16.8, 19.2],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['weekly', 'weekly', 'weekly']
        }),
        'monthly': pd.DataFrame({
            'code': ['000002', '000006', '000007'],
            'name': ['万科A', '深振业A', '全新好'],
            'j_value': [11.8, 17.5, 20.1],
            'date': ['2025-06-10', '2025-06-10', '2025-06-10'],
            'dimension': ['monthly', 'monthly', 'monthly']
        })
    }
    
    # 创建测试配置
    test_config = {
        'smtp_server': 'smtp.example.com',
        'smtp_port': 465,
        'sender': 'test@example.com',
        'password': 'test_password',
        'use_ssl': True
    }
    
    # 创建邮件发送器
    sender = AsyncEmailSender(test_config)
    
    # 测试生成邮件内容
    logger.info("=== 测试生成邮件内容 ===")
    html_content = sender._generate_email_content(test_results)
    logger.info(f"生成的HTML内容长度: {len(html_content)} 字符")
    
    # 测试生成CSV附件
    logger.info("\n=== 测试生成CSV附件 ===")
    attachments = sender._generate_csv_attachment(test_results)
    logger.info(f"生成的附件数量: {len(attachments)}")
    for filename, content in attachments.items():
        logger.info(f"附件: {filename}, 大小: {len(content)} 字节")
    
    # 注意：不实际发送邮件，避免测试时发送真实邮件
    logger.info("\n=== 邮件发送测试已跳过 ===")
    logger.info("在实际环境中，可以使用以下代码发送邮件:")
    logger.info("success = sender.send_email('receiver@example.com', 'A股KDJ指标J值最低股票筛选结果', test_results)")
