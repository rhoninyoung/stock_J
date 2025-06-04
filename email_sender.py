#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
邮件推送模块：将筛选结果通过邮件发送给用户
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pandas as pd
import logging
from datetime import datetime
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('email_sender')

class EmailSender:
    """邮件发送类"""
    
    def __init__(self, config=None):
        """
        初始化邮件发送器
        
        Args:
            config (dict): 邮件配置，包含smtp_server, smtp_port, sender, password等
        """
        # 默认配置
        self.default_config = {
            'smtp_server': 'smtp.163.com',
            'smtp_port': 465,
            'sender': 'your_email@163.com',
            'password': 'your_password',
            'use_ssl': True
        }
        
        # 如果提供了配置，则更新默认配置
        if config:
            self.config = {**self.default_config, **config}
        else:
            # 尝试从环境变量或配置文件加载
            self.config = self._load_config()
    
    def _load_config(self):
        """
        从环境变量或配置文件加载邮件配置
        
        Returns:
            dict: 邮件配置
        """
        config = self.default_config.copy()
        
        # 尝试从环境变量加载
        if os.environ.get('EMAIL_SMTP_SERVER'):
            config['smtp_server'] = os.environ.get('EMAIL_SMTP_SERVER')
        if os.environ.get('EMAIL_SMTP_PORT'):
            config['smtp_port'] = int(os.environ.get('EMAIL_SMTP_PORT'))
        if os.environ.get('EMAIL_SENDER'):
            config['sender'] = os.environ.get('EMAIL_SENDER')
        if os.environ.get('EMAIL_PASSWORD'):
            config['password'] = os.environ.get('EMAIL_PASSWORD')
        if os.environ.get('EMAIL_USE_SSL'):
            config['use_ssl'] = os.environ.get('EMAIL_USE_SSL').lower() == 'true'
        
        # 尝试从配置文件加载
        config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f).get('email', {})
                    config.update(file_config)
            except Exception as e:
                logger.error(f"加载配置文件失败: {str(e)}")
        
        return config
    
    def _generate_html_table(self, df):
        """
        将DataFrame转换为HTML表格
        
        Args:
            df (pandas.DataFrame): 数据
            
        Returns:
            str: HTML表格
        """
        if df.empty:
            return "<p>无数据</p>"
        
        # 设置表格样式
        table_style = """
        <style>
        table {
            border-collapse: collapse;
            width: 100%;
            font-family: Arial, sans-serif;
        }
        th, td {
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        </style>
        """
        
        # 生成HTML表格
        html_table = df.to_html(index=False)
        
        return table_style + html_table
    
    def _generate_email_content(self, results_dict):
        """
        生成邮件内容
        
        Args:
            results_dict (dict): 包含三个时间维度筛选结果的字典
            
        Returns:
            str: HTML格式的邮件内容
        """
        today = datetime.now().strftime('%Y-%m-%d')
        
        html_content = f"""
        <html>
        <head>
            <title>A股KDJ指标J值最低股票筛选结果</title>
        </head>
        <body>
            <h1>A股KDJ指标J值最低股票筛选结果</h1>
            <p>报告生成日期: {today}</p>
            
            <h2>日线KDJ指标J值最低的20支股票</h2>
            {self._generate_html_table(results_dict.get('daily', pd.DataFrame()))}
            
            <h2>周线KDJ指标J值最低的20支股票</h2>
            {self._generate_html_table(results_dict.get('weekly', pd.DataFrame()))}
            
            <h2>月线KDJ指标J值最低的20支股票</h2>
            {self._generate_html_table(results_dict.get('monthly', pd.DataFrame()))}
            
            <p>注: J值为KDJ指标中的J值，J值较低可能意味着股票超卖，但请结合其他指标和基本面分析进行投资决策。</p>
            <p>本邮件由自动化系统生成，请勿直接回复。</p>
        </body>
        </html>
        """
        
        return html_content
    
    def _generate_csv_attachment(self, results_dict):
        """
        生成CSV附件
        
        Args:
            results_dict (dict): 包含三个时间维度筛选结果的字典
            
        Returns:
            dict: 包含CSV文件名和内容的字典
        """
        today = datetime.now().strftime('%Y%m%d')
        attachments = {}
        
        for dimension, df in results_dict.items():
            if not df.empty:
                filename = f"A股KDJ指标J值最低股票_{dimension}_{today}.csv"
                csv_content = df.to_csv(index=False)
                attachments[filename] = csv_content
        
        return attachments
    
    def send_email(self, receiver, subject, results_dict, include_csv=True):
        """
        发送邮件
        
        Args:
            receiver (str): 接收者邮箱
            subject (str): 邮件主题
            results_dict (dict): 包含三个时间维度筛选结果的字典
            include_csv (bool): 是否包含CSV附件
            
        Returns:
            bool: 是否发送成功
        """
        try:
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.config['sender']
            msg['To'] = receiver
            msg['Subject'] = subject
            
            # 添加HTML内容
            html_content = self._generate_email_content(results_dict)
            msg.attach(MIMEText(html_content, 'html'))
            
            # 添加CSV附件
            if include_csv:
                attachments = self._generate_csv_attachment(results_dict)
                for filename, content in attachments.items():
                    attachment = MIMEApplication(content)
                    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
                    msg.attach(attachment)
            
            # 连接SMTP服务器并发送邮件
            if self.config['use_ssl']:
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'])
            else:
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
                server.starttls()
            
            server.login(self.config['sender'], self.config['password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"邮件已成功发送至 {receiver}")
            return True
        
        except Exception as e:
            logger.error(f"发送邮件失败: {str(e)}")
            return False


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    test_data = {
        'daily': pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['平安银行', '万科A', '中国石化'],
            'j_value': [10.5, 15.2, 18.7],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
        }),
        'weekly': pd.DataFrame({
            'code': ['000001', '000004', '000005'],
            'name': ['平安银行', '国农科技', '世纪星源'],
            'j_value': [12.3, 16.8, 19.2],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
        }),
        'monthly': pd.DataFrame({
            'code': ['000002', '000006', '000007'],
            'name': ['万科A', '深振业A', '全新好'],
            'j_value': [11.8, 17.5, 20.1],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01']
        })
    }
    
    # 创建邮件发送器（使用测试配置）
    test_config = {
        'smtp_server': 'smtp.example.com',
        'smtp_port': 465,
        'sender': 'test@example.com',
        'password': 'test_password',
        'use_ssl': True
    }
    sender = EmailSender(test_config)
    
    # 生成邮件内容（仅用于测试，不实际发送）
    html_content = sender._generate_email_content(test_data)
    print("生成的HTML邮件内容示例:")
    print(html_content[:500] + "...")
    
    # 生成CSV附件（仅用于测试）
    attachments = sender._generate_csv_attachment(test_data)
    print("\n生成的CSV附件:")
    for filename in attachments.keys():
        print(f"- {filename}")
    
    print("\n注意: 实际发送邮件功能需要配置正确的SMTP服务器信息")
