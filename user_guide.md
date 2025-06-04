#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A股KDJ指标J值筛选与邮件推送系统使用说明文档
"""

# A股KDJ指标J值筛选与邮件推送系统

## 1. 系统概述

本系统用于自动获取A股当日KDJ指标中的J值，按照月/周/日三个时间维度排序，筛选出J值最低的20支股票，并通过邮件推送给用户。系统使用免费的akshare数据源，无需付费token/API，可部署在Azure等公有云上以Lambda函数的形式运行。

## 2. 系统架构

系统由以下几个核心模块组成：

1. **数据获取模块 (data_fetcher.py)**：负责从akshare获取A股股票列表和历史K线数据
2. **KDJ计算模块 (kdj_calculator.py)**：负责计算各股票的KDJ指标，特别是J值
3. **数据排序筛选模块 (stock_selector.py)**：按照日/周/月维度对J值进行排序，筛选出J值最低的20支股票
4. **邮件推送模块 (email_sender.py)**：将筛选结果通过邮件发送给用户
5. **主控模块 (main.py)**：协调各模块工作，适配云函数运行环境

## 3. 安装与配置

### 3.1 环境要求

- Python 3.8+
- 依赖包：akshare, pandas, numpy

### 3.2 安装依赖

```bash
pip install akshare pandas numpy
```

### 3.3 配置文件

系统使用`config.json`文件进行配置，主要配置项包括：

```json
{
    "email": {
        "smtp_server": "smtp.example.com",
        "smtp_port": 465,
        "sender": "your_email@example.com",
        "password": "your_password",
        "use_ssl": true,
        "receiver": "receiver@example.com"
    },
    "stock": {
        "max_stocks": null,  // 获取全部股票，设置具体数值可限制获取数量
        "exclude_st": true,  // 排除ST股票
        "exclude_new": true  // 排除上市不足一年的新股
    },
    "kdj": {
        "n": 9,  // RSV计算周期
        "m1": 3, // K值平滑因子
        "m2": 3  // D值平滑因子
    },
    "selector": {
        "top_n": 20  // 筛选数量
    }
}
```

也可以通过环境变量配置邮件相关信息：

- `EMAIL_SMTP_SERVER`: SMTP服务器地址
- `EMAIL_SMTP_PORT`: SMTP服务器端口
- `EMAIL_SENDER`: 发件人邮箱
- `EMAIL_PASSWORD`: 发件人密码
- `EMAIL_USE_SSL`: 是否使用SSL
- `EMAIL_RECEIVER`: 收件人邮箱

## 4. 本地运行

### 4.1 直接运行

```bash
python main.py
```

### 4.2 使用配置文件

```bash
python main.py --config /path/to/config.json
```

## 5. 云函数部署

系统可以部署到Azure Functions等云函数服务上，详细部署步骤请参考`azure_deployment_guide.md`文件。

## 6. 功能说明

### 6.1 数据获取

系统使用akshare获取A股股票列表和历史K线数据，支持日/周/月三个时间维度的数据获取。

### 6.2 KDJ计算

系统使用标准的KDJ计算公式：

1. RSV = (收盘价 - 最低价) / (最高价 - 最低价) * 100
2. K = 前一日K值 * (m1-1)/m1 + 当日RSV/m1
3. D = 前一日D值 * (m2-1)/m2 + 当日K值/m2
4. J = 3*K - 2*D

### 6.3 股票筛选

系统按照J值升序排序，筛选出J值最低的20支股票（数量可配置）。

### 6.4 邮件推送

系统将筛选结果通过HTML格式的邮件发送给用户，同时附带CSV格式的附件，便于用户进一步分析。

## 7. 注意事项

1. 由于使用免费的akshare数据源，数据获取速度可能较慢，尤其是在获取大量股票数据时
2. 系统默认排除ST股票，可通过配置文件修改
3. 邮件推送功能需要配置正确的SMTP服务器信息
4. 在云函数环境中，建议使用环境变量配置敏感信息，如邮箱密码
5. KDJ指标仅供参考，投资决策应结合其他指标和基本面分析

## 8. 常见问题

### 8.1 数据获取失败

可能原因：
- 网络连接问题
- akshare接口变更
- 股票代码错误

解决方法：
- 检查网络连接
- 更新akshare版本
- 检查股票代码格式

### 8.2 邮件发送失败

可能原因：
- SMTP服务器配置错误
- 邮箱密码错误
- 网络连接问题

解决方法：
- 检查SMTP服务器配置
- 检查邮箱密码
- 检查网络连接

## 9. 联系与支持

如有任何问题或建议，请联系系统开发者。
