#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Azure Function部署指南（优化版）：如何将优化后的A股KDJ指标J值筛选系统部署到Azure Functions
"""

# Azure Function部署指南（优化版）

## 1. 准备工作

### 1.1 安装Azure CLI和Azure Functions Core Tools

首先，需要安装Azure CLI和Azure Functions Core Tools：

```bash
# 安装Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# 安装Azure Functions Core Tools
npm install -g azure-functions-core-tools@4
```

### 1.2 登录Azure

```bash
az login
```

## 2. 创建Azure Function项目

### 2.1 创建项目目录结构

```bash
mkdir -p azure_function_deploy
cd azure_function_deploy

# 创建Azure Function项目
func init --worker-runtime python --docker
```

### 2.2 创建定时触发器函数

```bash
func new --name StockKDJFunction --template "Timer trigger"
```

## 3. 准备部署文件

### 3.1 修改function_app.py

Azure Functions现在使用function_app.py作为入口点，而不是传统的__init__.py。编辑`function_app.py`文件：

```python
import azure.functions as func
import logging
import datetime
import os
import sys

# 添加当前目录到路径，以便导入自定义模块
sys.path.append(os.path.dirname(__file__))

# 导入主应用
from stock_kdj_app import StockKDJApp

app = func.FunctionApp()

@app.function_name(name="StockKDJFunction")
@app.schedule(schedule="0 0 15 * * 1-5", arg_name="mytimer")
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # 获取配置文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'config.json')
    
    # 创建并运行应用
    app = StockKDJApp(config_file)
    success = app.run()
    
    # 记录结果
    if success:
        logging.info("A股KDJ指标J值筛选与邮件推送成功")
    else:
        logging.error("A股KDJ指标J值筛选与邮件推送失败")
```

### 3.2 重命名main.py为stock_kdj_app.py

为避免与Azure Functions内置的main模块冲突，需要将我们的`main.py`重命名为`stock_kdj_app.py`：

```bash
cp main.py stock_kdj_app.py
```

### 3.3 准备requirements.txt

在项目根目录创建`requirements.txt`文件，列出所有依赖：

```
akshare>=1.16.0
pandas>=1.3.0
numpy>=1.20.0
```

### 3.4 复制项目文件

将以下文件复制到Azure Function项目目录：

- `stock_kdj_app.py` (重命名后的main.py)
- `data_fetcher.py`
- `kdj_calculator.py`
- `stock_selector.py`
- `email_sender.py`
- `config.json`

## 4. 配置应用设置

### 4.1 创建本地设置文件

创建`local.settings.json`文件：

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "EMAIL_SMTP_SERVER": "smtp.example.com",
    "EMAIL_SMTP_PORT": "465",
    "EMAIL_SENDER": "your_email@example.com",
    "EMAIL_PASSWORD": "your_password",
    "EMAIL_USE_SSL": "true",
    "EMAIL_RECEIVER": "receiver@example.com"
  }
}
```

## 5. 本地测试

### 5.1 运行本地测试

```bash
func start
```

## 6. 部署到Azure

### 6.1 创建Azure Function App

```bash
# 创建资源组
az group create --name StockKDJResourceGroup --location eastasia

# 创建存储账户
az storage account create --name stockkdjstorage --location eastasia --resource-group StockKDJResourceGroup --sku Standard_LRS

# 创建Function App
az functionapp create --resource-group StockKDJResourceGroup --consumption-plan-location eastasia --runtime python --runtime-version 3.9 --functions-version 4 --name StockKDJApp --storage-account stockkdjstorage --os-type Linux
```

### 6.2 配置应用设置

```bash
# 配置邮件设置
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SMTP_SERVER=smtp.example.com"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SMTP_PORT=465"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SENDER=your_email@example.com"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_PASSWORD=your_password"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_USE_SSL=true"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_RECEIVER=receiver@example.com"
```

### 6.3 部署函数

```bash
func azure functionapp publish StockKDJApp
```

## 7. 验证部署

### 7.1 查看函数日志

```bash
func azure functionapp logstream StockKDJApp
```

### 7.2 手动触发函数

可以在Azure门户中手动触发函数，验证功能是否正常。

## 8. 监控与维护

### 8.1 设置监控告警

在Azure门户中，可以为Function App设置监控告警，以便在函数执行失败时收到通知。

### 8.2 查看执行历史

在Azure门户中，可以查看函数的执行历史和日志，以便排查问题。

## 9. 成本优化

Azure Functions采用消费计划，按实际执行次数和执行时间计费。由于本应用每天只执行一次，且执行时间较短，成本应该很低。

## 10. 安全考虑

- 敏感信息（如邮箱密码）应使用Azure Key Vault存储，而不是直接在应用设置中配置
- 考虑为Function App配置托管身份，以便安全访问其他Azure服务
- 定期更新依赖包，以修复潜在的安全漏洞

## 11. 优化版特别说明

### 11.1 缓存目录配置

优化版增加了本地缓存功能，在Azure Functions环境中，需要确保缓存目录有写入权限：

```bash
# 配置缓存目录为临时目录
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "CACHE_DIR=/tmp/stock_kdj_cache"
```

然后在代码中读取此环境变量：

```python
cache_dir = os.environ.get('CACHE_DIR', '/tmp/stock_kdj_cache')
```

### 11.2 处理存储服务连接问题

如果遇到"Unable to get table reference or create table"等存储服务连接问题，可能是Azure平台临时性的服务问题，可以尝试：

1. 等待几分钟后重试
2. 重新创建存储账户
3. 检查网络设置，确保没有防火墙阻止访问

### 11.3 处理模块导入冲突

如果遇到"ImportError: cannot import name 'StockKDJApp' from 'main'"等导入错误，确保已将main.py重命名为stock_kdj_app.py，并在function_app.py中正确导入。
