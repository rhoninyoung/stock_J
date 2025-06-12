#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Azure Function部署指南（代理池优化版）：如何将支持代理池的A股KDJ指标J值筛选系统部署到Azure Functions
"""

# Azure Function部署指南（代理池优化版）

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
        
        # 如果启用了代理池，输出代理统计信息
        proxy_stats = app.get_proxy_stats()
        if proxy_stats:
            logging.info("代理统计信息:")
            for proxy, stat in proxy_stats.items():
                if proxy:  # 跳过None
                    logging.info(f"{proxy}: 成功 {stat['success']}，失败 {stat['failure']}，成功率 {stat.get('success_rate', 0):.2f}")
    else:
        logging.error("A股KDJ指标J值筛选与邮件推送失败")
```

### 3.2 重命名main_with_proxy.py为stock_kdj_app.py

为避免与Azure Functions内置的main模块冲突，需要将我们的`main_with_proxy.py`重命名为`stock_kdj_app.py`：

```bash
cp main_with_proxy.py stock_kdj_app.py
```

### 3.3 准备requirements.txt

在项目根目录创建`requirements.txt`文件，列出所有依赖：

```
akshare>=1.16.0
pandas>=1.3.0
numpy>=1.20.0
requests>=2.25.0
```

### 3.4 复制项目文件

将以下文件复制到Azure Function项目目录：

- `stock_kdj_app.py` (重命名后的main_with_proxy.py)
- `data_fetcher_with_proxy.py` (重命名为data_fetcher.py)
- `proxy_pool.py`
- `kdj_calculator.py`
- `stock_selector.py`
- `email_sender.py`
- `config.json` (使用config_with_proxy.json，重命名为config.json)

## 4. 配置代理池

### 4.1 准备代理列表文件

如果您计划使用代理池功能，需要准备一个代理列表文件。创建`proxies.txt`文件，每行一个代理地址：

```
http://proxy1.example.com:8080
http://proxy2.example.com:8080
http://proxy3.example.com:8080
```

### 4.2 配置代理池选项

编辑`config.json`文件，启用代理池功能：

```json
{
    "data_fetcher": {
        "use_proxy": true,
        "proxy_list": [],
        "proxy_file": "proxies.txt"
    }
}
```

## 5. 配置应用设置

### 5.1 创建本地设置文件

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
    "EMAIL_RECEIVER": "receiver@example.com",
    "USE_PROXY": "true",
    "PROXY_FILE": "proxies.txt",
    "CACHE_DIR": "/tmp/stock_cache"
  }
}
```

## 6. 本地测试

### 6.1 运行本地测试

```bash
func start
```

## 7. 部署到Azure

### 7.1 创建Azure Function App

```bash
# 创建资源组
az group create --name StockKDJResourceGroup --location eastasia

# 创建存储账户
az storage account create --name stockkdjstorage --location eastasia --resource-group StockKDJResourceGroup --sku Standard_LRS

# 创建Function App
az functionapp create --resource-group StockKDJResourceGroup --consumption-plan-location eastasia --runtime python --runtime-version 3.9 --functions-version 4 --name StockKDJApp --storage-account stockkdjstorage --os-type Linux
```

### 7.2 配置应用设置

```bash
# 配置邮件设置
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SMTP_SERVER=smtp.example.com"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SMTP_PORT=465"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_SENDER=your_email@example.com"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_PASSWORD=your_password"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_USE_SSL=true"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "EMAIL_RECEIVER=receiver@example.com"

# 配置代理池设置
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "USE_PROXY=true"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "PROXY_FILE=proxies.txt"
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "CACHE_DIR=/tmp/stock_cache"
```

### 7.3 部署函数

```bash
func azure functionapp publish StockKDJApp
```

## 8. 验证部署

### 8.1 查看函数日志

```bash
func azure functionapp logstream StockKDJApp
```

### 8.2 手动触发函数

可以在Azure门户中手动触发函数，验证功能是否正常。

## 9. 代理池特别说明

### 9.1 代理池配置选项

代理池功能提供了多种配置选项：

1. **启用/禁用代理池**：
   - 通过`config.json`中的`data_fetcher.use_proxy`设置
   - 或通过环境变量`USE_PROXY`设置

2. **代理来源**：
   - 直接在`config.json`中的`data_fetcher.proxy_list`中指定代理列表
   - 或通过`data_fetcher.proxy_file`指定代理文件路径
   - 或通过环境变量`PROXY_FILE`指定代理文件路径

3. **代理选择策略**：
   - 轮询策略（round_robin）：按顺序使用代理列表中的代理
   - 随机策略（random）：随机选择代理
   - 加权策略（weighted）：根据成功率选择代理

### 9.2 代理池监控

代理池会自动记录每个代理的使用情况，包括成功次数、失败次数和成功率。您可以在日志中查看这些统计信息。

### 9.3 代理失效处理

代理池会自动检测代理的可用性，如果某个代理连续失败多次，会自动将其从代理池中移除。如果所有代理都不可用，系统会自动切换到直连模式。

### 9.4 代理池安全性

在Azure Functions环境中使用代理池时，需要注意以下安全事项：

1. 不要在代码中硬编码代理信息，应该使用配置文件或环境变量
2. 确保代理服务器是可信的，避免使用不安全的公共代理
3. 定期更新代理列表，移除不可用或不安全的代理

## 10. 故障排除

### 10.1 处理存储服务连接问题

如果遇到"Unable to get table reference or create table"等存储服务连接问题，可能是Azure平台临时性的服务问题，可以尝试：

1. 等待几分钟后重试
2. 重新创建存储账户
3. 检查网络设置，确保没有防火墙阻止访问

### 10.2 处理模块导入冲突

如果遇到"ImportError: cannot import name 'StockKDJApp' from 'main'"等导入错误，确保已将main_with_proxy.py重命名为stock_kdj_app.py，并在function_app.py中正确导入。

### 10.3 处理代理池问题

如果代理池功能不正常工作，可以尝试：

1. 检查代理列表是否正确配置
2. 暂时禁用代理池功能，使用直连模式测试
3. 查看日志中的代理统计信息，了解代理失败的原因

## 11. 性能优化

### 11.1 缓存优化

代理池版本增加了本地缓存功能，在Azure Functions环境中，需要确保缓存目录有写入权限：

```bash
# 配置缓存目录为临时目录
az functionapp config appsettings set --name StockKDJApp --resource-group StockKDJResourceGroup --settings "CACHE_DIR=/tmp/stock_cache"
```

### 11.2 代理池优化

为了提高代理池的效率，可以考虑以下优化：

1. 使用高质量的代理服务器
2. 适当增加代理数量，分散请求压力
3. 调整代理选择策略，优先使用成功率高的代理
4. 定期更新代理列表，移除不可用的代理

### 11.3 批处理优化

调整批处理参数可以平衡请求速度和稳定性：

```json
{
    "stock": {
        "batch_size": 20
    },
    "data_fetcher": {
        "request_delay": [1, 3]
    }
}
```

- 增大`batch_size`可以减少批次数量，加快处理速度
- 减小`request_delay`可以减少请求间隔，但可能增加被限制的风险

## 12. 总结

本指南详细介绍了如何将支持代理池的A股KDJ指标J值筛选系统部署到Azure Functions。通过代理池功能，系统可以有效避免AKShare请求过量导致的拒绝访问问题，提高数据获取的稳定性和可靠性。

代理池功能是可选的，您可以根据需要灵活开启或关闭。即使在没有代理的情况下，系统也能通过请求延迟、重试机制、分批处理和缓存等多层优化策略，保证数据获取的稳定性。
