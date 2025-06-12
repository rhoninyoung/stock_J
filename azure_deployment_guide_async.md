#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Azure Function部署指南 - 异步优化版
"""

# Azure Function部署配置

## 1. 项目结构

```
project_root/
├── async_data_fetcher.py     # 异步数据获取模块
├── async_kdj_calculator.py   # 异步KDJ计算模块
├── async_stock_selector.py   # 异步股票筛选模块
├── async_email_sender.py     # 异步邮件发送模块
├── async_main.py             # 异步主控模块
├── function_app.py           # Azure Function入口点
├── config.json               # 配置文件
└── requirements.txt          # 依赖列表
```

## 2. function_app.py 内容

```python
import azure.functions as func
import logging
import os
import sys
import datetime
import asyncio

# 添加当前目录到路径，以便导入自定义模块
sys.path.append(os.path.dirname(__file__))

# 导入异步主控模块
from async_main import AsyncStockKDJApp

app = func.FunctionApp()

@app.function_name(name="StockKDJFunction")
@app.schedule(schedule="0 0 15 * * 1-5", arg_name="mytimer")
async def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Function入口点，定时触发执行A股KDJ指标J值筛选与邮件推送
    
    Args:
        mytimer: Azure Function定时触发器
    """
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # 获取配置文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'config.json')
    
    # 创建应用
    app = AsyncStockKDJApp(config_file)
    
    # 运行应用
    success = await app.run_async()
    
    # 记录结果
    if success:
        logging.info("A股KDJ指标J值筛选与邮件推送成功")
    else:
        logging.error("A股KDJ指标J值筛选与邮件推送失败")
```

## 3. requirements.txt 内容

```
azure-functions
akshare>=1.16.0
pandas>=1.3.0
numpy>=1.20.0
aiohttp>=3.8.0
```

## 4. 部署步骤

### 4.1 准备环境

1. 安装Azure CLI和Azure Functions Core Tools：

```bash
# 安装Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# 安装Azure Functions Core Tools
npm install -g azure-functions-core-tools@4 --unsafe-perm true
```

2. 登录Azure：

```bash
az login
```

### 4.2 创建Azure资源

1. 创建资源组：

```bash
az group create --name StockKDJResourceGroup --location eastasia
```

2. 创建存储账户：

```bash
az storage account create --name stockkdjstorage --location eastasia --resource-group StockKDJResourceGroup --sku Standard_LRS
```

3. 创建Function App：

```bash
az functionapp create --resource-group StockKDJResourceGroup --consumption-plan-location eastasia --runtime python --runtime-version 3.9 --functions-version 4 --name stockkdj-function --storage-account stockkdjstorage --os-type linux
```

### 4.3 配置项目

1. 初始化本地Function项目：

```bash
mkdir stockkdj-function
cd stockkdj-function
func init --python
```

2. 复制所有Python模块和配置文件到项目目录：

```bash
cp /path/to/async_*.py .
cp /path/to/config.json .
```

3. 创建function_app.py文件，内容如上述示例。

4. 更新requirements.txt，添加所需依赖。

### 4.4 配置Azure Function应用设置

1. 设置应用设置（可选，如果有敏感信息如邮箱密码）：

```bash
az functionapp config appsettings set --name stockkdj-function --resource-group StockKDJResourceGroup --settings "EMAIL_PASSWORD=your_email_password"
```

2. 在代码中读取环境变量（如果使用了应用设置）：

```python
# 在async_main.py中添加
import os

# 从环境变量读取敏感信息
email_password = os.environ.get('EMAIL_PASSWORD', config['email']['password'])
```

### 4.5 部署Function

```bash
func azure functionapp publish stockkdj-function
```

### 4.6 验证部署

1. 查看Function日志：

```bash
func azure functionapp logstream stockkdj-function
```

2. 手动触发Function（用于测试）：

```bash
az functionapp function trigger --name StockKDJFunction --resource-group StockKDJResourceGroup --function-name stockkdj-function
```

## 5. 性能优化配置

### 5.1 调整并发设置

在config.json中配置适合Azure Function的并发参数：

```json
{
  "data_fetcher": {
    "max_workers": 16,
    "use_multiprocessing": false,
    "request_delay": [1, 3]
  },
  "stock": {
    "batch_size": 20,
    "use_cache": true
  }
}
```

### 5.2 配置缓存目录

Azure Function的临时存储路径为`/tmp`，需要在配置中指定：

```json
{
  "data_fetcher": {
    "cache_dir": "/tmp/stock_cache"
  }
}
```

### 5.3 调整函数超时时间

对于大量数据处理，可能需要增加函数超时时间（默认为5分钟）：

```bash
az functionapp config appsettings set --name stockkdj-function --resource-group StockKDJResourceGroup --settings "FUNCTIONS_EXTENSION_VERSION=~4" "WEBSITE_RUN_FROM_PACKAGE=1" "FUNCTIONS_WORKER_PROCESS_COUNT=4" "WEBSITE_MAX_DYNAMIC_APPLICATION_SCALE_OUT=3" "FUNCTIONS_WORKER_RUNTIME=python" "PYTHON_THREADPOOL_THREAD_COUNT=32"
```

## 6. 故障排除

### 6.1 常见问题

1. **导入模块错误**：确保所有模块都在同一目录，并且已添加到sys.path。

2. **权限问题**：确保Azure Function有足够权限访问存储和发送邮件。

3. **超时错误**：增加函数超时时间或优化代码减少执行时间。

4. **内存限制**：减少批处理大小或优化内存使用。

### 6.2 日志查看

```bash
# 查看实时日志
func azure functionapp logstream stockkdj-function

# 下载日志
az webapp log download --resource-group StockKDJResourceGroup --name stockkdj-function
```

### 6.3 监控性能

在Azure门户中，可以查看Function的执行时间、内存使用等指标，根据这些指标进一步优化配置。

## 7. 安全性考虑

1. 不要在代码中硬编码敏感信息，使用应用设置。

2. 定期更新依赖包，避免安全漏洞。

3. 限制Function的网络访问范围。

4. 使用托管身份进行Azure服务之间的认证。

## 8. 成本优化

1. 使用Consumption计划，按实际执行付费。

2. 优化执行时间，减少计算资源使用。

3. 合理设置缓存过期时间，减少不必要的数据获取。

4. 监控使用情况，及时调整资源配置。
