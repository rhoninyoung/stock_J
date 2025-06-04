# A股KDJ指标J值筛选系统设计文档

## 1. 整体架构

该系统将采用模块化设计，主要包含以下几个核心模块：

1. **数据获取模块**：负责从免费数据源获取A股股票列表和历史K线数据
2. **KDJ计算模块**：负责计算各股票的KDJ指标，特别是J值
3. **数据排序筛选模块**：按照日/周/月维度对J值进行排序，筛选出J值最低的20支股票
4. **邮件推送模块**：将筛选结果通过邮件发送给用户
5. **主控模块**：协调各模块工作，适配云函数运行环境

## 2. 模块详细设计

### 2.1 数据获取模块 (data_fetcher.py)

```python
# 功能：获取A股股票列表和历史K线数据
# 接口：
# - get_stock_list(): 获取所有A股股票列表
# - get_stock_data(stock_code, period, start_date, end_date): 获取指定股票的历史K线数据
#   - period可选值: daily, weekly, monthly
```

### 2.2 KDJ计算模块 (kdj_calculator.py)

```python
# 功能：计算股票的KDJ指标
# 接口：
# - calculate_kdj(stock_data, n=9, m1=3, m2=3): 计算KDJ指标
#   - n: RSV计算周期
#   - m1: K值平滑因子
#   - m2: D值平滑因子
# 返回：包含K、D、J值的DataFrame
```

### 2.3 数据排序筛选模块 (stock_selector.py)

```python
# 功能：按不同时间维度排序J值，筛选出J值最低的股票
# 接口：
# - select_lowest_j_stocks(stock_kdj_dict, time_dimension, top_n=20): 筛选J值最低的股票
#   - time_dimension可选值: daily, weekly, monthly
#   - top_n: 筛选数量
# 返回：筛选出的股票列表，包含股票代码、名称、J值等信息
```

### 2.4 邮件推送模块 (email_sender.py)

```python
# 功能：将筛选结果通过邮件发送给用户
# 接口：
# - send_email(receiver, subject, content, attachment=None): 发送邮件
#   - receiver: 接收者邮箱
#   - subject: 邮件主题
#   - content: 邮件内容
#   - attachment: 附件路径
```

### 2.5 主控模块 (main.py)

```python
# 功能：协调各模块工作，适配云函数运行环境
# 主要流程：
# 1. 获取A股股票列表
# 2. 获取各股票历史K线数据
# 3. 计算各股票KDJ指标
# 4. 按日/周/月维度排序，筛选J值最低的20支股票
# 5. 生成报告
# 6. 发送邮件
```

## 3. 配置文件设计 (config.py)

```python
# 功能：存储系统配置信息
# 配置项：
# - EMAIL_CONFIG: 邮件服务器配置
# - STOCK_FILTER: 股票筛选条件
# - KDJ_PARAMS: KDJ计算参数
```

## 4. 云函数适配设计

为了适配Azure Functions等无状态云函数环境，需要考虑以下几点：

1. **依赖管理**：使用requirements.txt管理依赖
2. **配置管理**：使用环境变量或配置文件管理敏感信息
3. **触发器设计**：使用定时触发器实现定期执行
4. **状态管理**：无状态设计，每次执行独立完成

## 5. 文件结构

```
/
├── requirements.txt          # 依赖列表
├── function.json             # Azure Function配置
├── host.json                 # Azure Function主机配置
├── main.py                   # 主入口，适配Azure Function
├── config.py                 # 配置文件
├── data_fetcher.py           # 数据获取模块
├── kdj_calculator.py         # KDJ计算模块
├── stock_selector.py         # 股票筛选模块
└── email_sender.py           # 邮件发送模块
```

## 6. 执行流程

1. 云函数定时触发执行main.py
2. main.py调用data_fetcher获取股票列表和K线数据
3. 对每支股票调用kdj_calculator计算KDJ指标
4. 调用stock_selector按三个时间维度分别筛选J值最低的20支股票
5. 生成报告内容
6. 调用email_sender发送邮件报告
7. 执行完成，等待下次触发
