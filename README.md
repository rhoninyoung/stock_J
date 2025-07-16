# A股KDJ指标监控系统

一个专门用于A股市场的KDJ技术指标监控系统，支持日线、周线、月线数据获取与分析。

## 功能特点

- **A股专用**: 专门处理A股市场，使用6位数字股票代码
- **多时间维度**: 支持日线(daily)、周线(weekly)、月线(monthly)数据
- **KDJ指标计算**: 自动计算KDJ技术指标
- **数据存储**: 使用SQLite数据库存储历史数据
- **J值筛选**: 可筛选J值最低的股票，辅助超卖信号识别
- **批量更新**: 支持批量更新股票数据
- **灵活配置**: 通过配置文件自定义监控参数

## 安装指南

### 系统要求

- Python 3.7 或更高版本
- pip 包管理器

### 安装步骤

1. **克隆或下载项目**
   ```bash
   git clone <repository-url>
   cd stock
   ```

2. **创建虚拟环境** (推荐)
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate     # Windows
   ```

3. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

## 使用方法

### 基本用法

#### 1. 更新股票数据
更新所有配置的A股数据：
```bash
python stock_monitor_a.py --update
```

指定更新特定时间维度：
```bash
python stock_monitor_a.py --update --update-timeframes daily weekly
```

#### 2. 查询特定股票
查询特定股票的KDJ数据：
```bash
python stock_monitor_a.py --query 600519
```

#### 3. 查看J值最低的股票
显示J值最低的前10只股票：
```bash
python stock_monitor_a.py --top 10
```

指定时间维度：
```bash
python stock_monitor_a.py --top 10 --timeframe weekly
```

### 命令行参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--config` | 配置文件路径 | `--config config.ini` |
| `--update` | 更新所有股票数据 | `--update` |
| `--update-timeframes` | 指定更新的时间维度 | `--update-timeframes daily weekly` |
| `--query` | 查询特定股票 | `--query 600519` |
| `--top` | 显示J值最低的前N只股票 | `--top 20` |
| `--timeframe` | 时间维度选择 | `--timeframe monthly` |

## 配置文件说明

### config.ini 结构

```ini
[settings]
# 时间维度配置: daily, weekly, monthly
timeframe = weekly
# 显示J值最低的前N只股票
top_n = 10
# 数据库文件路径
db_path = stock_data.db
# 更新数据时的时间维度（逗号分隔）
update_timeframes = daily,weekly,monthly

[stocks]
# A股代码列表 - 直接使用6位数字
a_stocks = 600519,601398,601939,601857,601288,600036,601318,601988,601012,600900

[indicators]
# KDJ指标参数
kdj_n = 9
kdj_m1 = 3
kdj_m2 = 3
```

### 股票代码格式

- **A股**: 使用6位数字代码，如 `600519`、`000858`、`300750`
- 系统会自动识别沪市(6开头)和深市(0/3开头)股票

## 数据说明

### KDJ指标解释

- **K值**: 快速随机指标，反映短期价格波动
- **D值**: 慢速随机指标，反映中期价格趋势
- **J值**: 领先指标，J值<0通常表示超卖，J值>100通常表示超买

### 数据库结构

系统使用SQLite数据库`stock_data.db`存储数据，主要包含`kdj_data`表：

| 字段 | 说明 |
|------|------|
| symbol | 股票代码 |
| date | 交易日期 |
| timeframe | 时间维度(daily/weekly/monthly) |
| k_value | K值 |
| d_value | D值 |
| j_value | J值 |
| volume | 交易量 |
| price_close | 收盘价 |

## 使用示例

### 场景1：日常监控
```bash
# 更新日线数据
python stock_monitor_a.py --update --update-timeframes daily

# 查看J值最低的20只股票
python stock_monitor_a.py --top 20 --timeframe daily
```

### 场景2：波段分析
```bash
# 更新周线数据
python stock_monitor_a.py --update --update-timeframes weekly

# 查看周线J值最低的股票
python stock_monitor_a.py --top 10 --timeframe weekly
```

### 场景3：长期投资
```bash
# 更新月线数据
python stock_monitor_a.py --update --update-timeframes monthly

# 查询特定股票月线数据
python stock_monitor_a.py --query 600519 --timeframe monthly
```

## 注意事项

1. **数据延迟**: 使用yfinance获取数据，可能有15-20分钟延迟
2. **网络要求**: 需要稳定的网络连接获取股票数据
3. **API限制**: 避免过于频繁的请求，系统已内置延迟机制
4. **股票代码**: 确保使用正确的6位数字A股代码

## 常见问题

### Q: 为什么某些股票没有数据？
A: 可能原因：
- 股票代码不正确或已退市
- 网络连接问题
- yfinance数据源限制

### Q: 如何添加更多股票？
A: 编辑`config.ini`文件，在`[stocks]`部分的`a_stocks`中添加股票代码，用逗号分隔。

### Q: 数据多久更新一次？
A: 建议每日运行更新命令，历史数据会增量更新。

## 技术支持

如有问题或建议，请提交Issue或联系项目维护者。

## 免责声明

本项目仅供学习和研究使用，不构成投资建议。投资有风险，入市需谨慎。
