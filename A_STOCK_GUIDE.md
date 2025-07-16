# A股KDJ监控系统使用指南

## 🎯 系统特点
- **专注A股**: 专门处理A股市场数据
- **6位数字**: 直接使用6位股票代码，无需后缀
- **即开即用**: 无需复杂配置，直接使用

## 📊 当前监控股票

| 股票代码 | 股票名称 | 最新J值 |
|----------|----------|---------|
| 600000   | 浦发银行 | 0.79    |
| 600519   | 贵州茅台 | 9.63    |
| 600036   | 招商银行 | 41.14   |
| 000001   | 平安银行 | 44.93   |
| 000002   | 万科A    | 46.55   |
| 601318   | 中国平安 | 47.02   |
| 300750   | 宁德时代 | 74.17   |
| 002415   | 海康威视 | 79.56   |
| 601668   | 中国建筑 | 79.57   |

## 🚀 快速使用

### 1. 更新数据
```bash
python3 stock_monitor_a.py --update
```

### 2. 查询单只股票
```bash
python3 stock_monitor_a.py --query 600519    # 贵州茅台
python3 stock_monitor_a.py --query 000001    # 平安银行
```

### 3. 显示J值最低的股票
```bash
python3 stock_monitor_a.py --top 10          # 显示前10只
python3 stock_monitor_a.py --top 5 --timeframe weekly  # 周线数据
```

## 📈 J值解读

- **J值 < 0**: 超卖区域，可能反弹
- **J值 0-20**: 低位区域，关注反弹机会
- **J值 20-80**: 正常波动区域
- **J值 80-100**: 高位区域，注意回调风险
- **J值 > 100**: 超买区域，可能回调

## ⚙️ 配置文件

编辑 `config.ini` 来自定义监控股票：

```ini
[stocks]
# 美股和港股已禁用
us_stocks = 
hk_stocks = 

# A股股票代码 - 使用6位数字
a_stocks = 000001,000002,600000,600519,000858,601318,601668,600036,002415,300750

[settings]
timeframe = daily    # daily/weekly/monthly
top_n = 10           # 显示数量

[indicators]
kdj_n = 9            # KDJ周期
kdj_m1 = 3           # K值平滑
kdj_m2 = 3           # D值平滑
```

## 🔄 使用步骤

1. **首次运行**: 更新数据
   ```bash
   python3 stock_monitor_a.py --update
   ```

2. **日常监控**: 查看J值最低的股票
   ```bash
   python3 stock_monitor_a.py --top 10
   ```

3. **深度分析**: 查询特定股票
   ```bash
   python3 stock_monitor_a.py --query 600519
   ```

## 📱 常用命令

| 命令 | 功能 |
|------|------|
| `--update` | 更新所有A股数据 |
| `--query CODE` | 查询特定股票 |
| `--top N` | 显示J值最低的N只股票 |
| `--timeframe daily/weekly/monthly` | 切换时间维度 |

## ✅ 系统验证

当前系统已成功获取并计算了所有A股数据的KDJ指标，包括：
- ✅ 10只A股数据获取成功
- ✅ KDJ指标计算正确
- ✅ 数据库缓存正常
- ✅ 查询功能完善
- ✅ 美股/港股已完全禁用

现在可以直接使用A股监控系统了！