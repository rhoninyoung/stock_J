#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股KDJ指标监控系统
专门处理A股市场，使用6位数字股票代码
"""

import argparse
import configparser
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional


import pandas as pd
import numpy as np
import yfinance as yf


class StockDatabase:
    """SQLite数据库管理类"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS kdj_data (
                symbol TEXT,
                date DATE,
                timeframe TEXT,
                k_value REAL,
                d_value REAL,
                j_value REAL,
                volume INTEGER,
                price_close REAL,
                PRIMARY KEY (symbol, date, timeframe)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_kdj_symbol_timeframe 
            ON kdj_data(symbol, timeframe)
        ''')
        
        conn.commit()
        conn.close()
    
    def save_kdj_data(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """保存KDJ数据到数据库"""
        conn = sqlite3.connect(self.db_path)
        
        for _, row in df.iterrows():
            cursor = conn.cursor()
            try:
                date_str = str(row['Date'])
                if hasattr(row['Date'], 'strftime'):
                    date_str = row['Date'].strftime('%Y-%m-%d')
                
                cursor.execute('''
                    INSERT OR REPLACE INTO kdj_data 
                    (symbol, date, timeframe, k_value, d_value, j_value, volume, price_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    date_str,
                    timeframe,
                    float(row['K']),
                    float(row['D']),
                    float(row['J']),
                    int(row['Volume']),
                    float(row['Close'])
                ))
            except Exception as e:
                logging.error(f"保存数据失败: {e}")
        
        conn.commit()
        conn.close()
    
    def get_lowest_j_stocks(self, timeframe: str, limit: int) -> List[Dict]:
        """获取J值最低的股票列表"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT symbol, date, j_value, volume, price_close
            FROM kdj_data
            WHERE timeframe = ?
            AND date = (
                SELECT MAX(date) 
                FROM kdj_data k2 
                WHERE k2.symbol = kdj_data.symbol 
                AND k2.timeframe = kdj_data.timeframe
            )
            ORDER BY j_value ASC
            LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=(timeframe, limit))
        conn.close()
        
        return df.to_dict('records')

    def get_latest_kdj_data(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """获取最新的KDJ数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM kdj_data 
            WHERE symbol = ? AND timeframe = ?
            ORDER BY date DESC LIMIT 1
        ''', (symbol, timeframe))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'symbol': row[0],
                'date': row[1],
                'timeframe': row[2],
                'k': row[3],
                'd': row[4],
                'j': row[5],
                'volume': row[6],
                'close': row[7]
            }
        return None

class AShareDataFetcher:
    """A股数据获取器"""
    
    def __init__(self):
        pass
    
    def fetch_a_stock(self, symbol: str, period: str, timeframe: str = 'daily') -> pd.DataFrame:
        """获取A股数据，支持daily/weekly/monthly"""
        try:
            # 根据股票代码确定yfinance格式
            if symbol.startswith('6'):
                # 沪市股票
                yf_symbol = f"{symbol}.SS"
            elif symbol.startswith(('0', '3')):
                # 深市股票
                yf_symbol = f"{symbol}.SZ"
            else:
                yf_symbol = symbol
            
            logging.info(f"获取A股 {symbol} ({yf_symbol}) {timeframe} 数据...")
            
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period=period)
            
            if df.empty:
                logging.warning(f"A股 {symbol} 无数据")
                return pd.DataFrame()
            
            # 根据时间维度进行数据重采样
            if timeframe == 'weekly':
                # 重采样为周线数据（使用周五收盘作为周线数据）
                df = df.resample('W-FRI').agg({
                    'Open': 'first',
                    'High': 'max', 
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'
                }).dropna()
            elif timeframe == 'monthly':
                # 重采样为月线数据（使用月末最后一个交易日）
                df = df.resample('ME').agg({
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min', 
                    'Close': 'last',
                    'Volume': 'sum'
                }).dropna()
            
            df = df.reset_index()
            df = df.rename(columns={
                'Date': 'Date',
                'Open': 'Open',
                'High': 'High',
                'Low': 'Low',
                'Close': 'Close',
                'Volume': 'Volume'
            })
            
            return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
        except Exception as e:
            logging.error(f"获取A股 {symbol} 数据失败: {e}")
            return pd.DataFrame()

class KDJCalculator:
    """KDJ指标计算类"""
    
    def __init__(self, n: int = 9, m1: int = 3, m2: int = 3):
        self.n = n
        self.m1 = m1
        self.m2 = m2
    
    def calculate_kdj(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算KDJ指标"""
        if df.empty:
            return df
        
        df = df.sort_values('Date').reset_index(drop=True)
        
        # 计算RSV (Raw Stochastic Value)
        low_list = df['Low'].rolling(window=self.n, min_periods=1).min()
        high_list = df['High'].rolling(window=self.n, min_periods=1).max()
        
        # 避免除以零
        rsv = (df['Close'] - low_list) / (high_list - low_list) * 100
        rsv = rsv.fillna(50)
        
        # 计算K值
        k_values = rsv.ewm(com=self.m1 - 1, adjust=False).mean()
        
        # 计算D值
        d_values = k_values.ewm(com=self.m2 - 1, adjust=False).mean()
        
        # 计算J值
        j_values = 3 * k_values - 2 * d_values
        
        # 添加到DataFrame
        df['K'] = k_values
        df['D'] = d_values
        df['J'] = j_values
        
        return df

class AShareMonitor:
    """A股监控主类"""
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        self.db = StockDatabase(self.config.get('settings', 'db_path', fallback='a_stock_data.db'))
        self.fetcher = AShareDataFetcher()
        self.calculator = KDJCalculator(
            int(self.config.get('indicators', 'kdj_n', fallback='9')),
            int(self.config.get('indicators', 'kdj_m1', fallback='3')),
            int(self.config.get('indicators', 'kdj_m2', fallback='3'))
        )
        
        self.timeframe = self.config.get('settings', 'timeframe', fallback='daily')
        self.top_n = int(self.config.get('settings', 'top_n', fallback='10'))
        
        # 从配置读取要更新的时间维度
        update_timeframes_str = self.config.get('settings', 'update_timeframes', fallback='daily,weekly,monthly')
        if update_timeframes_str:
            self.update_timeframes = [tf.strip() for tf in update_timeframes_str.split(',') if tf.strip()]
        else:
            self.update_timeframes = ['daily', 'weekly', 'monthly']
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def get_a_stocks(self) -> List[str]:
        """获取配置的A股股票代码"""
        a_stocks = self.config.get('stocks', 'a_stocks', fallback='')
        if not a_stocks or not a_stocks.strip():
            # 默认A股列表
            return ['000001', '000002', '600000', '600519', '000858']
        
        return [s.strip() for s in a_stocks.split(',') if s.strip()]
    
    def update_stock_data(self, symbol: str, timeframe: str = None) -> bool:
        """更新单个A股数据"""
        if timeframe is None:
            timeframe = self.timeframe
            
        try:
            # 根据时间维度设置获取数据的时间段
            if timeframe == 'daily':
                period = '1y'  # 增加日线数据周期以确保准确性
            elif timeframe == 'weekly':
                period = '3y'  # 增加周线数据周期
            elif timeframe == 'monthly':
                period = '5y'  # 月线保持5年
            else:
                period = '1y'
                
            df = self.fetcher.fetch_a_stock(symbol, period, timeframe)
            
            if df.empty:
                logging.warning(f"无法获取 {symbol} {timeframe} 的数据")
                return False
            
            # 计算KDJ指标
            df = self.calculator.calculate_kdj(df)
            if df.empty:
                logging.warning(f"无法计算 {symbol} {timeframe} 的KDJ指标")
                return False
            
            # 保存到数据库
            self.db.save_kdj_data(symbol, timeframe, df)
            logging.info(f"成功更新 {symbol} {timeframe} 的数据")
            return True
            
        except Exception as e:
            logging.error(f"更新 {symbol} {timeframe} 数据时出错: {e}")
            return False
    
    def update_all_stocks(self, timeframes: List[str] = None):
        """更新所有A股数据，支持指定时间维度"""
        stocks = self.get_a_stocks()
        if not stocks:
            logging.warning("没有配置任何A股股票")
            return
        
        if timeframes is None:
            timeframes = self.update_timeframes
        
        # 验证时间维度是否有效
        valid_timeframes = ['daily', 'weekly', 'monthly']
        timeframes = [tf for tf in timeframes if tf in valid_timeframes]
        
        if not timeframes:
            logging.warning("未指定有效的时间维度")
            return
        
        for timeframe in timeframes:
            logging.info(f"开始更新 {len(stocks)} 只A股的{timeframe}数据")
            
            success_count = 0
            for i, stock in enumerate(stocks, 1):
                logging.info(f"[{i}/{len(stocks)}] 更新 {stock} {timeframe}...")
                if self.update_stock_data(stock, timeframe):
                    success_count += 1
                time.sleep(0.5)  # 避免过于频繁
            
            logging.info(f"{timeframe}数据更新完成，成功更新 {success_count}/{len(stocks)} 只A股")
    
    def get_lowest_j_stocks(self, limit: int = None) -> List[Dict]:
        """获取J值最低的A股股票列表"""
        if limit is None:
            limit = self.top_n
        return self.db.get_lowest_j_stocks(self.timeframe, limit)
    
    def query_stock(self, symbol: str) -> Optional[Dict]:
        """查询特定A股的KDJ数据"""
        return self.db.get_latest_kdj_data(symbol.upper(), self.timeframe)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股KDJ指标监控系统')
    parser.add_argument('--config', default='config.ini', help='配置文件路径')
    parser.add_argument('--update', action='store_true', help='更新A股数据')
    parser.add_argument('--update-timeframes', nargs='*', 
                       choices=['daily', 'weekly', 'monthly'],
                       help='指定要更新的时间维度（可选，默认为全部）')
    parser.add_argument('--query', help='查询特定A股的KDJ数据')
    parser.add_argument('--top', type=int, help='显示J值最低的前N只A股')
    parser.add_argument('--timeframe', choices=['daily', 'weekly', 'monthly'], 
                       help='时间维度(daily/weekly/monthly)')
    
    args = parser.parse_args()
    
    try:
        monitor = AShareMonitor(args.config)
        
        if args.timeframe:
            monitor.timeframe = args.timeframe
        
        if args.update or args.update_timeframes is not None:
            if args.update_timeframes:
                monitor.update_all_stocks(args.update_timeframes)
            else:
                monitor.update_all_stocks()
        
        if args.query:
            result = monitor.query_stock(args.query)
            if result:
                print(f"\nA股代码: {result['symbol']}")
                print(f"日期: {result['date']}")
                print(f"时间维度: {result['timeframe']}")
                print(f"J值: {result['j']:.2f}")
                print(f"K值: {result['k']:.2f}")
                print(f"D值: {result['d']:.2f}")
                print(f"交易量: {result['volume']:,}")
                print(f"收盘价: ¥{result['close']:.2f}")
            else:
                print(f"未找到 {args.query} 的数据")
        
        if args.top:
            stocks = monitor.get_lowest_j_stocks(args.top)
            if stocks:
                print(f"\nJ值最低的 {len(stocks)} 只A股 ({monitor.timeframe}):")
                print("-" * 60)
                print(f"{'股票代码':<15} {'J值':<10} {'交易量':<15} {'收盘价':<10}")
                print("-" * 60)
                for stock in stocks:
                    print(f"{stock['symbol']:<15} {stock['j_value']:<10.2f} "
                          f"{stock['volume']:<15,} ¥{stock['price_close']:<10.2f}")
            else:
                print("没有找到A股数据")
        
        if not any([args.update, args.query, args.top]):
            parser.print_help()
    
    except Exception as e:
        logging.error(f"程序执行错误: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()