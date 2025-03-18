"""
数据库操作模块。
"""

import os
import json
import mysql.connector
from datetime import datetime
from typing import Dict, Any, Optional

from currency_sync.utils.logger import setup_logger

logger = setup_logger('database')

class ExchangeRateDB:
    """汇率数据库操作类"""
    
    def __init__(self, config=None):
        """初始化数据库连接配置"""
        self.config = config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'currency_db'),
            'port': int(os.getenv('DB_PORT', '3306'))
        }
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """连接到数据库"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return False
    
    def close(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def save_rates(self, base_currency: str, currencies: Dict[str, float], 
                  provider: str, updated_at: datetime) -> bool:
        """保存汇率数据到数据库"""
        try:
            if not self.connect():
                return False
                
            # 将汇率数据转换为JSON字符串
            currencies_json = json.dumps(currencies)
            
            # 准备SQL语句
            sql = """
            INSERT INTO exchange_rates 
            (base_currency, currencies, data_provider, data_updated_at) 
            VALUES (%s, %s, %s, %s)
            """
            
            # 执行SQL
            self.cursor.execute(sql, (base_currency, currencies_json, provider, updated_at))
            self.conn.commit()
            
            logger.info(f"成功保存{provider}的汇率数据")
            return True
        except Exception as e:
            logger.error(f"保存汇率数据失败: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            self.close()