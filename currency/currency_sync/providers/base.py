"""
汇率提供方基类模块。
"""

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Dict, Optional, Any
import os

from currency_sync.utils.logger import setup_logger

logger = setup_logger('provider')

class ExchangeRateProvider(ABC):
    """汇率数据提供方抽象基类"""
    
    def __init__(self):
        """初始化提供方"""
        self.name = self.__class__.__name__
        self.base_currency = os.getenv('BASE_CURRENCY', 'USD')
        logger.info(f"初始化提供方: {self.name}")
    
    @abstractmethod
    def fetch_rates(self, target_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """
        获取汇率数据
        
        Args:
            target_date: 目标日期，如果为None则获取最新汇率
        
        Returns:
            Optional[Dict[str, Any]]: 汇率数据，格式为:
            {
                'currencies': {'EUR': 0.85, 'JPY': 110.2, ...},
                'data_updated_at': datetime对象,
                'data_provider': '提供方名称'
            }
            如果获取失败则返回None
        """
        pass
    
    def get_timestamp(self) -> datetime:
        """获取当前时间戳"""
        return datetime.now()
    
    def __str__(self) -> str:
        return f"{self.name} (base: {self.base_currency})"