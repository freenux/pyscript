"""
Currencyapi 汇率提供方模块。
"""

import os
import requests
from datetime import datetime, date
from typing import Dict, Optional, Any

from currency_sync.providers.base import ExchangeRateProvider
from currency_sync.utils.logger import setup_logger

logger = setup_logger('currencyapi_provider')

class CurrencyApiProvider(ExchangeRateProvider):
    """Currencyapi 汇率提供方"""
    
    def __init__(self):
        super().__init__()
        self.api_url = "https://api.currencyapi.com/v3/latest"
        self.historical_api_url = "https://api.currencyapi.com/v3/historical"
        self.api_key = os.getenv('CURRENCYAPI_KEY')
    
    def fetch_rates(self, target_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """
        获取 Currencyapi 汇率数据
        
        Args:
            target_date: 目标日期，如果为None则获取最新汇率
        """
        try:
            if target_date:
                return self._fetch_historical_rates(target_date)
            else:
                return self._fetch_latest_rates()
        except Exception as e:
            logger.error(f"Currencyapi 获取汇率失败: {str(e)}")
            return None
    
    def _fetch_latest_rates(self) -> Optional[Dict[str, Any]]:
        """获取最新汇率数据"""
        logger.info(f"从 Currencyapi 获取最新汇率数据，基础货币: {self.base_currency}")
        
        params = {
            'base_currency': self.base_currency,
            'apikey': self.api_key
        }
        
        response = requests.get(self.api_url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.debug(f"Currencyapi 响应数据: {data}")
        
        # 解析 Currencyapi 特有的数据格式
        currencies = {}
        if 'data' in data:
            for currency_code, currency_data in data['data'].items():
                currencies[currency_code] = currency_data.get('value', 0.0)
        
        # 获取时间戳
        last_updated = data.get('meta', {}).get('last_updated_at')
        if last_updated:
            try:
                # Currencyapi 返回的格式可能是 "2023-04-14T23:30:00Z"
                updated_at = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            except ValueError:
                updated_at = self.get_timestamp()
        else:
            updated_at = self.get_timestamp()
            
        return {
            'currencies': currencies,
            'data_updated_at': updated_at,
            'data_provider': 'Currencyapi'
        }
    
    def _fetch_historical_rates(self, target_date: date) -> Optional[Dict[str, Any]]:
        """
        获取历史汇率数据
        
        Args:
            target_date: 目标日期
        """
        logger.info(f"从 Currencyapi 获取 {target_date.isoformat()} 的历史汇率数据，基础货币: {self.base_currency}")
        
        params = {
            'base_currency': self.base_currency,
            'apikey': self.api_key,
            'date': target_date.isoformat()
        }
        response = requests.get(self.historical_api_url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # 解析 Currencyapi 特有的数据格式
        currencies = {}
        if 'data' in data:
            for currency_code, currency_data in data['data'].items():
                currencies[currency_code] = currency_data.get('value', 0.0)
        
        # 创建日期时间对象
        updated_at = datetime.combine(target_date, datetime.min.time())
            
        return {
            'currencies': currencies,
            'data_updated_at': updated_at,
            'data_provider': 'Currencyapi'
        }