"""
ExchangeRate-API 汇率提供方模块。
"""

import os
import requests
from datetime import datetime, date
from typing import Dict, Optional, Any

from currency_sync.providers.base import ExchangeRateProvider
from currency_sync.utils.logger import setup_logger

logger = setup_logger('exchangerate_api_provider')

class ExchangeRateApiProvider(ExchangeRateProvider):
    """ExchangeRate-API 汇率提供方"""
    
    def __init__(self):
        super().__init__()
        # 最新汇率API
        self.latest_api_url = "https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"
        # 历史汇率API
        self.history_api_url = "https://v6.exchangerate-api.com/v6/{api_key}/history/{base_currency}/{year}/{month}/{day}"
        self.api_key = os.getenv('EXCHANGERATE_API_KEY')
    
    def fetch_rates(self, target_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """
        获取 ExchangeRate-API 汇率数据
        
        Args:
            target_date: 目标日期，如果为None则获取最新汇率
        """
        try:
            if target_date:
                return self._fetch_historical_rates(target_date)
            else:
                return self._fetch_latest_rates()
        except Exception as e:
            logger.error(f"ExchangeRate-API 获取汇率失败: {str(e)}")
            return None
    
    def _fetch_latest_rates(self) -> Optional[Dict[str, Any]]:
        """获取最新汇率数据"""
        logger.info(f"从 ExchangeRate-API 获取最新汇率数据，基础货币: {self.base_currency}")
        
        # 构建 URL
        url = self.latest_api_url.format(
            api_key=self.api_key,
            base_currency=self.base_currency
        )
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.debug(f"ExchangeRate-API 响应数据: {data}")
        
        # 检查响应状态
        if data.get('result') != 'success':
            error_type = data.get('error-type', '未知错误')
            logger.error(f"ExchangeRate-API 返回错误: {error_type}")
            return None
        
        # 获取汇率数据
        currencies = data.get('conversion_rates', {})
        
        # 获取时间戳
        time_last_update = data.get('time_last_update_unix')
        if time_last_update:
            updated_at = datetime.fromtimestamp(time_last_update)
        else:
            updated_at = self.get_timestamp()
            
        return {
            'currencies': currencies,
            'data_updated_at': updated_at,
            'data_provider': 'ExchangeRate-API'
        }
    
    def _fetch_historical_rates(self, target_date: date) -> Optional[Dict[str, Any]]:
        """
        获取历史汇率数据
        
        Args:
            target_date: 目标日期
        """
        logger.info(f"从 ExchangeRate-API 获取 {target_date.isoformat()} 的历史汇率数据，基础货币: {self.base_currency}")
        
        # 构建 URL
        url = self.history_api_url.format(
            api_key=self.api_key,
            base_currency=self.base_currency,
            year=target_date.year,
            month=target_date.month,
            day=target_date.day
        )
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # 检查响应状态
        if data.get('result') != 'success':
            error_type = data.get('error-type', '未知错误')
            logger.error(f"ExchangeRate-API 返回错误: {error_type}")
            return None
        
        # 获取汇率数据
        # 注意：历史API可能返回不同的数据结构
        conversion_rates = data.get('conversion_rates', {})
        
        # 创建日期时间对象
        updated_at = datetime.combine(target_date, datetime.min.time())
            
        return {
            'currencies': conversion_rates,
            'data_updated_at': updated_at,
            'data_provider': 'ExchangeRate-API'
        }