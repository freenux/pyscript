"""
汇率提供方注册模块。
"""

from typing import List, Optional
from currency_sync.providers.base import ExchangeRateProvider
from currency_sync.providers.currencyapi import CurrencyApiProvider
from currency_sync.providers.exchangerate_api import ExchangeRateApiProvider

from currency_sync.utils.logger import setup_logger

logger = setup_logger('providers')

# 按优先级顺序注册提供方
def get_providers() -> List[ExchangeRateProvider]:
    """获取所有注册的汇率提供方，按优先级排序"""
    return [
        CurrencyApiProvider(),
        ExchangeRateApiProvider(),
    ]

def get_provider_by_name(name: str) -> Optional[ExchangeRateProvider]:
    """
    根据名称获取特定的汇率提供方
    
    Args:
        name: 提供方名称，不区分大小写
        
    Returns:
        Optional[ExchangeRateProvider]: 找到的提供方实例，如果未找到则返回None
    """
    name = name.lower()
    
    # 提供方名称映射
    provider_map = {
        'currencyapi': CurrencyApiProvider,
        'currencyapiprovider': CurrencyApiProvider,
        'exchangerate': ExchangeRateApiProvider,
        'exchangerateapi': ExchangeRateApiProvider,
        'exchangerateapiprovider': ExchangeRateApiProvider,
    }
    
    provider_class = provider_map.get(name)
    if provider_class:
        return provider_class()
    
    logger.warning(f"未找到名为 {name} 的提供方")
    logger.info(f"可用的提供方: {', '.join(provider_map.keys())}")
    return None

# 导出所有提供方类，方便直接导入
__all__ = [
    'ExchangeRateProvider',
    'CurrencyApiProvider',
    'ExchangeRateApiProvider',
    'get_providers',
    'get_provider_by_name'
]