#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
汇率数据同步主程序。
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date
from typing import Optional, List
from dotenv import load_dotenv

from currency_sync.utils.logger import setup_logger
from currency_sync.db.database import ExchangeRateDB
from currency_sync.providers import get_providers, get_provider_by_name

# 加载环境变量
load_dotenv()

logger = setup_logger('main')

# 设置 requests 的日志记录
requests_log = logging.getLogger("requests.packages.urllib3")

class ExchangeRateSynchronizer:
    """汇率数据同步器"""

    def __init__(self, target_date: Optional[date] = None, provider_name: Optional[str] = None, debug: bool = False):
        """
        初始化同步器

        Args:
            target_date: 目标日期，如果为None则获取最新汇率
            provider_name: 指定的提供方名称，如果为None则使用所有提供方
            debug: 是否启用HTTP请求调试模式
        """
        # 获取提供方
        if provider_name:
            provider = get_provider_by_name(provider_name)
            self.providers = [provider] if provider else []
            if not self.providers:
                logger.error(f"未找到名为 {provider_name} 的提供方")
        else:
            self.providers = get_providers()

        self.db = ExchangeRateDB()
        self.target_date = target_date
        self.debug = debug
        
        # 如果启用调试模式，设置 requests 的日志级别
        if self.debug:
            requests_log.setLevel(logging.DEBUG)
            requests_log.propagate = True
            
            # 添加控制台处理器
            if not requests_log.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
                requests_log.addHandler(handler)

    def sync(self) -> bool:
        """
        尝试从各个提供方获取数据并保存

        Returns:
            bool: 是否成功获取并保存数据
        """
        if not self.providers:
            logger.error("没有可用的提供方")
            return False

        date_str = self.target_date.isoformat() if self.target_date else "最新"
        logger.info(f"开始同步{date_str}汇率数据")

        for provider in self.providers:
            logger.info(f"尝试从{provider.name}获取{date_str}汇率数据")

            # 获取汇率数据，传递调试标志
            rate_data = provider.fetch_rates(self.target_date)

            # 如果获取成功，保存到数据库并返回
            if rate_data:
                success = self.db.save_rates(
                    provider.base_currency,
                    rate_data['currencies'],
                    rate_data['data_provider'],
                    rate_data['data_updated_at']
                )

                if success:
                    logger.info(f"成功从{provider.name}获取并保存{date_str}汇率数据")
                    return True

            logger.warning(f"{provider.name}获取{date_str}汇率失败，尝试下一个提供方")

        logger.error(f"所有提供方都获取{date_str}汇率失败")
        return False

def parse_date(date_str: str) -> date:
    """
    解析日期字符串

    Args:
        date_str: 日期字符串，格式为YYYY-MM-DD

    Returns:
        date: 解析后的日期对象

    Raises:
        ValueError: 如果日期格式不正确
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"日期格式不正确: {date_str}，请使用YYYY-MM-DD格式")

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="汇率数据同步工具")
    parser.add_argument(
        "--date", "-d",
        help="指定获取历史汇率的日期 (格式: YYYY-MM-DD)，默认获取最新汇率",
        type=parse_date,
        default=None
    )
    parser.add_argument(
        "--provider", "-p",
        help="指定使用的汇率提供方，默认按优先级尝试所有提供方",
        type=str,
        default=None
    )
    parser.add_argument(
        "--verbose", "-v",
        help="启用HTTP请求调试模式，显示请求和响应的详细信息",
        action="store_true"
    )
    args = parser.parse_args()

    try:
        synchronizer = ExchangeRateSynchronizer(args.date, args.provider, args.verbose)
        success = synchronizer.sync()

        if success:
            logger.info("汇率数据同步完成")
            return 0
        else:
            logger.error("汇率数据同步失败")
            return 1
    except Exception as e:
        logger.error(f"同步过程中发生错误: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
