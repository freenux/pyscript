"""
日志工具模块。
"""

import logging
import os
from pathlib import Path

def setup_logger(name='currency_sync'):
    """配置并返回日志记录器"""
    # 获取项目根目录
    project_root = Path(__file__).parent.parent.parent
    log_file = project_root / 'logs' / 'currency_sync.log'
    
    # 确保日志目录存在
    log_file.parent.mkdir(exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 避免重复添加处理器
    if not logger.handlers:
        # 文件处理器
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger