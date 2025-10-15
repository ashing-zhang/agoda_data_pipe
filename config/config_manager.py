"""
配置管理器模块
负责加载和管理配置文件
"""

import yaml
import os
from pathlib import Path
import logging
from typing import Optional, Dict, Any


class ConfigManager:
    """
    配置管理器，负责加载和管理配置文件
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path(__file__).parent / "config.yml"
        self._config_cache: Optional[Dict[str, Any]] = None
    
    def load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            dict: 配置字典
        """
        if self._config_cache is None:
            try:
                with open(self.config_path, 'r', encoding='utf-8') as file:
                    self._config_cache = yaml.safe_load(file)
                    logging.info(f"配置文件加载成功: {self.config_path}")
            except FileNotFoundError:
                logging.error(f"配置文件未找到: {self.config_path}")
                raise
            except yaml.YAMLError as e:
                logging.error(f"配置文件格式错误: {e}")
                raise
        return self._config_cache
    
    def get_table_name(self) -> str:
        """
        获取表名配置
        
        Returns:
            str: 表名
        """
        config = self.load_config()
        return config['database']['table_name']
    
    def get_log_level(self) -> str:
        """
        获取日志级别配置
        
        Returns:
            str: 日志级别
        """
        config = self.load_config()
        return config.get('app', {}).get('log_level', 'INFO')
    
    def get_batch_size(self) -> int:
        """
        获取批处理大小配置
        
        Returns:
            int: 批处理大小
        """
        config = self.load_config()
        return config.get('app', {}).get('batch_size', 100)
    
    def get_connection_pool_config(self) -> Dict[str, Any]:
        """
        获取连接池配置
        
        Returns:
            Dict[str, Any]: 连接池配置字典
        """
        config = self.load_config()
        return config.get('app', {}).get('connection_pool', {
            'min_connections': 2,
            'max_connections': 10,
            'connection_timeout': 30,
            'idle_timeout': 300
        })
    
    def get_threading_config(self) -> Dict[str, Any]:
        """
        获取多线程配置
        
        Returns:
            Dict[str, Any]: 多线程配置字典
        """
        config = self.load_config()
        return config.get('app', {}).get('threading', {
            'max_workers': 4,
            'chunk_size': 1000,
            'enable_threading': False
        })
    
    def is_threading_enabled(self) -> bool:
        """
        检查是否启用多线程
        
        Returns:
            bool: 是否启用多线程
        """
        threading_config = self.get_threading_config()
        return threading_config.get('enable_threading', False)