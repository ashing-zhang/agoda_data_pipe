'''
数据库配置模块
从YAML配置文件加载数据库连接配置
'''

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

# 配置日志
logger = logging.getLogger(__name__)


class DatabaseConfigError(Exception):
    """数据库配置异常"""
    pass


class DatabaseConfigLoader:
    """数据库配置加载器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器
        
        Args:
            config_path: 配置文件路径，默认为当前目录下的config.yml
        """
        if config_path is None:
            config_path = Path("config.yml")
        
        self.config_path = Path(config_path)
        self._config_cache: Optional[Dict[str, Any]] = None
        
        logger.info(f"初始化数据库配置加载器，配置文件路径: {self.config_path}")
    
    def _load_yaml_config(self) -> Dict[str, Any]:
        """加载YAML配置文件"""
        try:
            if not self.config_path.exists():
                raise DatabaseConfigError(f"配置文件不存在: {self.config_path}")
            
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                
            if not config:
                raise DatabaseConfigError("配置文件为空或格式错误")
                
            logger.debug(f"成功加载配置文件: {self.config_path}")
            return config
            
        except yaml.YAMLError as e:
            logger.error(f"YAML配置文件解析错误: {e}")
            raise DatabaseConfigError(f"YAML配置文件解析错误: {e}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise DatabaseConfigError(f"加载配置文件失败: {e}")
    
    def get_config(self, force_reload: bool = False) -> Dict[str, Any]:
        """获取配置，支持缓存"""
        if self._config_cache is None or force_reload:
            self._config_cache = self._load_yaml_config()
            logger.info("配置已加载到缓存")
        
        return self._config_cache
    
    def get_current_environment(self) -> str:
        """获取当前环境"""
        config = self.get_config()
        
        try:
            env = config['environment']['current']
            logger.info(f"当前环境: {env}")
            return env
        except KeyError:
            logger.warning("配置文件中未找到环境设置，使用默认环境: development")
            return 'development'
    
    def get_database_config(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """获取数据库配置"""
        config = self.get_config()
        
        if environment is None:
            environment = self.get_current_environment()
        
        try:
            db_config = config['database'][environment]
            logger.info(f"获取 {environment} 环境数据库配置")
            return db_config
        except KeyError:
            logger.error(f"未找到环境 '{environment}' 的数据库配置")
            raise DatabaseConfigError(f"未找到环境 '{environment}' 的数据库配置")


# 全局配置加载器实例
_config_loader = DatabaseConfigLoader()


def load_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    加载数据库配置
    
    Args:
        environment: 指定环境，如果为None则使用配置文件中的当前环境
        
    Returns:
        数据库配置字典
        
    Raises:
        DatabaseConfigError: 配置加载失败时抛出
    """
    try:
        return _config_loader.get_database_config(environment)
    except Exception as e:
        logger.error(f"加载数据库配置失败: {e}")
        raise


def get_current_environment() -> str:
    """
    获取当前环境
    
    Returns:
        当前环境名称
    """
    return _config_loader.get_current_environment()


def reload_config() -> None:
    """
    重新加载配置文件
    """
    logger.info("重新加载配置文件")
    _config_loader.get_config(force_reload=True)