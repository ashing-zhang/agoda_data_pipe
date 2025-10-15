'''
数据库配置使用示例
演示如何使用新的配置驱动的数据库配置系统
'''

import logging
from utils.sql_config import load_config, get_current_environment, DatabaseConfigError

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """主函数，演示配置系统的使用"""
    try:
        # 获取当前环境
        current_env = get_current_environment()
        logger.info(f"当前环境: {current_env}")
        
        # 加载当前环境的数据库配置
        db_config = load_config()
        logger.info(f"数据库配置: {db_config}")
        
        # 加载指定环境的配置
        prod_config = load_config('production')
        logger.info(f"生产环境配置: {prod_config}")
        
        dev_config = load_config('development')
        logger.info(f"开发环境配置: {dev_config}")
        
    except DatabaseConfigError as e:
        logger.error(f"配置错误: {e}")
    except Exception as e:
        logger.error(f"未知错误: {e}")


if __name__ == '__main__':
    main()