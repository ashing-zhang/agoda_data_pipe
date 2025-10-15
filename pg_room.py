'''
    python -m pg_room
'''
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import pool
from pathlib import Path
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.sql_config import load_config as load_db_config
from config.config_manager import ConfigManager


class ConnectionPoolManager:
    """
    连接池管理器，负责管理数据库连接池
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self._db_config: Optional[Dict[str, Any]] = None
        self._connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self._lock = threading.Lock()
    
    def _get_db_config(self) -> Dict[str, Any]:
        """
        获取数据库配置
        
        Returns:
            dict: 数据库配置字典
        """
        if self._db_config is None:
            self._db_config = load_db_config()
            logging.info(f"数据库配置加载成功: {self._db_config['host']}:{self._db_config['port']}")
        return self._db_config
    
    def _initialize_pool(self) -> None:
        """
        初始化连接池
        """
        if self._connection_pool is None:
            with self._lock:
                if self._connection_pool is None:
                    db_config = self._get_db_config()
                    pool_config = self.config_manager.get_connection_pool_config()
                    
                    self._connection_pool = pool.ThreadedConnectionPool(
                        minconn=pool_config['min_connections'],
                        maxconn=pool_config['max_connections'],
                        **db_config
                    )
                    logging.info(f"连接池初始化成功: min={pool_config['min_connections']}, max={pool_config['max_connections']}")
    
    @contextmanager
    def get_connection(self):
        """
        从连接池获取数据库连接的上下文管理器
        
        Yields:
            psycopg2.extensions.connection: 数据库连接
        """
        self._initialize_pool()
        conn = None
        try:
            conn = self._connection_pool.getconn()
            logging.debug("从连接池获取连接成功")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
                logging.error(f"数据库操作失败，已回滚: {str(e)}")
            raise
        finally:
            if conn:
                self._connection_pool.putconn(conn)
                logging.debug("连接已归还到连接池")
    
    def close_pool(self) -> None:
        """
        关闭连接池
        """
        if self._connection_pool:
            self._connection_pool.closeall()
            logging.info("连接池已关闭")


class DatabaseManager:
    """
    数据库管理器，负责数据库连接和操作（单连接模式）
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self._db_config: Optional[Dict[str, Any]] = None
    
    def _get_db_config(self) -> Dict[str, Any]:
        """
        获取数据库配置
        
        Returns:
            dict: 数据库配置字典
        """
        if self._db_config is None:
            self._db_config = load_db_config()
            logging.info(f"数据库配置加载成功: {self._db_config['host']}:{self._db_config['port']}")
        return self._db_config
    
    @contextmanager
    def get_connection(self):
        """
        获取数据库连接的上下文管理器
        
        Yields:
            psycopg2.extensions.connection: 数据库连接
        """
        conn = None
        try:
            db_config = self._get_db_config()
            conn = psycopg2.connect(**db_config)
            logging.info("数据库连接建立成功")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
                logging.error(f"数据库操作失败，已回滚: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                logging.info("数据库连接已关闭")


class TableManager:
    """
    表管理器，负责表的创建和维护
    """
    
    def __init__(self, db_manager: DatabaseManager, config_manager: ConfigManager):
        self.db_manager = db_manager
        self.config_manager = config_manager
    
    def create_table(self, table_name: Optional[str] = None) -> bool:
        """
        创建agoda源数据表
        
        Args:
            table_name (str): 表名，如果为None则从配置文件读取
            
        Returns:
            bool: 创建是否成功
        """
        if table_name is None:
            table_name = self.config_manager.get_table_name()
            logging.info(f"从配置文件读取表名: {table_name}")
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # 创建表的SQL语句（PostgreSQL语法）
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        room_id VARCHAR(500),
                        check_in_date DATE,
                        room_name VARCHAR(500),
                        room_enname VARCHAR(500),
                        area VARCHAR(100),
                        capacity VARCHAR(100),
                        bed_type VARCHAR(200),
                        smoking VARCHAR(50),
                        price DECIMAL(10,2),
                        breakfast VARCHAR(500),
                        cancel VARCHAR(500),
                        currency VARCHAR(10) DEFAULT 'CNY',
                        hotel_name VARCHAR(500),
                        defaultname VARCHAR(500),
                        distance_m DECIMAL(10,7),
                        rating DECIMAL(3,2),
                        longitude DECIMAL(10,7),
                        latitude DECIMAL(10,7),
                        hop_hotel_name VARCHAR(500),
                        hop_hotel_id VARCHAR(500),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                
                # 添加列注释的SQL语句（PostgreSQL语法）
                comment_sqls = [
                    f"COMMENT ON COLUMN {table_name}.id IS '主键ID'",
                    f"COMMENT ON COLUMN {table_name}.room_id IS '房型ID'",
                    f"COMMENT ON COLUMN {table_name}.check_in_date IS '入住日期'",
                    f"COMMENT ON COLUMN {table_name}.room_name IS '房型名称'",
                    f"COMMENT ON COLUMN {table_name}.room_enname IS '房型英文名称'",
                    f"COMMENT ON COLUMN {table_name}.area IS '面积'",
                    f"COMMENT ON COLUMN {table_name}.capacity IS '入住人数'",
                    f"COMMENT ON COLUMN {table_name}.bed_type IS '床型'",
                    f"COMMENT ON COLUMN {table_name}.smoking IS '是否禁烟'",
                    f"COMMENT ON COLUMN {table_name}.price IS '价格'",
                    f"COMMENT ON COLUMN {table_name}.breakfast IS '是否含早'",
                    f"COMMENT ON COLUMN {table_name}.cancel IS '是否可取消'",
                    f"COMMENT ON COLUMN {table_name}.currency IS '币种'",
                    f"COMMENT ON COLUMN {table_name}.hotel_name IS '酒店名称'",
                    f"COMMENT ON COLUMN {table_name}.defaultname IS '默认名称'",
                    f"COMMENT ON COLUMN {table_name}.distance_m IS '距离（米）'",
                    f"COMMENT ON COLUMN {table_name}.rating IS '评分'",
                    f"COMMENT ON COLUMN {table_name}.longitude IS '经度'",
                    f"COMMENT ON COLUMN {table_name}.latitude IS '纬度'",
                    f"COMMENT ON COLUMN {table_name}.hop_hotel_name IS 'HOP酒店名称'",
                    f"COMMENT ON COLUMN {table_name}.hop_hotel_id IS 'HOP酒店ID'",
                    f"COMMENT ON COLUMN {table_name}.created_at IS '创建时间'",
                    f"COMMENT ON COLUMN {table_name}.updated_at IS '更新时间'"
                ]
                
                # 执行创建表语句
                cursor.execute(create_table_sql)
                logging.info(f"表 {table_name} 创建SQL执行成功")
                
                # 执行添加注释语句
                for comment_sql in comment_sqls:
                    cursor.execute(comment_sql)
                
                conn.commit()
                logging.info(f"表 {table_name} 创建成功")
                print(f"表 {table_name} 创建成功")
                return True
                
        except Exception as e:
            logging.error(f"创建表失败: {str(e)}")
            print(f"创建表失败: {str(e)}")
            return False


class ThreadedDataManager:
    """
    多线程数据管理器，负责多线程数据插入和处理
    """
    
    def __init__(self, connection_pool_manager: ConnectionPoolManager, config_manager: ConfigManager):
        self.connection_pool_manager = connection_pool_manager
        self.config_manager = config_manager
    
    def _chunk_data(self, data: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
        """
        将数据分块
        
        Args:
            data: 原始数据列表
            chunk_size: 每块大小
        
        Returns:
            List[List[Dict[str, Any]]]: 分块后的数据列表
        """
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def _insert_chunk(self, chunk: List[Dict[str, Any]], target_table: str, thread_id: int) -> int:
        """
        插入单个数据块
        
        Args:
            chunk: 数据块
            target_table: 目标表名
            thread_id: 线程ID
        
        Returns:
            int: 插入的记录数
        """
        if not chunk:
            return 0
        
        batch_size = self.config_manager.get_batch_size()
        
        with self.connection_pool_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 动态生成占位符
            columns = list(chunk[0].keys())
            placeholders = [f"%({col})s" for col in columns]
            
            insert_sql = f"""
                INSERT INTO {target_table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            try:
                execute_batch(
                    cursor,
                    insert_sql,
                    chunk,
                    page_size=batch_size
                )
                conn.commit()
                logging.info(f"线程 {thread_id}: 成功插入 {len(chunk)} 条数据到表 {target_table}")
                return len(chunk)
            except Exception as e:
                conn.rollback()
                logging.error(f"线程 {thread_id}: 数据插入失败: {str(e)}")
                raise
    
    def insert_raw_data_threaded(self, room_list: List[Dict[str, Any]], target_table: str) -> bool:
        """
        多线程插入原始数据到数据库
        
        Args:
            room_list: 房间数据列表
            target_table: 目标表名
            
        Returns:
            bool: 插入是否成功
        """
        if not room_list:
            logging.warning("房间数据列表为空，跳过插入操作")
            print("房间数据列表为空，跳过插入操作")
            return True
        
        threading_config = self.config_manager.get_threading_config()
        max_workers = threading_config['max_workers']
        chunk_size = threading_config['chunk_size']
        
        # 分块数据
        chunks = self._chunk_data(room_list, chunk_size)
        total_inserted = 0
        
        logging.info(f"开始多线程插入: {len(room_list)} 条数据，分为 {len(chunks)} 块，使用 {max_workers} 个线程")
        print(f"开始多线程插入: {len(room_list)} 条数据，分为 {len(chunks)} 块，使用 {max_workers} 个线程")
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_chunk = {
                    executor.submit(self._insert_chunk, chunk, target_table, i): (chunk, i)
                    for i, chunk in enumerate(chunks)
                }
                
                # 处理完成的任务
                for future in as_completed(future_to_chunk):
                    chunk, thread_id = future_to_chunk[future]
                    try:
                        inserted_count = future.result()
                        total_inserted += inserted_count
                    except Exception as e:
                        logging.error(f"线程 {thread_id} 执行失败: {str(e)}")
                        print(f"线程 {thread_id} 执行失败: {str(e)}")
                        return False
            
            logging.info(f"多线程插入完成: 总共插入 {total_inserted} 条数据到表 {target_table}")
            print(f"多线程插入完成: 总共插入 {total_inserted} 条数据到表 {target_table}")
            return True
            
        except Exception as e:
            logging.error(f"多线程插入失败: {str(e)}")
            print(f"多线程插入失败: {str(e)}")
            return False


class DataManager:
    """
    数据管理器，负责数据的插入和操作
    """
    
    def __init__(self, db_manager: DatabaseManager, config_manager: ConfigManager):
        self.db_manager = db_manager
        self.config_manager = config_manager
    
    def insert_raw_data(self, room_list: List[Dict[str, Any]], target_table: Optional[str] = None) -> bool:
        """
        批量插入原始数据
        
        Args:
            room_list (List[Dict[str, Any]]): 房间数据列表
            target_table (str): 目标表名
            
        Returns:
            bool: 插入是否成功
        """
        if not room_list:
            logging.warning("数据列表为空，无需插入")
            print("数据列表为空，无需插入")
            return True

        columns = room_list[0].keys()
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        # 插入SQL
        insert_sql = f"""
            INSERT INTO {target_table} ({columns_str})
            VALUES ({placeholders})
        """

        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                # 从配置文件获取批处理大小
                batch_size = self.config_manager.get_batch_size()
                logging.info(f"使用批处理大小: {batch_size}")
                
                # 批量插入（直接传入原始字典列表）
                execute_batch(cursor, insert_sql, room_list, page_size=batch_size)
                conn.commit()
                
                logging.info(f"成功插入 {len(room_list)} 条原始数据")
                print(f"成功插入 {len(room_list)} 条原始数据")
                return True
                
        except Exception as e:
            logging.error(f"插入失败: {str(e)}")
            print(f"插入失败: {str(e)}")
            return False


class AgodaDataPipeline:
    """
    Agoda数据管道主类，协调各个管理器的工作
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_manager = ConfigManager(config_path)
        
        # 根据配置选择使用单线程还是多线程模式
        if self.config_manager.is_threading_enabled():
            self.connection_pool_manager = ConnectionPoolManager(self.config_manager)
            self.threaded_data_manager = ThreadedDataManager(self.connection_pool_manager, self.config_manager)
            logging.info("初始化为多线程模式")
        else:
            self.db_manager = DatabaseManager(self.config_manager)
            self.data_manager = DataManager(self.db_manager, self.config_manager)
            logging.info("初始化为单线程模式")
        
        self.table_manager = TableManager(self._get_database_manager(), self.config_manager)
        
        logging.info("Agoda数据管道初始化完成")
    
    def _get_database_manager(self):
        """
        获取数据库管理器（用于创建表等操作）
        
        Returns:
            数据库管理器
        """
        if self.config_manager.is_threading_enabled():
            # 对于表操作，使用单连接模式
            return DatabaseManager(self.config_manager)
        else:
            return self.db_manager
    
    def create_table(self, table_name: Optional[str] = None) -> bool:
        """
        创建表
        
        Args:
            table_name (str): 表名，如果为None则从配置文件读取
            
        Returns:
            bool: 创建是否成功
        """
        return self.table_manager.create_table(table_name)
    
    def insert_data(self, room_list: List[Dict[str, Any]], target_table: Optional[str] = None) -> bool:
        """
        插入数据（自动选择单线程或多线程模式）
        
        Args:
            room_list (List[Dict[str, Any]]): 房间数据列表
            target_table (Optional[str]): 目标表名，如果为None则使用配置文件中的默认表名
            
        Returns:
            bool: 插入是否成功
        """
        if target_table is None:
            target_table = self.config_manager.get_table_name()
        
        if self.config_manager.is_threading_enabled():
            return self.threaded_data_manager.insert_raw_data_threaded(room_list, target_table)
        else:
            return self.data_manager.insert_raw_data(room_list, target_table)
    
    def insert_data_single_thread(self, room_list: List[Dict[str, Any]], target_table: Optional[str] = None) -> bool:
        """
        强制使用单线程模式插入数据（向后兼容接口）
        
        Args:
            room_list (List[Dict[str, Any]]): 房间数据列表
            target_table (Optional[str]): 目标表名，如果为None则使用配置文件中的默认表名
            
        Returns:
            bool: 插入是否成功
        """
        if target_table is None:
            target_table = self.config_manager.get_table_name()
        
        # 如果当前是多线程模式，临时创建单线程管理器
        if self.config_manager.is_threading_enabled():
            temp_database_manager = DatabaseManager(self.config_manager)
            temp_data_manager = DataManager(temp_database_manager, self.config_manager)
            return temp_data_manager.insert_raw_data(room_list, target_table)
        else:
            return self.data_manager.insert_raw_data(room_list, target_table)
    
    def insert_data_multi_thread(self, room_list: List[Dict[str, Any]], target_table: Optional[str] = None) -> bool:
        """
        强制使用多线程模式插入数据
        
        Args:
            room_list (List[Dict[str, Any]]): 房间数据列表
            target_table (Optional[str]): 目标表名，如果为None则使用配置文件中的默认表名
            
        Returns:
            bool: 插入是否成功
        """
        if target_table is None:
            target_table = self.config_manager.get_table_name()
        
        # 如果当前是单线程模式，临时创建多线程管理器
        if not self.config_manager.is_threading_enabled():
            temp_connection_pool_manager = ConnectionPoolManager(self.config_manager)
            temp_threaded_data_manager = ThreadedDataManager(temp_connection_pool_manager, self.config_manager)
            try:
                result = temp_threaded_data_manager.insert_raw_data_threaded(room_list, target_table)
                temp_connection_pool_manager.close_pool()
                return result
            except Exception as e:
                logging.error(f"强制多线程插入失败: {str(e)}")
                temp_connection_pool_manager.close_pool()
                return False
        else:
            return self.threaded_data_manager.insert_raw_data_threaded(room_list, target_table)
    
    def close(self) -> None:
        """
        关闭资源
        """
        if self.config_manager.is_threading_enabled() and hasattr(self, 'connection_pool_manager'):
            self.connection_pool_manager.close_pool()
            logging.info("连接池已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 向后兼容的函数接口
def create_table(table_name: Optional[str] = None) -> bool:
    """
    创建表的向后兼容函数
    
    Args:
        table_name (str): 表名，如果为None则从配置文件读取
        
    Returns:
        bool: 创建是否成功
    """
    with AgodaDataPipeline() as pipeline:
        return pipeline.create_table(table_name)


def insert_raw_data(room_list: List[Dict[str, Any]]) -> bool:
    """
    插入原始数据的向后兼容函数
    
    Args:
        room_list (List[Dict[str, Any]]): 房间数据列表
        
    Returns:
        bool: 插入是否成功
    """
    with AgodaDataPipeline() as pipeline:
        return pipeline.insert_data(room_list)


def insert_raw_data_threaded(room_list: List[Dict[str, Any]], target_table: Optional[str] = None) -> bool:
    """
    多线程插入原始数据的函数
    
    Args:
        room_list (List[Dict[str, Any]]): 房间数据列表
        target_table (Optional[str]): 目标表名
        
    Returns:
        bool: 插入是否成功
    """
    with AgodaDataPipeline() as pipeline:
        return pipeline.insert_data_multi_thread(room_list, target_table)


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 使用面向对象方式（推荐使用上下文管理器）
    with AgodaDataPipeline() as pipeline:
        # 从配置文件创建表
        success = pipeline.create_table()
        
        if success:
            logging.info("表创建成功，可以继续进行数据插入操作")
        else:
            logging.error("表创建失败，请检查配置和数据库连接")
        
        # # 执行插入（示例）
        # sample_data = [
        #     {"room_name": "测试房间", "price": "100", "hotel_name": "测试酒店"}
        # ]
        # # 自动选择单线程或多线程模式
        # pipeline.insert_data(sample_data)
        # 
        # # 或者强制使用多线程模式
        # pipeline.insert_data_multi_thread(sample_data)
        # 
        # # 或者强制使用单线程模式
        # pipeline.insert_data_single_thread(sample_data)