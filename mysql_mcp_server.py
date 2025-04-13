import os
from typing import List, Dict, Any, Optional, Union
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import QueuePool
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
import logging
from logging.handlers import RotatingFileHandler

# 初始化MCP服务器
mcp = FastMCP("MySQLMCP")

# --------------------------
# 配置模型
# --------------------------

class DatabaseConfig(BaseSettings):
    """数据库配置模型"""
    DB_HOST: str = Field(..., env="DB_HOST")
    DB_PORT: int = Field(3306, env="DB_PORT")
    DB_USER: str = Field(..., env="DB_USER")
    DB_PASSWORD: str = Field(..., env="DB_PASSWORD")
    DB_NAME: str = Field(..., env="DB_NAME")
    DB_POOL_SIZE: int = Field(5, env="DB_POOL_SIZE")
    DB_POOL_RECYCLE: int = Field(3600, env="DB_POOL_RECYCLE")
    DB_MAX_OVERFLOW: int = Field(2, env="DB_MAX_OVERFLOW")
    QUERY_TIMEOUT: int = Field(30, env="QUERY_TIMEOUT")
    MAX_RESULT_ROWS: int = Field(1000, env="MAX_RESULT_ROWS")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# --------------------------
# 初始化配置和日志
# --------------------------

def setup_logging():
    """配置日志系统"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger("mysql_mcp")
    logger.setLevel(logging.INFO)
    
    # 文件日志 (最大10MB，保留3个备份)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "mysql_mcp.log"),
        maxBytes=10*1024*1024,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))
    logger.addHandler(file_handler)
    
    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(levelname)s - %(message)s"
    ))
    logger.addHandler(console_handler)
    
    return logger

# 加载配置和日志
load_dotenv()
config = DatabaseConfig()
logger = setup_logging()

# --------------------------
# 数据库连接池
# --------------------------

def create_db_engine():
    """创建数据库连接池引擎"""
    db_url = URL.create(
        drivername="mysql+pymysql",
        username=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        query={
            "charset": "utf8mb4",
            "connect_timeout": str(config.QUERY_TIMEOUT)
        }
    )
    
    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=config.DB_POOL_SIZE,
        max_overflow=config.DB_MAX_OVERFLOW,
        pool_recycle=config.DB_POOL_RECYCLE,
        pool_pre_ping=True,
        pool_use_lifo=True,
        echo=False
    )

engine = create_db_engine()

# --------------------------
# MCP工具函数
# --------------------------

class QueryParams(BaseModel):
    """查询参数模型"""
    query: str
    params: Optional[Dict[str, Any]] = None

class TableQueryParams(BaseModel):
    """表查询参数模型"""
    table_name: str
    columns: Optional[List[str]] = None
    where: Optional[str] = None
    order_by: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = 0

@mcp.tool()
def list_tables() -> List[str]:
    """列出数据库中所有表"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logger.info(f"成功获取表列表，共{len(tables)}张表")
        return tables
    except Exception as e:
        logger.error(f"获取表列表失败: {str(e)}")
        raise

@mcp.tool()
def describe_table(table_name: str) -> List[Dict[str, str]]:
    """获取表结构信息"""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        result = [{
            "name": col["name"],
            "type": str(col["type"]),
            "nullable": col["nullable"],
            "default": col["default"],
            "comment": col.get("comment", "")
        } for col in columns]
        
        logger.info(f"成功获取表结构: {table_name}")
        return result
    except Exception as e:
        logger.error(f"获取表结构失败[{table_name}]: {str(e)}")
        raise

@mcp.tool()
def execute_query(query_params: QueryParams) -> List[Dict[str, Any]]:
    """执行SQL查询"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(query_params.query), 
                query_params.params or {}
            )
            rows = [dict(row._mapping) for row in result]
            
            if len(rows) > config.MAX_RESULT_ROWS:
                rows = rows[:config.MAX_RESULT_ROWS]
                logger.warning(f"查询结果被截断，最多返回{config.MAX_RESULT_ROWS}行")
            
            logger.info(f"执行查询成功，返回{len(rows)}行")
            return rows
    except Exception as e:
        logger.error(f"执行查询失败: {str(e)}")
        raise

@mcp.tool()
def execute_write(query_params: QueryParams) -> Dict[str, Any]:
    """执行写操作(INSERT/UPDATE/DELETE)"""
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(query_params.query), 
                query_params.params or {}
            )
            logger.info(f"写操作成功，影响{result.rowcount}行")
            return {
                "status": "success",
                "affected_rows": result.rowcount,
                "lastrowid": result.lastrowid
            }
    except Exception as e:
        logger.error(f"执行写操作失败: {str(e)}")
        raise

@mcp.tool()
def get_table_data(params: TableQueryParams) -> List[Dict[str, Any]]:
    """获取表数据"""
    try:
        # 构建安全查询
        columns = params.columns or ["*"]
        column_list = ", ".join(columns) if isinstance(columns, list) else columns
        
        query = f"SELECT {column_list} FROM {params.table_name}"
        
        if params.where:
            query += f" WHERE {params.where}"
        if params.order_by:
            query += f" ORDER BY {params.order_by}"
        
        query += f" LIMIT {params.limit} OFFSET {params.offset}"
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(row._mapping) for row in result]
            
            if len(rows) > config.MAX_RESULT_ROWS:
                rows = rows[:config.MAX_RESULT_ROWS]
                logger.warning(f"查询结果被截断，最多返回{config.MAX_RESULT_ROWS}行")
            
            logger.info(f"获取表数据成功: {params.table_name}, 返回{len(rows)}行")
            return rows
    except Exception as e:
        logger.error(f"获取表数据失败[{params.table_name}]: {str(e)}")
        raise

# --------------------------
# 主程序
# --------------------------

if __name__ == "__main__":
    try:
        logger.info("启动MySQL MCP Server...")
        logger.info(f"连接数据库: {config.DB_USER}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
        
        # 测试数据库连接
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("数据库连接测试成功")
        
        # 启动MCP服务器
        mcp.run(transport="stdio")
    except Exception as e:
        logger.critical(f"服务器启动失败: {str(e)}")
        raise