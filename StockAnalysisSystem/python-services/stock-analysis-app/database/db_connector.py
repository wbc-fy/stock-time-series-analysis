"""
数据库连接与会话管理

提供 DatabaseConnector 类作为项目唯一的数据库入口：
- 连接超时 & 失败诊断
- session_scope() 上下文管理器（自动 commit / rollback / close）
- batch_insert_ignore() 通用批量写入
- read_table() / execute_query() 读取工具

所有其他模块必须通过 DatabaseConnector 访问数据库，禁止自行拼接连接字符串。
"""

from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, declarative_base

from config.settings import DATABASE_CONFIG, mask_secret
from config.logging_config import get_logger

logger = get_logger(__name__)

# 创建Base类
Base = declarative_base()


class DatabaseConnector:
    """数据库连接器 — 项目唯一入口"""

    def __init__(self, config=None):
        """
        初始化数据库连接

        Args:
            config: 数据库配置字典，默认使用 DATABASE_CONFIG
        """
        self.config = config or DATABASE_CONFIG
        self.engine = None
        self.Session = None
        self._connect()

    # ── 连接管理 ──────────────────────────────────────────────

    def _connect(self):
        """建立数据库连接（含超时、活跃检测、失败诊断）"""
        cfg = self.config
        try:
            url = URL.create('mysql+pymysql', username=cfg['user'], password=cfg['password'],
                             host=cfg['host'], port=int(cfg['port']), database=cfg['database'],
                             query={'charset': cfg['charset']})
            connect_args = {'connect_timeout': cfg.get('connect_timeout', 10)}
            # Opt-in per caller; legacy connection defaults remain unchanged.
            for timeout in ('read_timeout', 'write_timeout'):
                if timeout in cfg:
                    connect_args[timeout] = cfg[timeout]
            self.engine = create_engine(
                url,
                echo=False,
                pool_recycle=cfg.get('pool_recycle', 3600),
                pool_pre_ping=cfg.get('pool_pre_ping', True),
                pool_timeout=cfg.get('pool_timeout', 30),
                connect_args=connect_args,
            )
            self.Session = sessionmaker(bind=self.engine)
            logger.info(
                f"数据库连接成功 ({cfg['host']}:{cfg['port']}/{cfg['database']})"
            )
        except OperationalError as e:
            safe_msg = str(e).replace(cfg['password'], mask_secret(cfg['password']))
            logger.error(
                f"数据库连接失败: {safe_msg}\n"
                f"  请检查:\n"
                f"    1. MySQL 服务是否运行 ({cfg['host']}:{cfg['port']})\n"
                f"    2. 用户名/密码是否正确 (user={cfg['user']})\n"
                f"    3. 数据库 '{cfg['database']}' 是否存在\n"
                f"    4. .env 配置是否与实际一致"
            )
            raise
        except Exception as e:
            safe_msg = str(e).replace(cfg['password'], mask_secret(cfg['password']))
            logger.error(f"数据库连接异常: {safe_msg}")
            raise

    def create_tables(self):
        """创建所有 ORM 定义的表"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("数据库表创建/同步完成")
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            raise

    def get_session(self):
        """获取新 Session 实例"""
        return self.Session()

    def close(self):
        """关闭连接池"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接已关闭")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ── 会话上下文管理器 ─────────────────────────────────────

    @contextmanager
    def session_scope(self):
        """
        自动管理 session 生命周期：

        - 正常退出 → commit
        - 异常退出 → rollback
        - 始终 → close session

        Usage::

            with DatabaseConnector() as db:
                with db.session_scope() as session:
                    session.execute(...)
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── 通用数据操作 ──────────────────────────────────────────

    def batch_insert_ignore(self, table_name, records, columns=None, batch_size=5000):
        """
        通用 INSERT IGNORE 批量写入。

        Args:
            table_name: 目标表名
            records: list[dict] 数据记录
            columns: 列名列表（默认取 records[0].keys()）
            batch_size: 每批大小

        Returns:
            int: 写入行数
        """
        if not records:
            return 0

        if columns is None:
            columns = list(records[0].keys())

        col_str = ', '.join(columns)
        placeholders = ', '.join([f':{c}' for c in columns])
        stmt = text(
            f'INSERT IGNORE INTO {table_name} '
            f'({col_str}) VALUES ({placeholders})'
        )

        total = 0
        with self.session_scope() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    session.execute(stmt, batch)
                except Exception:
                    session.rollback()
                    raise
                total += len(batch)
                if (i // batch_size + 1) % 20 == 0:
                    logger.info(
                        f"已写入 {total}/{len(records)} 条 → {table_name}"
                    )
        return total

    def read_table(self, table_name, **kwargs):
        """
        读取整张表为 DataFrame。

        Args:
            table_name: 表名
            **kwargs: 透传给 pd.read_sql_table

        Returns:
            DataFrame
        """
        with self.session_scope() as session:
            return pd.read_sql_table(table_name, session.bind, **kwargs)

    def execute_query(self, sql, params=None):
        """
        执行 SQL 查询并返回 DataFrame。

        Args:
            sql: SQL 字符串（会被 text() 包装）
            params: 查询参数 dict

        Returns:
            DataFrame
        """
        with self.session_scope() as session:
            return pd.read_sql(text(sql), session.bind, params=params)
