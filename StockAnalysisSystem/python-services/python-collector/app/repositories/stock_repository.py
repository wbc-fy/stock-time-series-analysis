"""
股票数据访问层 (StockRepository)

只负责数据库读写：
    - 写入股票基础信息
    - 写入日线行情
    - 批量插入数据
    - 根据业务键处理重复数据（INSERT IGNORE）
    - 查询某个交易日是否已有数据

不负责：
    - 调用 Tushare / AkShare
    - 控制采集流程
    - 更新任务状态
"""

from datetime import date
from typing import List, Optional, Dict

import pandas as pd
from sqlalchemy import text

from app.models.stock_daily import StockDailyRecord
from app.utils.logger import get_logger

logger = get_logger(__name__)

# 表名
TABLE_STOCK_BASIC = 'stock_basic'
TABLE_STOCK_DAILY = 'stock_daily'

_BATCH_SIZE = 5000


class StockRepository:
    """股票数据访问仓库"""

    def __init__(self, session):
        """
        Args:
            session: SQLAlchemy Session 实例
        """
        self.session = session

    # ── 写入操作 ──────────────────────────────────────────────

    def save_daily_records(self, records: List[dict], batch_size: int = _BATCH_SIZE) -> int:
        """
        批量写入日线数据（INSERT IGNORE 去重）。

        业务唯一键：ts_code + trade_date

        Args:
            records: 日线数据字典列表
            batch_size: 每批大小

        Returns:
            int: 提交行数
        """
        if not records:
            return 0

        stmt = text(
            f'INSERT IGNORE INTO `{TABLE_STOCK_DAILY}` '
            '(`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, '
            '`pre_close`, `change`, `pct_chg`, `vol`, `amount`) '
            'VALUES (:ts_code, :trade_date, :open, :high, :low, :close, '
            ':pre_close, :change, :pct_chg, :vol, :amount)'
        )
        return self._batch_execute(stmt, records, batch_size, TABLE_STOCK_DAILY)

    def save_stock_basic(self, records: List[dict]) -> int:
        """
        写入股票基本信息（INSERT IGNORE 去重）。

        业务唯一键：ts_code

        Args:
            records: 股票基本信息字典列表

        Returns:
            int: 提交行数
        """
        if not records:
            return 0

        stmt = text(
            f'INSERT IGNORE INTO `{TABLE_STOCK_BASIC}` '
            '(ts_code, symbol, name, area, industry, list_date) '
            'VALUES (:ts_code, :symbol, :name, :area, :industry, :list_date)'
        )
        return self._batch_execute(stmt, records, len(records), TABLE_STOCK_BASIC)

    # ── 读取操作 ──────────────────────────────────────────────

    def iter_daily_records(
        self, start_date: date, end_date: date, batch_size: int = _BATCH_SIZE,
        ts_code: Optional[str] = None,
    ):
        """Stream inclusive, deterministically ordered stock_daily rows in batches."""
        if batch_size <= 0:
            raise ValueError('batch_size 必须大于 0')
        if start_date > end_date:
            raise ValueError('start_date 不能晚于 end_date')

        ts_code_filter = 'AND ts_code = :ts_code ' if ts_code is not None else ''
        stmt = text(
            'SELECT ts_code, trade_date, open, high, low, close, pre_close, '
            '`change`, pct_chg, vol, amount '
            'FROM stock_daily WHERE trade_date BETWEEN :start AND :end '
            f'{ts_code_filter}'
            'ORDER BY trade_date ASC, ts_code ASC'
        ).execution_options(stream_results=True, yield_per=batch_size)
        params = {'start': start_date, 'end': end_date}
        if ts_code is not None:
            params['ts_code'] = ts_code
        rows = self.session.execute(stmt, params).mappings()
        batch = []
        for row in rows:
            batch.append(StockDailyRecord(**dict(row), source='MYSQL_REPLAY'))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def load_stock_basic_df(self) -> pd.DataFrame:
        """读取 stock_basic 整表。"""
        return pd.read_sql_table(TABLE_STOCK_BASIC, self.session.bind)

    def get_latest_trade_dates(self) -> Dict[str, date]:
        """
        查询每只股票的最新交易日期。

        Returns:
            dict: {ts_code: max_trade_date}
        """
        result = self.session.execute(text(
            f'SELECT ts_code, MAX(trade_date) as max_date '
            f'FROM `{TABLE_STOCK_DAILY}` GROUP BY ts_code'
        ))
        return {row[0]: row[1] for row in result}

    def check_trade_date_exists(self, trade_date: str) -> bool:
        """
        检查某个交易日是否已有行情数据。

        Args:
            trade_date: YYYYMMDD 或 YYYY-MM-DD 格式

        Returns:
            True 如果已有数据
        """
        trade_date_clean = trade_date.replace('-', '')
        # 尝试两种格式
        for fmt in [trade_date_clean, f"{trade_date_clean[:4]}-{trade_date_clean[4:6]}-{trade_date_clean[6:8]}"]:
            result = self.session.execute(text(
                f'SELECT COUNT(*) FROM `{TABLE_STOCK_DAILY}` '
                f'WHERE trade_date = :td LIMIT 1'
            ), {'td': fmt})
            count = result.scalar()
            if count and count > 0:
                return True

        # 直接用 date 对象尝试
        try:
            d = date(
                int(trade_date_clean[:4]),
                int(trade_date_clean[4:6]),
                int(trade_date_clean[6:8]),
            )
            result = self.session.execute(text(
                f'SELECT COUNT(*) FROM `{TABLE_STOCK_DAILY}` WHERE trade_date = :td'
            ), {'td': d})
            count = result.scalar()
            return count is not None and count > 0
        except (ValueError, IndexError):
            return False

    def count_records_by_date(self, trade_date: str) -> int:
        """
        统计某个交易日的行情记录数。

        Args:
            trade_date: YYYYMMDD

        Returns:
            int: 记录数
        """
        trade_date_clean = trade_date.replace('-', '')
        try:
            d = date(
                int(trade_date_clean[:4]),
                int(trade_date_clean[4:6]),
                int(trade_date_clean[6:8]),
            )
            result = self.session.execute(text(
                f'SELECT COUNT(*) FROM `{TABLE_STOCK_DAILY}` WHERE trade_date = :td'
            ), {'td': d})
            return result.scalar() or 0
        except (ValueError, IndexError):
            return 0

    # ── 内部工具 ──────────────────────────────────────────────

    def _batch_execute(self, stmt, records: List[dict], batch_size: int, table_name: str) -> int:
        """通用分批执行 INSERT 并记录进度。"""
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self.session.execute(stmt, batch)
            total += len(batch)
            if (i // batch_size + 1) % 20 == 0:
                logger.info(f"已写入 {total}/{len(records)} 条 → {table_name}")
        return total
