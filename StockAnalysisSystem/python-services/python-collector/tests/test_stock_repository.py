"""
StockRepository 单元测试

验证行情写入、去重、读取等功能。
"""

import unittest
from unittest.mock import MagicMock, patch, call
from datetime import date
from decimal import Decimal

import pandas as pd
from sqlalchemy import text


class TestStockRepository(unittest.TestCase):
    """StockRepository 测试"""

    def _make_repo(self):
        from app.repositories.stock_repository import StockRepository
        session = MagicMock()
        return StockRepository(session), session

    def test_save_daily_records_empty(self):
        """空记录不执行写入"""
        repo, session = self._make_repo()
        count = repo.save_daily_records([])
        self.assertEqual(count, 0)
        session.execute.assert_not_called()

    def test_save_daily_records_batch(self):
        """批量写入调用正确次数"""
        repo, session = self._make_repo()

        records = [
            {
                'ts_code': f'{i:06d}.SZ',
                'trade_date': date(2026, 7, 3),
                'open': 10.0, 'high': 10.5, 'low': 9.5,
                'close': 10.2, 'pre_close': 9.8,
                'change': 0.4, 'pct_chg': 4.08,
                'vol': 100000, 'amount': 102000,
            }
            for i in range(10)
        ]

        count = repo.save_daily_records(records, batch_size=5)
        self.assertEqual(count, 10)
        # 应该调用 2 次（5+5）
        self.assertEqual(session.execute.call_count, 2)

    def test_save_stock_basic_empty(self):
        """空股票列表不执行写入"""
        repo, session = self._make_repo()
        count = repo.save_stock_basic([])
        self.assertEqual(count, 0)

    def test_get_latest_trade_dates(self):
        """获取最新交易日期"""
        repo, session = self._make_repo()

        mock_result = [('000001.SZ', date(2026, 7, 3)), ('000002.SZ', date(2026, 7, 2))]
        session.execute.return_value = mock_result

        result = repo.get_latest_trade_dates()

        self.assertEqual(result['000001.SZ'], date(2026, 7, 3))
        self.assertEqual(result['000002.SZ'], date(2026, 7, 2))

    def test_iter_daily_records_streams_ordered_bounded_batches(self):
        repo, session = self._make_repo()
        rows = [
            {
                'ts_code': code, 'trade_date': day,
                'open': Decimal('10.1234'), 'high': Decimal('11.0000'),
                'low': Decimal('9.0000'), 'close': Decimal('10.5000'),
                'pre_close': Decimal('10.0000'), 'change': Decimal('0.5000'),
                'pct_chg': Decimal('5.0000'), 'vol': Decimal('100.0000'),
                'amount': Decimal('1050.0000'),
            }
            for day, code in [
                (date(2026, 8, 1), '000001.SZ'),
                (date(2026, 8, 1), '000002.SZ'),
                (date(2026, 8, 2), '000001.SZ'),
            ]
        ]
        session.execute.return_value.mappings.return_value = iter(rows)

        batches = repo.iter_daily_records(
            date(2026, 8, 1), date(2026, 8, 2), batch_size=2
        )
        session.execute.assert_not_called()
        first = next(batches)
        second = next(batches)
        with self.assertRaises(StopIteration):
            next(batches)

        self.assertEqual([len(first), len(second)], [2, 1])
        self.assertEqual(first[0].source, 'MYSQL_REPLAY')
        self.assertEqual(first[0].open, Decimal('10.1234'))
        self.assertEqual(second[0].trade_date, date(2026, 8, 2))
        stmt, params = session.execute.call_args.args
        self.assertIn('BETWEEN :start AND :end', str(stmt))
        self.assertNotIn('ts_code = :ts_code', str(stmt))
        self.assertIn('ORDER BY trade_date ASC, ts_code ASC', str(stmt))
        self.assertEqual(params, {
            'start': date(2026, 8, 1), 'end': date(2026, 8, 2)
        })
        self.assertTrue(stmt.get_execution_options().get('stream_results'))

    def test_iter_daily_records_filters_one_stock_with_bound_parameter(self):
        repo, session = self._make_repo()
        session.execute.return_value.mappings.return_value = iter([])

        list(repo.iter_daily_records(
            date(2026, 8, 1), date(2026, 8, 28), batch_size=20,
            ts_code='000001.SZ',
        ))

        stmt, params = session.execute.call_args.args
        self.assertIn('AND ts_code = :ts_code', str(stmt))
        self.assertIn('ORDER BY trade_date ASC, ts_code ASC', str(stmt))
        self.assertEqual(params, {
            'start': date(2026, 8, 1),
            'end': date(2026, 8, 28),
            'ts_code': '000001.SZ',
        })

    def test_iter_daily_records_rejects_nonpositive_batch_size(self):
        repo, session = self._make_repo()
        with self.assertRaises(ValueError):
            list(repo.iter_daily_records(date(2026, 8, 1), date(2026, 8, 2), 0))
        session.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
